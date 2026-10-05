"""Refresh `device_owner_window` from the CRM. Build step 7.

A local projection of CRM ownership, so the rollup tick can resolve `n_members`
at a bucket's instant without reaching across the database boundary once per
bucket. The tick runs every 15 minutes; ownership changes a few times a year.

----------------------------------------------------------------------------
THE ATTRIBUTION DATE IS THE BUCKET'S, NOT EACH READING'S.

Billing attributes a reading to the member whose window contains THE READING'S
Brussels-local date. This service attributes an HOUR BUCKET to the member whose
window contains THE BUCKET'S Brussels-local date, and the difference is real
though small.

`measurement.ts` is the END of its interval, so the reading stamped 00:00:00
local closes the 23:45-00:00 interval and lands - correctly - in the 23:00
bucket of the PREVIOUS day, while its own local date is already the next day. On
the single day a meter changes hands, billing gives that one quarter-hour to the
new holder and this gives it to the old one.

That is not a bug to be fixed by matching billing, because `n_members` is stored
PER BUCKET: a bucket has exactly one membership set, and a rule that splits a
bucket between two owners has nowhere to put the answer. The bucket is the unit,
so the bucket's date is the key. The divergence is at most one interval per
transfer, on an INDICATIVE view, while billing remains the only basis for money.
----------------------------------------------------------------------------

EVERY EAN THAT HAS EVER HAD A DEVICE, NOT EVERY LIVE ONE.

Revoking a device does not delete what it already sent (protocol 8.6), so its
measurements stay in the table and keep needing an owner. Filtering on
`status <> REVOKED` here would make every historical bucket of a replaced device
un-attributable, which shows up as `n_members` falling and k suppressing data
that was visible yesterday.
"""

import datetime
import logging
from collections.abc import Sequence
from collections.abc import Set as AbstractSet
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from domain import buckets
from domain.ownership import OwnershipWindow, local_date_of
from ports.crm_core import CrmCoreReadPort
from shared.const import ROLLUP_DAY_TIMEZONE
from worker.retention import raw_floor

logger = logging.getLogger(__name__)

_COMMUNITIES_SQL = text("SELECT DISTINCT id_community FROM device ORDER BY id_community")

_EANS_SQL = text("SELECT DISTINCT ean FROM device WHERE id_community = :id_community ORDER BY ean")

_EXISTING_SQL = text(
    """
    SELECT ean, id_member, valid_from, valid_to, ambiguous, id_sharing_operation
      FROM device_owner_window
     WHERE id_community = :id_community
    """
)

_DELETE_SQL = text("DELETE FROM device_owner_window WHERE id_community = :id_community")

_INSERT_SQL = text(
    """
    INSERT INTO device_owner_window
        (ean, id_community, id_member, valid_from, valid_to, ambiguous,
         id_sharing_operation, refreshed_at)
    VALUES
        (:ean, :id_community, :id_member, :valid_from, :valid_to, :ambiguous,
         :id_sharing_operation, :refreshed_at)
    """
)

# Only buckets that already have a community rollup row, and only below `lo`.
#
# Below `lo`, because everything at or above it is inside the 48-hour window the
# tick recomputes unconditionally - marking those would be pure churn on the
# hottest table in the job.
#
# And restricted to rows that EXIST, because the alternative is generating a row
# per hour across the whole changed range, most of which never held a
# measurement. A community that corrected a 2019 window would otherwise get fifty
# thousand dirty rows and the tick would spend the next hour recomputing empty
# hours.
#
# `bucket` is the START of the hour, so its own local date is the bucket's date.
# No `- 1 second` here, unlike the measurement-to-bucket conversion - that
# adjustment exists because `measurement.ts` is an interval END, and a bucket
# start is not.
#
# And never below `:raw_floor`, the raw retention edge. `rollup_community_hour`
# is kept far longer than `measurement`, so an old bucket can be marked after its
# readings are gone - and the tick would then DELETE the hour, re-insert nothing,
# and re-derive that day's `rollup_community_day` row from nothing: deleting the
# one series meant to be kept for ever. A window corrected back to 2020 would do
# it to every day since. Found 2026-10-04, before any history was old enough.
_MARK_DIRTY_SQL = text(
    """
    INSERT INTO rollup_dirty (id_community, bucket)
    SELECT r.id_community, r.bucket
      FROM rollup_community_hour r
     WHERE r.id_community = :id_community
       AND r.bucket < :lo
       AND r.bucket >= :raw_floor
       AND (r.bucket AT TIME ZONE :tz)::date BETWEEN :dirty_from AND :dirty_to
    ON CONFLICT DO NOTHING
    RETURNING bucket
    """
)


@dataclass(frozen=True, slots=True)
class OwnershipRefreshResult:
    communities: int = 0
    windows_written: int = 0
    buckets_marked: int = 0


def _comparable(window: OwnershipWindow) -> tuple:
    """The identity of a window for change detection.

    `refreshed_at` is excluded deliberately - it changes on every run by
    construction, and including it would mark every bucket dirty every 15
    minutes: a self-inflicted denial of service that looks like a busy scheduler.

    `id_sharing_operation` is INCLUDED: a meter moved between operations changes
    which operation row its energy lands in, so the buckets must be recomputed.
    crm-backend's same-day correction rewrites a window's operation in place
    (`addMeterData` merges rather than opening a row), and only this comparison
    can see that.
    """
    return (
        window.ean,
        window.id_member,
        window.valid_from,
        window.valid_to,
        window.ambiguous,
        window.id_sharing_operation,
    )


def changed_date_span(
    before: Sequence[OwnershipWindow],
    after: Sequence[OwnershipWindow],
    *,
    today: datetime.date,
) -> tuple[datetime.date, datetime.date] | None:
    """The date range touched by the symmetric difference, or None if identical.

    A RANGE rather than the set of dates: a window can span years, and one opened
    in 2019 would otherwise materialise a couple of thousand dates to pass as a
    bind parameter. The range OVER-covers - it includes dates whose ownership did
    not change - and over-covering is safe here because the only consequence is
    recomputing a bucket to the value it already had.

    An open-ended window (`valid_to IS NULL`) is clamped to `today`: there are no
    buckets in the future to mark.
    """
    difference = set(map(_comparable, before)) ^ set(map(_comparable, after))
    if not difference:
        return None
    starts = [row[2] for row in difference]
    ends = [row[3] if row[3] is not None else today for row in difference]
    # `max(starts)` guards the degenerate row whose end precedes its start, which
    # the CRM does not prevent and which would otherwise produce an empty BETWEEN
    # that silently marks nothing.
    return min(starts), max(max(ends), max(starts))


async def refresh_community(
    local: AsyncSession,
    crm: CrmCoreReadPort,
    *,
    id_community: int,
    now: datetime.datetime,
) -> tuple[int, int]:
    """Refresh one community's windows. Returns (windows_written, buckets_marked).

    Does NOT commit. The caller owns the unit of work, so a test can run this
    inside its rolled-back transaction and the scheduler can commit per community.
    """
    eans = list((await local.execute(_EANS_SQL, {"id_community": id_community})).scalars().all())
    fresh = await crm.ownership_windows(eans=eans, id_community=id_community)

    rows = await local.execute(_EXISTING_SQL, {"id_community": id_community})
    existing = [
        OwnershipWindow(
            ean=row.ean,
            id_community=id_community,
            id_member=row.id_member,
            valid_from=row.valid_from,
            valid_to=row.valid_to,
            ambiguous=row.ambiguous,
            id_sharing_operation=row.id_sharing_operation,
        )
        for row in rows
    ]

    span = changed_date_span(existing, fresh, today=local_date_of(now))

    # DELETE-then-INSERT rather than an upsert, because `device_owner_window` has
    # no unique constraint for ON CONFLICT to name. That is not an oversight in
    # the DDL: the CRM permits two ACTIVE rows for one EAN over the same dates -
    # it is the very thing `ambiguous` exists to record - so a unique key here
    # would leave the projection unable to represent what it is projecting.
    await local.execute(_DELETE_SQL, {"id_community": id_community})
    for window in fresh:
        await local.execute(
            _INSERT_SQL,
            {
                "ean": window.ean,
                "id_community": id_community,
                "id_member": window.id_member,
                "valid_from": window.valid_from,
                "valid_to": window.valid_to,
                "ambiguous": window.ambiguous,
                "id_sharing_operation": window.id_sharing_operation,
                "refreshed_at": now,
            },
        )

    marked = 0
    if span is not None:
        lo, _hi = buckets.window(now)
        result = await local.execute(
            _MARK_DIRTY_SQL,
            {
                "id_community": id_community,
                "lo": lo,
                "raw_floor": raw_floor(now),
                "tz": ROLLUP_DAY_TIMEZONE,
                "dirty_from": span[0],
                "dirty_to": span[1],
            },
        )
        # RETURNING rather than `rowcount`: with ON CONFLICT DO NOTHING, only
        # rows actually inserted come back, so this counts buckets newly marked
        # rather than buckets considered. A bucket already dirty is not news.
        marked = len(result.all())

    return len(fresh), marked


async def refresh_ownership(
    local: AsyncSession,
    crm: CrmCoreReadPort,
    *,
    now: datetime.datetime,
    communities: AbstractSet[int] | None,
) -> OwnershipRefreshResult:
    """Refresh every community that has at least one device, within `communities`.

    `communities` is keyword-only with NO default, deliberately (D-12): the
    scheduler passes the ACTIVE set, and a default of "everyone" would let a
    caller that forgot it refresh switched-off communities silently. An explicit
    `None` means every community with a device.
    """
    ids = list((await local.execute(_COMMUNITIES_SQL)).scalars().all())
    if communities is not None:
        ids = [id_community for id_community in ids if id_community in communities]
    written = 0
    marked = 0
    for id_community in ids:
        community_written, community_marked = await refresh_community(
            local, crm, id_community=id_community, now=now
        )
        written += community_written
        marked += community_marked
    logger.info(
        "ownership refresh: %d communities, %d windows, %d buckets marked dirty",
        len(ids),
        written,
        marked,
    )
    return OwnershipRefreshResult(
        communities=len(ids), windows_written=written, buckets_marked=marked
    )
