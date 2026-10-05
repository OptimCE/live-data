"""Time-sliced CRM ownership: which member held a meter, and when.

The module `ports/crm_read.py` reserved by name. It is deliberately NOT an
extension of that file: `crm_read` answers two scalar questions about NOW
(`find_active_meter`, `is_feature_active_unscoped`) and takes the most recent
window when several match. That shortcut is right for a creation-time snapshot
and wrong for everything here.

Raw `text()` rather than ORM models, matching `crm_read` and the sibling
annexes: a CRM schema change then breaks ONE file with an obvious diff instead of
surfacing as a mapper error somewhere else. SELECT-only; read-only-ness is
enforced by the `live_data_svc` Postgres role (postgres/provision/30-crm-grants.sql),
not by anything in this file.

----------------------------------------------------------------------------
TWO DIVERGENCES FROM `billing/ports/crm_core.py`, WHICH THIS OTHERWISE MIRRORS.

1.  `id_sharing_operation` is READ, never a scope. Billing scopes every
    ownership query by one operation; here the scope is `meter.id_community`,
    the tenant boundary this service uses everywhere else, and the operation is
    projected alongside the member so the rollup can attribute each device-hour
    to one operation (D-14, migration 0003).

2.  NO OVERLAP PRE-FLIGHT, and no 422. Billing runs `find_ownership_overlaps`
    first and REFUSES the run when it finds anything, because an invoice that
    might be wrong must not be issued. A background projection has nobody to
    refuse to, so overlap detection moves into `domain/ownership.mark_ambiguous`
    and becomes a per-window flag rather than a per-run veto.

    That is also why the overlap test is Python here and SQL there: billing wants
    one boolean for a period, this wants a flag on each window it is already
    holding. A self-join would re-fetch what the caller has in hand.
----------------------------------------------------------------------------
"""

from collections.abc import Sequence
from typing import Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from domain.ownership import OwnershipWindow, mark_ambiguous

# MeterDataStatus.ACTIVE, as in crm-backend/src/modules/meters/shared/meter.types.ts
# and in `ports/crm_read.py`. 1 ACTIVE, 2 INACTIVE, 3 WAITING_GRD, 4 WAITING_MANAGER.
_METER_DATA_ACTIVE = 1


class CrmCoreReadPort(Protocol):
    """The one read the ownership projection needs."""

    async def ownership_windows(
        self, *, eans: Sequence[str], id_community: int
    ) -> list[OwnershipWindow]:
        """Every ACTIVE ownership window for these EANs in this community.

        ALL of them, not the current one and not the most recent: the projection
        has to answer "who held this meter at 03:00 last Tuesday", and a reading
        may legitimately be 35 days old (`ts_too_old` rejects only beyond that).

        The returned windows carry `ambiguous` already resolved. Returns an empty
        list for an empty `eans`, without issuing a query - a naive
        `ean = ANY('{}')` is not wrong, but the caller's "no devices yet" case is
        the common one on a fresh community and deserves not to be a round trip.
        """
        ...


class SqlAlchemyCrmCoreRead:
    """The real adapter. SELECT-only."""

    # Scoped through `meter.id_community`, the tenant boundary every other CRM
    # read here uses (`crm_read.find_active_meter` included). `meter_data` carries
    # an `id_community` of its own in the real CRM - an earlier comment here said
    # it did not - but one definition of "this community's meter" is the point:
    # without a community predicate this would read another community's
    # ownership history for any EAN an attacker could guess, and EANs are printed
    # on the meter.
    _WINDOWS_SQL = text(
        """
        SELECT md.ean                  AS ean,
               m.id_community          AS id_community,
               md.id_member            AS id_member,
               md.start_date           AS valid_from,
               md.end_date             AS valid_to,
               md.id_sharing_operation AS id_sharing_operation
          FROM meter_data md
          JOIN meter m ON m.ean = md.ean
         WHERE m.id_community = :id_community
           AND md.status = :active
           AND md.ean = ANY(:eans)
         ORDER BY md.ean, md.start_date, md.id
        """
    )

    def __init__(self, crm_session: AsyncSession) -> None:
        self._session = crm_session

    async def ownership_windows(
        self, *, eans: Sequence[str], id_community: int
    ) -> list[OwnershipWindow]:
        if not eans:
            return []
        rows = await self._session.execute(
            self._WINDOWS_SQL,
            {"eans": list(eans), "id_community": id_community, "active": _METER_DATA_ACTIVE},
        )
        windows = [
            OwnershipWindow(
                ean=row.ean,
                id_community=row.id_community,
                id_member=row.id_member,
                valid_from=row.valid_from,
                valid_to=row.valid_to,
                id_sharing_operation=row.id_sharing_operation,
            )
            for row in rows
        ]
        # Flagged HERE rather than by the caller, so there is no way to obtain
        # unflagged windows from this port. `ambiguous=False` is the dataclass
        # default, and a window that skipped this step would read as unambiguous
        # while being nothing of the kind.
        return mark_ambiguous(windows)


class FakeCrmCoreRead:
    """In-memory ownership for tests. Records calls.

    Applies `mark_ambiguous` exactly as the real adapter does. A fake that
    returned windows unflagged would make every ambiguity test pass against the
    fake and fail against Postgres - the shape that once hid an unrevokable
    device behind 270 green tests.
    """

    def __init__(self, windows: Sequence[OwnershipWindow] | None = None) -> None:
        self.windows = list(windows or [])
        self.calls: list[tuple[str, object]] = []

    async def ownership_windows(
        self, *, eans: Sequence[str], id_community: int
    ) -> list[OwnershipWindow]:
        self.calls.append(("ownership_windows", (tuple(eans), id_community)))
        if not eans:
            return []
        wanted = set(eans)
        return mark_ambiguous(
            window
            for window in self.windows
            if window.ean in wanted and window.id_community == id_community
        )
