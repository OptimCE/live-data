"""The CRM reads outside the ownership projection. Protocol, adapter and fake.

DELIBERATELY NOT `ports/crm_core.py`. That name belongs to build step 7's
ownership projection - resolving which member held a meter at a given instant,
which is a genuinely hard time-sliced problem (`meter_data` has overlapping
windows that the database does not prevent, and billing guards against them with
a separate pre-flight query). Naming this module `crm_core` would pre-empt that
design and invite the next person to bolt ownership onto it.

Three reads on a session the caller already has open: the two scalar reads
enrolment needs (build steps 1-5), and the SET of subscribed communities the
ingest worker and the scheduler filter on (D-12). Every query is SELECT-only;
read-only-ness is enforced by the `live_data_svc` Postgres role
(postgres/provision/30-crm-grants.sql), not by anything here.

Raw `text()` rather than ORM models, matching the sibling annexes: a CRM schema
change then breaks ONE file with an obvious diff, instead of surfacing as a
mapper error somewhere else.
"""

import datetime
from dataclasses import dataclass
from typing import Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from domain.ownership import local_date_of

# MeterDataStatus.ACTIVE in crm-backend/src/modules/meters/shared/meter.types.ts.
# 1 ACTIVE, 2 INACTIVE, 3 WAITING_GRD, 4 WAITING_MANAGER.
_METER_DATA_ACTIVE = 1


@dataclass(frozen=True, slots=True)
class MeterSnapshot:
    """What a device records about its meter at creation time.

    A SNAPSHOT, not a live view: `capacity_kva` is copied onto `device` so the
    ingest worker can apply the `implausible_production` ceiling without a CRM
    round-trip per message.

    The name carries the unit because the CRM column does not. Deviation 4:
    `meter_data.total_generating_capacity` is described in all four locales as
    the maximum power in kVA the installation can INJECT - the AC inverter and
    grid-connection limit, not the DC panel peak (kWc) that a forecasting model
    would want. A PV array is routinely oversized against its inverter, so this
    is a ceiling to clip against and never a capacity to trust.
    """

    ean: str
    id_community: int
    capacity_kva: float | None
    injection_status: int | None
    production_chain: int | None


class CrmReadPort(Protocol):
    """The CRM reads steps 1-5 need, plus the subscription set D-12 needs. Three."""

    async def find_active_meter(
        self, *, ean: str, id_community: int, now: datetime.datetime
    ) -> MeterSnapshot | None:
        """The active meter for this EAN in this community at `now`, or None.

        `device.ean` is a plain column in another database and never a foreign
        key, so this is the ONLY thing that will ever validate it. A typo'd EAN
        otherwise produces a device that ingests happily and is attributed to
        nobody - discovered at step 7 or 8, months later, with real data already
        stored against it.

        `now` is an aware instant, and the window is tested against its
        Brussels-local date (`domain.ownership.local_date_of`), which is the CRM's
        own "today" (`appTodayISO()`). Never `CURRENT_DATE`: that is the date in
        the database session's timezone, which nothing sets, so UTC. For an hour
        or two after every Belgian midnight the two disagree. The Add-device
        picker, fed by the CRM's active-meter list, then offers a meter whose
        window starts today, and this read refuses it with EAN_NOT_FOUND. A window
        that ended yesterday is still accepted.
        """
        ...

    async def is_feature_active_unscoped(self, *, id_community: int, feature: str) -> bool:
        """Is this community subscribed to `feature`?

        `_unscoped` IS THE LOAD-BEARING PART OF THE NAME, and this method exists
        only because `require_feature` cannot run on the public enrolment leg.

        `require_feature` fails there twice over. It calls `require_community()`,
        which raises 401 when there is no `X-Community-ID` header - and the
        public leg deliberately has none, because nginx blanks it. And its query
        goes through `with_community_scope`, which returns `stmt.where(false())`
        when the tenant ContextVar is unset.

        So the naive implementations fail SILENTLY IN BOTH DIRECTIONS: written as
        "accept only when a row is found", every enrolment is rejected; written
        as "reject only when a row is missing", every enrolment from a lapsed
        community succeeds. Neither raises, neither logs, and both look correct.

        The community id is therefore an explicit bind parameter, resolved from
        the DEVICE ROW THE TOKEN POINTS AT - never from a header, which on this
        leg is attacker-supplied.
        """
        ...

    async def active_communities_unscoped(self, *, feature: str) -> frozenset[int]:
        """Every community with an ACTIVE subscription to `feature`.

        Unscoped for the same reason as `is_feature_active_unscoped`, one level
        up. The ingest worker and the scheduler serve no request, so there is no
        tenant ContextVar: through `with_community_scope` this would be
        `WHERE false`, an EMPTY set - and the worker discards the telemetry of
        every community missing from it. The whole fleet would go quiet with
        nothing raised. The feature is an explicit bind parameter.

        An inactive row and no row are the same answer: not in the set. The
        inactive row is the common case, because crm-backend's unsubscribe flips
        `is_active` and keeps the row.
        """
        ...


class SqlAlchemyCrmRead:
    """The real adapter. SELECT-only."""

    # `:today` is the caller's Brussels-local date, bound from Python. Not
    # CURRENT_DATE: see the Protocol's docstring.
    _METER_SQL = text(
        """
        SELECT m.ean,
               m.id_community,
               md.total_generating_capacity,
               md.injection_status,
               md.production_chain
          FROM meter m
          JOIN meter_data md ON md.ean = m.ean
         WHERE m.ean = :ean
           AND m.id_community = :id_community
           AND md.status = :active
           AND CAST(:today AS DATE) BETWEEN md.start_date
                                        AND COALESCE(md.end_date, DATE 'infinity')
         ORDER BY md.start_date DESC
         LIMIT 1
        """
    )

    # NOT `with_community_scope`, and not by accident - see the Protocol's
    # docstring. The community is a bind parameter the caller resolved from the
    # token's device row.
    _SUBSCRIPTION_SQL = text(
        """
        SELECT is_active
          FROM community_subscription
         WHERE id_community = :id_community
           AND feature = :feature
        """
    )

    # The same table, read as a SET for the worker and the scheduler (D-12).
    # Unscoped for the same reason as the query above: see the Protocol.
    _ACTIVE_SET_SQL = text(
        """
        SELECT id_community
          FROM community_subscription
         WHERE feature = :feature
           AND is_active IS TRUE
        """
    )

    def __init__(self, crm_session: AsyncSession) -> None:
        self._session = crm_session

    async def find_active_meter(
        self, *, ean: str, id_community: int, now: datetime.datetime
    ) -> MeterSnapshot | None:
        row = (
            await self._session.execute(
                self._METER_SQL,
                {
                    "ean": ean,
                    "id_community": id_community,
                    "active": _METER_DATA_ACTIVE,
                    "today": local_date_of(now),
                },
            )
        ).first()
        if row is None:
            return None
        # LIMIT 1 with ORDER BY start_date DESC rather than a uniqueness
        # assumption: `meter_data` has no constraint preventing two overlapping
        # ACTIVE windows for one EAN, and billing only gets away with assuming
        # otherwise because it runs a separate overlap pre-flight first. Taking
        # the most recent is right for a snapshot; step 7's ownership projection
        # will have to do the harder thing.
        return MeterSnapshot(
            ean=row.ean,
            id_community=row.id_community,
            capacity_kva=(
                float(row.total_generating_capacity)
                if row.total_generating_capacity is not None
                else None
            ),
            injection_status=row.injection_status,
            production_chain=row.production_chain,
        )

    async def is_feature_active_unscoped(self, *, id_community: int, feature: str) -> bool:
        value = await self._session.scalar(
            self._SUBSCRIPTION_SQL, {"id_community": id_community, "feature": feature}
        )
        # No row and an inactive row are the same answer to the caller, and both
        # are FALSE. Written as an explicit bool so a None can never be truthy
        # by accident.
        return bool(value)

    async def active_communities_unscoped(self, *, feature: str) -> frozenset[int]:
        rows = await self._session.scalars(self._ACTIVE_SET_SQL, {"feature": feature})
        return frozenset(int(id_community) for id_community in rows)


class FakeCrmRead:
    """In-memory CRM for tests. Records calls.

    `meters` has no dates, so the fake does not model the window. It does
    resolve `now` through `local_date_of`, as the adapter does, so a caller
    passing a naive datetime fails here too rather than only against Postgres.
    """

    def __init__(
        self,
        meters: dict[tuple[str, int], MeterSnapshot] | None = None,
        subscribed: set[tuple[int, str]] | None = None,
    ) -> None:
        self.meters = meters or {}
        self.subscribed = subscribed or set()
        self.calls: list[tuple[str, object]] = []

    async def find_active_meter(
        self, *, ean: str, id_community: int, now: datetime.datetime
    ) -> MeterSnapshot | None:
        self.calls.append(("find_active_meter", (ean, id_community, local_date_of(now))))
        return self.meters.get((ean, id_community))

    async def is_feature_active_unscoped(self, *, id_community: int, feature: str) -> bool:
        self.calls.append(("is_feature_active_unscoped", (id_community, feature)))
        return (id_community, feature) in self.subscribed

    async def active_communities_unscoped(self, *, feature: str) -> frozenset[int]:
        self.calls.append(("active_communities_unscoped", feature))
        return frozenset(c for c, f in self.subscribed if f == feature)
