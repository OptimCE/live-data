"""Owned-database access for devices and their enrolment tokens.

Every SELECT on tenant data goes through `_scoped()`, and every INSERT stamps
`id_community` from `tenant_id()` - which is the same function, so a missing
community in the request context is a 403 on both paths rather than an empty
result on one of them.

The repository takes ONE session and never commits - the service owns the unit
of work.

----------------------------------------------------------------------------
THE ONE UNSCOPED READ, AND WHY IT IS SAFE

`find_device_by_token_hash` is deliberately NOT community-scoped, and is named
so nobody "fixes" it. The public enrolment leg has no user, no community header,
and therefore no tenant ContextVar - that is the entire point of the nginx
location that blanks those headers. The tenant has to be DISCOVERED from the
token's device row.

It is safe because `enrollment_token.token_hash` is UNIQUE over 256 bits of
hash space, so this can match at most one row and the caller cannot steer which.
Everything the service does afterwards is scoped to THAT row's community, passed
as an explicit parameter.
----------------------------------------------------------------------------
"""

from __future__ import annotations

import datetime
import logging
import uuid
from typing import TYPE_CHECKING, NamedTuple

from sqlalchemy import (
    TIMESTAMP,
    Date,
    and_,
    case,
    cast,
    distinct,
    func,
    join,
    literal_column,
    or_,
    select,
    text,
    update,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql import Select

from core.context_vars import current_internal_community_id
from core.errors.errors import ErrorException
from domain import buckets
from shared.const import (
    NO_SHARING_OPERATION,
    ROLLUP_DAY_TIMEZONE,
    DeviceStatus,
    ProductionChain,
)
from shared.custom_errors import errors
from shared.models.local_models import (
    CommunityLiveSettingsModel,
    DeviceLastModel,
    DeviceModel,
    DeviceOwnerWindowModel,
    EnrollmentTokenModel,
    IngestDeadLetterModel,
    MeasurementModel,
    RollupCommunityDayModel,
    RollupCommunityHourModel,
    RollupDeviceHourModel,
    RollupDirtyModel,
    RollupOperationDayModel,
    RollupOperationHourModel,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy import Row
    from sqlalchemy.ext.asyncio import AsyncSession


logger = logging.getLogger(__name__)


class RollupWatermarks(NamedTuple):
    """The three per-community facts `domain.rollup_freshness` judges from."""

    newest_bucket: datetime.datetime | None
    computed_at: datetime.datetime | None
    pending_since: datetime.datetime | None


def tenant_id() -> int:
    """The internal community id for the current request.

    RAISES rather than returning None. An INSERT without a tenant would create an
    unreachable row, and a SELECT without one would silently return nothing -
    which reads as "you have no devices" rather than as a failure. A clear 403 is
    better than either.

    This is the house pattern from the sibling annexes, and it is why this
    service needs no separate policy object: the property that "a missing tenant
    is a 403, not an empty list" is guaranteed here and applied by `_scoped`.
    """
    internal_id = current_internal_community_id.get()
    if internal_id is None:
        raise ErrorException(errors.auth.FORBIDDEN, status_code=403)
    return internal_id


def _scoped[TStmt: Select](stmt: TStmt, model: type) -> TStmt:
    """THE CHOKEPOINT. Every read of tenant data in this class goes through it.

    ---- why not `with_community_scope` ----
    The platform helper answers a missing tenant with `stmt.where(false())`, and
    for a LIST that is defensible: no rows, no leak. For an AGGREGATE it is not.
    `SUM(production_wh)` over no rows is NULL, which this service would render as
    a summary saying the community produced nothing - indistinguishable from
    night, and arriving with a 200. A missing tenant must be a 403, never a
    number.

    So `_scoped` applies `tenant_id()`, which raises. The three device reads use
    it too, although `where(false())` was adequate for them: plan section 18
    row 31 dropped the separate policy object because "one mechanism, not two",
    and two mechanisms is exactly what a read path that picks per query is.

    ---- why this is a function and not a mixin or a base class ----
    `tests/test_route_coverage.py` AST-walks this module and requires every
    method containing `select(` to also call `_scoped(`. That check is only
    meaningful against a call it can see.
    """
    return stmt.where(model.id_community == tenant_id())  # type: ignore[attr-defined]


def _scoped_join[TStmt: Select](stmt: TStmt, model: type) -> TStmt:
    """The same predicate, for a statement whose scoped table is OUTER-joined.

    Separate from `_scoped` because the two cannot be interchanged, and mixing
    them up is silent.

    A LEFT JOIN cannot be scoped in the WHERE clause. Writing
    `WHERE device_last.id_community = :t` after `LEFT JOIN device_last` discards
    every row where the join found nothing - the NULL fails the comparison - and
    the outer join quietly becomes an inner one. On the device list that means
    EVERY DEVICE THAT HAS NEVER REPORTED VANISHES: the list still renders, still
    looks right, and is missing exactly the devices an administrator opened it to
    find.

    The predicate therefore belongs in the JOIN's ON clause, and the caller is
    responsible for putting it there. This helper exists to scope the DRIVING
    table - the one that is always present - so the two roles stay distinct at
    the call site rather than in someone's memory.
    """
    return _scoped(stmt, model)


class LiveRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    # ---- devices --------------------------------------------------------

    async def list_devices(self) -> Sequence[DeviceModel]:
        """Every device in the caller's community, newest first.

        No pagination: a community has a handful of meters, not thousands, and a
        page parameter nothing needs is a contract to keep true for ever. Build
        step 6 adds one if a pilot ever proves it necessary.
        """
        stmt = _scoped(select(DeviceModel), DeviceModel).order_by(DeviceModel.created_at.desc())
        return (await self._session.execute(stmt)).scalars().all()

    async def get_device(self, public_id: uuid.UUID) -> DeviceModel | None:
        """One device, scoped. None when it belongs to another community.

        The scoping is what makes "another community's device" indistinguishable
        from "no such device" - a 404 rather than a 403, which tells a caller
        nothing about what exists elsewhere.
        """
        stmt = _scoped(select(DeviceModel), DeviceModel).where(DeviceModel.public_id == public_id)
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def live_device_exists_for_ean(self, ean: str) -> bool:
        """Mirrors `uq_device_community_ean_live`, so the clash is a clean 409.

        The partial unique index is the real guard; this exists so the caller
        gets a domain error instead of an IntegrityError surfacing as a 400 from
        `with_default_error`.
        """
        stmt = _scoped(select(DeviceModel.id), DeviceModel).where(
            DeviceModel.ean == ean,
            DeviceModel.status != int(DeviceStatus.REVOKED),
        )
        return (await self._session.execute(stmt)).first() is not None

    def add_device(
        self,
        *,
        public_id: uuid.UUID,
        device_type: int,
        ean: str,
        name: str,
        pure_injection: bool,
        capacity_kva: float | None,
        production_chain: int | None = None,
    ) -> DeviceModel:
        """Stage a PENDING device. Returns the un-flushed model."""
        device = DeviceModel(
            public_id=public_id,
            id_community=tenant_id(),
            type=device_type,
            ean=ean,
            name=name,
            status=int(DeviceStatus.PENDING),
            pure_injection=pure_injection,
            capacity_kva=capacity_kva,
            production_chain=production_chain,
        )
        self._session.add(device)
        return device

    # ---- enrolment tokens -----------------------------------------------

    def add_token(
        self,
        *,
        id_device: int,
        token_hash: str,
        expires_at: datetime.datetime,
    ) -> EnrollmentTokenModel:
        token = EnrollmentTokenModel(
            id_device=id_device,
            id_community=tenant_id(),
            token_hash=token_hash,
            expires_at=expires_at,
        )
        self._session.add(token)
        return token

    async def consume_open_tokens(self, id_device: int) -> None:
        """Invalidate any unconsumed token for a device.

        Called before issuing a new one: `uq_enrollment_token_device_unconsumed`
        allows at most one open token per device, so without this a reissue would
        raise. Marking the old one consumed rather than deleting it keeps the
        fact that it was issued.

        Scoped BY HAND: `_scoped` types SELECTs only, and an UPDATE
        without the tenant predicate is exactly the cross-tenant write that guard
        exists to prevent.
        """
        stmt = (
            update(EnrollmentTokenModel)
            .where(
                EnrollmentTokenModel.id_device == id_device,
                EnrollmentTokenModel.id_community == tenant_id(),
                EnrollmentTokenModel.consumed_at.is_(None),
            )
            .values(consumed_at=datetime.datetime.now(datetime.UTC))
        )
        await self._session.execute(stmt)

    async def devices_with_status(self, *, zero_window_hours: int = 24) -> Sequence[Row]:
        """Every device with its last-known state and its recent energy.

        A LEFT JOIN onto `device_last`, and the tenant predicate is in the ON
        clause rather than the WHERE. In the WHERE it would discard every row
        where the join found nothing - the NULL fails the comparison - silently
        converting the outer join to an inner one and DROPPING EVERY DEVICE THAT
        HAS NEVER REPORTED. Those are exactly the devices this page exists to
        surface, and the list would still render, still look right, and be
        missing them.

        `energy_recent_wh` is a correlated scalar subquery rather than a second
        join, for the reason the rollup gives at `n_members`: a join here fans out
        one row per matching hour and multiplies the device list.
        """
        recent = (
            select(
                func.sum(
                    func.coalesce(RollupDeviceHourModel.import_wh, 0.0)
                    + func.coalesce(RollupDeviceHourModel.export_wh, 0.0)
                    + func.coalesce(RollupDeviceHourModel.production_wh, 0.0)
                )
            )
            .where(
                RollupDeviceHourModel.id_device == DeviceModel.id,
                RollupDeviceHourModel.bucket
                >= func.now() - text(f"INTERVAL '{int(zero_window_hours)} hours'"),
            )
            .scalar_subquery()
        )
        stmt = _scoped_join(
            select(
                DeviceModel,
                DeviceLastModel.online,
                DeviceLastModel.diag,
                DeviceLastModel.diag_since,
                DeviceLastModel.last_seen_at,
                DeviceLastModel.last_reject_reason,
                DeviceLastModel.last_reject_at,
                DeviceLastModel.power_w,
                DeviceLastModel.ts.label("last_measurement_ts"),
                recent.label("energy_recent_wh"),
            ).select_from(
                join(
                    DeviceModel,
                    DeviceLastModel,
                    and_(
                        DeviceModel.id == DeviceLastModel.id_device,
                        DeviceLastModel.id_community == DeviceModel.id_community,
                    ),
                    isouter=True,
                )
            ),
            DeviceModel,
        ).order_by(DeviceModel.created_at.desc())
        return (await self._session.execute(stmt)).all()

    async def device_with_status(self, public_id: uuid.UUID, *, zero_window_hours: int = 24):
        """One device, same shape. None when it belongs to another community."""
        for row in await self.devices_with_status(zero_window_hours=zero_window_hours):
            if row.DeviceModel.public_id == public_id:
                return row
        return None

    async def dead_letter_count(self, *, hours: int = 24) -> int:
        """Messages from THIS community's devices that could not be stored.

        `ingest_dead_letter` is the only record that a message existed and was
        not stored - aiomqtt 2.x PUBACKs before any database write, so there is
        no redelivery and no other trace.

        ---- why this joins rather than scoping the table directly ----
        `ingest_dead_letter` HAS NO `id_community`, and that is deliberate: its
        `id_device` is NULL whenever the device could not be identified at all -
        an unparseable topic, an unknown device id - and a message nobody can
        attribute belongs to no community. There is no tenant to stamp.

        So the scope comes from the DEVICE, through an inner join, and rows with
        no device are excluded. Those rows are real and they matter, but they
        belong on a platform-operator view rather than on a community's page:
        showing them here would either leak another community's failures or
        invent an attribution the data does not support.
        """
        stmt = _scoped(
            select(func.count(IngestDeadLetterModel.id)).select_from(
                join(
                    IngestDeadLetterModel,
                    DeviceModel,
                    IngestDeadLetterModel.id_device == DeviceModel.id,
                )
            ),
            DeviceModel,
        ).where(
            IngestDeadLetterModel.received_at >= func.now() - text(f"INTERVAL '{int(hours)} hours'")
        )
        return int(await self._session.scalar(stmt) or 0)

    async def fleet_last_seen(self) -> Sequence[datetime.datetime]:
        """Every device in the community that has EVER reported, newest first.

        The evidence `domain.device_health.ingest_looks_healthy` needs, and the
        reason it is a list rather than a `max()`: the verdict turns on how many
        devices carry evidence at all, not only on the newest of them. One
        reporting device cannot distinguish "the collector is down" from "this
        meter is dead", so the count is load-bearing and a scalar would throw it
        away.

        `ops_health` does not call this - it already holds the rows. `diagnostics`
        does, because one device cannot see its own fleet.

        REVOKED devices are excluded, as `ops_health` excludes them: revocation
        freezes `last_seen_at`, and a frozen timestamp counted as "a quiet device"
        is how one silent meter plus one revoked one read as an ingest outage.
        """
        rows = await self._session.scalars(
            _scoped(
                select(DeviceLastModel.last_seen_at)
                .join(
                    DeviceModel,
                    and_(
                        DeviceModel.id == DeviceLastModel.id_device,
                        DeviceModel.id_community == DeviceLastModel.id_community,
                    ),
                )
                .where(
                    DeviceLastModel.last_seen_at.is_not(None),
                    DeviceModel.status != int(DeviceStatus.REVOKED),
                )
                .order_by(DeviceLastModel.last_seen_at.desc()),
                DeviceLastModel,
            )
        )
        # The WHERE already excludes NULL; the column is Optional in the ORM, so
        # the narrowing is for mypy rather than for the data.
        return [ts for ts in rows.all() if ts is not None]

    async def rollup_watermarks(self) -> RollupWatermarks:
        """What `domain.rollup_freshness` needs to say whether the TICK keeps up.

        The newest bucket alone cannot: it is how new the DATA is, and a quiet
        fleet stops producing buckets while the scheduler is healthy. That age
        was what the Ops tab once called "not recomputed", every quiet evening.
        See the module docstring of `domain/rollup_freshness.py` for what each
        of the three covers that the others cannot.
        """
        newest_bucket, computed_at = (
            await self._session.execute(
                _scoped(
                    select(
                        func.max(RollupCommunityHourModel.bucket),
                        func.max(RollupCommunityHourModel.computed_at),
                    ),
                    RollupCommunityHourModel,
                )
            )
        ).one()
        pending_since = await self._session.scalar(
            _scoped(select(func.min(RollupDirtyModel.marked_at)), RollupDirtyModel)
        )
        return RollupWatermarks(
            newest_bucket=newest_bucket, computed_at=computed_at, pending_since=pending_since
        )

    # ---- settings (build step 6) ----------------------------------------

    async def get_settings(self) -> CommunityLiveSettingsModel | None:
        """The community's settings row, or None when it has never been saved.

        None rather than a lazily-inserted default. A GET that writes breaks on a
        read replica, puts an audit row against a manager who merely opened a
        panel, and - worst - freezes today's platform default into a row, so a
        later change to that default silently does not apply to them.
        """
        stmt = _scoped(select(CommunityLiveSettingsModel), CommunityLiveSettingsModel)
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def upsert_settings(
        self, *, members_see_production: bool, members_see_aggregate: bool, k: int
    ) -> CommunityLiveSettingsModel:
        """Full replacement, one statement.

        INSERT ... ON CONFLICT DO UPDATE rather than read-modify-write: two
        managers saving the panel at once would otherwise interleave and one
        would silently lose the other's change.
        """
        stmt = (
            pg_insert(CommunityLiveSettingsModel)
            .values(
                id_community=tenant_id(),
                members_see_production=members_see_production,
                members_see_aggregate=members_see_aggregate,
                k=k,
            )
            .on_conflict_do_update(
                index_elements=[CommunityLiveSettingsModel.id_community],
                set_={
                    "members_see_production": members_see_production,
                    "members_see_aggregate": members_see_aggregate,
                    "k": k,
                },
            )
            .returning(CommunityLiveSettingsModel)
        )
        return (await self._session.execute(stmt)).scalar_one()

    async def production_chains(self) -> list[ProductionChain]:
        """The distinct CRM production chains among this community's devices.

        Reads `device_forecast_method` nothing and the CRM nothing: the chain was
        snapshotted onto the device at creation, alongside `capacity_kva`, for the
        same reason - so a read path does not need a cross-database round trip per
        device.

        Build step 9 uses it to decide whether ANY registered method could serve
        this community. Returns `[]` for a community with no devices, which is
        answered with the same named reason as "no method supports your chain" -
        from the caller's side they are the same situation.
        """
        stmt = _scoped(select(DeviceModel.production_chain).distinct(), DeviceModel).where(
            DeviceModel.production_chain.is_not(None)
        )
        raw = (await self._session.execute(stmt)).scalars().all()
        # A value this build does not recognise is DROPPED rather than passed
        # through as a bare int. `ProductionChain` mirrors an enum owned by
        # crm-backend, so a chain added there before it is added here arrives as
        # an integer nothing supports - and silently matching no method is the
        # correct answer, where crashing the summary is not.
        chains = []
        for value in raw:
            if value is None:  # already excluded by the WHERE; belt and braces
                continue
            try:
                chains.append(ProductionChain(value))
            except ValueError:
                logger.warning("unknown production_chain %s on a device - ignored", value)
        return chains

    # ---- the read surface (build step 6) --------------------------------

    async def latest_closed_community_hour(self) -> RollupCommunityHourModel | None:
        """The most recent community hour that is CLOSED - recomputed after it ended.

        The summary card says "last full hour". It used to read the newest hour,
        partial or not: the tick rewrites the hour in progress every 15 minutes,
        so from about :16 past every hour the card showed a quarter or two and
        dropped at every turn of the hour, reading as a fall in production.

        `computed_at >= bucket + 1 hour` rather than `bucket + 1 hour <= now()`.
        The wall clock would call an hour closed at :00 while its row still held
        the three quarters of its last recompute, until the tick after :00 adds
        the fourth. The criterion used here is the rollup's own statement that
        it has seen the hour end.
        """
        stmt = (
            _scoped(select(RollupCommunityHourModel), RollupCommunityHourModel)
            .where(
                RollupCommunityHourModel.computed_at
                >= RollupCommunityHourModel.bucket + text("INTERVAL '1 hour'")
            )
            .order_by(RollupCommunityHourModel.bucket.desc())
            .limit(1)
        )
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def instantaneous_power_w(self) -> Row | None:
        """Watts across the community, from the LAST COMPLETE INTERVAL.

        Two figures, `production_w` and `export_w`, and the service decides which
        may be shown. This used to return `SUM(COALESCE(production, export))`,
        which published a prosumer community's EXPORT - a grid term - with no k
        check at all (found 2026-10-04). Export is now the caller's to gate.

        `W = wh * 3600 / interval_s`, per plan 11.1, and NEVER the payload's
        `power_w`. That field is optional in the protocol and is normally absent
        for a P1 connector, which only differences index registers - so a summary
        sourced from it is blank over a full table, for every real deployment.

        `interval_s` is read per row rather than assumed, because it is a stored
        column and the arithmetic must match the row it came from even if a
        device sends something unexpected.
        """
        newest = await self._session.scalar(
            _scoped(select(func.max(MeasurementModel.ts)), MeasurementModel)
        )
        if newest is None:
            return None
        stmt = (
            _scoped(
                select(
                    func.sum(
                        MeasurementModel.production_wh * 3600.0 / MeasurementModel.interval_s
                    ).label("production_w"),
                    func.sum(
                        MeasurementModel.export_wh * 3600.0 / MeasurementModel.interval_s
                    ).label("export_w"),
                    MeasurementModel.ts.label("ts"),
                ),
                MeasurementModel,
            )
            .where(MeasurementModel.ts == newest)
            .group_by(MeasurementModel.ts)
        )
        return (await self._session.execute(stmt)).one_or_none()

    async def device_counts(self, *, now: datetime.datetime, silent_after_hours: int = 24) -> Row:
        """Total, online, and never-seen device counts in one pass.

        A LEFT JOIN onto `device_last`, because a device that has never reported
        HAS NO ROW THERE - and it is precisely the device an administrator opened
        this page to find. The tenant predicate therefore sits in the JOIN's ON
        clause; putting it in the WHERE would turn the outer join into an inner
        one and drop every never-seen device, leaving a list that renders
        perfectly and is missing the problem.
        """
        silent_before = now - datetime.timedelta(hours=silent_after_hours)
        stmt = _scoped_join(
            select(
                func.count(DeviceModel.id).label("total"),
                func.count(DeviceLastModel.id_device)
                .filter(DeviceLastModel.online.is_(True))
                .label("online"),
                func.count(DeviceModel.id)
                .filter(DeviceLastModel.last_seen_at.is_(None))
                .label("never_seen"),
                func.count(DeviceModel.id)
                .filter(DeviceLastModel.last_seen_at < silent_before)
                .label("silent"),
            ).select_from(
                join(
                    DeviceModel,
                    DeviceLastModel,
                    and_(
                        DeviceModel.id == DeviceLastModel.id_device,
                        # SCOPED IN THE ON CLAUSE. See the docstring.
                        DeviceLastModel.id_community == DeviceModel.id_community,
                    ),
                    isouter=True,
                )
            ),
            DeviceModel,
        ).where(DeviceModel.status != int(DeviceStatus.REVOKED))
        return (await self._session.execute(stmt)).one()

    async def community_hours(
        self, start: datetime.datetime, end: datetime.datetime
    ) -> Sequence[Row]:
        """Hour buckets in `[start, end)`, ascending."""
        stmt = (
            _scoped(
                select(
                    RollupCommunityHourModel.bucket,
                    RollupCommunityHourModel.import_wh,
                    RollupCommunityHourModel.export_wh,
                    RollupCommunityHourModel.production_wh,
                    RollupCommunityHourModel.n_members,
                    RollupCommunityHourModel.n_devices,
                ),
                RollupCommunityHourModel,
            )
            .where(
                RollupCommunityHourModel.bucket >= start,
                RollupCommunityHourModel.bucket < end,
            )
            .order_by(RollupCommunityHourModel.bucket)
        )
        return (await self._session.execute(stmt)).all()

    async def community_days(
        self, start: datetime.datetime, end: datetime.datetime
    ) -> Sequence[Row]:
        """Day buckets in `[start, end)`, ascending."""
        stmt = (
            _scoped(
                select(
                    RollupCommunityDayModel.bucket,
                    RollupCommunityDayModel.import_wh,
                    RollupCommunityDayModel.export_wh,
                    RollupCommunityDayModel.production_wh,
                    RollupCommunityDayModel.n_members,
                    RollupCommunityDayModel.n_members_min,
                    RollupCommunityDayModel.n_devices,
                ),
                RollupCommunityDayModel,
            )
            .where(
                RollupCommunityDayModel.bucket >= start,
                RollupCommunityDayModel.bucket < end,
            )
            .order_by(RollupCommunityDayModel.bucket)
        )
        return (await self._session.execute(stmt)).all()

    async def community_quarters(
        self, start: datetime.datetime, end: datetime.datetime
    ) -> Sequence[Row]:
        """Quarter-hour buckets, straight off `measurement`.

        The only resolution with no rollup behind it, and the only one whose
        `n_members` has to be borrowed: membership is resolved per COMMUNITY-HOUR,
        because `n_members` cannot be computed for a single device or a single
        quarter. Each quarter therefore inherits the k verdict of the hour that
        contains it, which is the conservative direction - a quarter can never be
        published on weaker evidence than its hour.

        `ts` is the END of the interval, so the containing hour is `ts - 1 second`
        truncated - `domain.buckets.bucket_sql`, shared with the tick and with
        ingest rather than written a third time here.
        """
        hour_of_ts = text(buckets.bucket_sql("measurement.ts"))
        stmt = (
            _scoped(
                select(
                    MeasurementModel.ts.label("bucket"),
                    func.sum(MeasurementModel.import_wh).label("import_wh"),
                    func.sum(MeasurementModel.export_wh).label("export_wh"),
                    func.sum(MeasurementModel.production_wh).label("production_wh"),
                    func.min(RollupCommunityHourModel.n_members).label("n_members"),
                    func.count(MeasurementModel.id_device).label("n_devices"),
                ),
                MeasurementModel,
            )
            .select_from(
                join(
                    MeasurementModel,
                    RollupCommunityHourModel,
                    and_(
                        RollupCommunityHourModel.bucket == hour_of_ts,
                        # SCOPED IN THE ON CLAUSE, for the same reason as
                        # `device_counts`: a quarter whose hour has not been
                        # rolled up yet must still appear, with n_members NULL -
                        # which `domain.kanon` then treats as "suppress".
                        RollupCommunityHourModel.id_community == MeasurementModel.id_community,
                    ),
                    isouter=True,
                )
            )
            .where(MeasurementModel.ts > start, MeasurementModel.ts <= end)
            .group_by(MeasurementModel.ts)
            .order_by(MeasurementModel.ts)
        )
        return (await self._session.execute(stmt)).all()

    # ---- sharing operations (D-14) ---------------------------------------

    async def operation_hours(
        self,
        start: datetime.datetime,
        end: datetime.datetime,
        *,
        operation: int | None = None,
    ) -> Sequence[Row]:
        """Operation-hour rows in `[start, end)`, ascending.

        EVERY row of the community, the remainder included, unless `operation`
        names one: the community's own verdict needs them all (domain/kanon.py).
        """
        model = RollupOperationHourModel
        stmt = (
            _scoped(
                select(
                    model.id_sharing_operation,
                    model.bucket,
                    model.import_wh,
                    model.export_wh,
                    model.production_wh,
                    model.shared_wh,
                    model.n_devices,
                    model.n_devices_production,
                    model.n_members,
                    model.computed_at,
                ),
                model,
            )
            .where(model.bucket >= start, model.bucket < end)
            .order_by(model.bucket, model.id_sharing_operation)
        )
        if operation is not None:
            stmt = stmt.where(model.id_sharing_operation == operation)
        return (await self._session.execute(stmt)).all()

    async def operation_days(
        self,
        start: datetime.datetime,
        end: datetime.datetime,
        *,
        operation: int | None = None,
    ) -> Sequence[Row]:
        """Operation-day rows in `[start, end)`, ascending, with `n_members_min`."""
        model = RollupOperationDayModel
        stmt = (
            _scoped(
                select(
                    model.id_sharing_operation,
                    model.bucket,
                    model.import_wh,
                    model.export_wh,
                    model.production_wh,
                    model.shared_wh,
                    model.n_devices,
                    model.n_devices_production,
                    model.n_members,
                    model.n_members_min,
                ),
                model,
            )
            .where(model.bucket >= start, model.bucket < end)
            .order_by(model.bucket, model.id_sharing_operation)
        )
        if operation is not None:
            stmt = stmt.where(model.id_sharing_operation == operation)
        return (await self._session.execute(stmt)).all()

    async def operation_quarters(
        self,
        start: datetime.datetime,
        end: datetime.datetime,
        *,
        operation: int | None = None,
    ) -> Sequence[Row]:
        """Quarter-hour sums per operation, straight off `measurement`.

        Attributed exactly as the tick attributes an hour: each (device, quarter)
        to the ONE non-ambiguous window covering its HOUR bucket's Brussels date,
        or to the remainder. Grouped back to one row per reading before anything
        is summed - rollup invariant 4, the same `COUNT(w.id) = 1` rule as
        worker/rollups.py - so a fan-out cannot reach an energy sum.

        `shared_wh` here IS the per-quarter figure the hour sums: LEAST of the
        operation's export and import at this one `ts`. `n_members` is not
        computed: a quarter inherits its hour's verdict, from `operation_hours`.

        Uses the windows as they are NOW, where the hour rows used them as of the
        last tick; the two agree except for a few minutes after a refresh.
        """
        hour_of_ts = literal_column(
            buckets.bucket_sql("measurement.ts"), type_=TIMESTAMP(timezone=True)
        )
        bucket_date = cast(func.timezone(ROLLUP_DAY_TIMEZONE, hour_of_ts), Date)
        window = DeviceOwnerWindowModel
        open_ended = literal_column("'infinity'::date", type_=Date)
        attributed = (
            _scoped(
                select(
                    MeasurementModel.id_device,
                    MeasurementModel.ts,
                    MeasurementModel.import_wh,
                    MeasurementModel.export_wh,
                    MeasurementModel.production_wh,
                    case(
                        (
                            func.count(window.id) == 1,
                            func.coalesce(
                                func.min(window.id_sharing_operation), NO_SHARING_OPERATION
                            ),
                        ),
                        else_=NO_SHARING_OPERATION,
                    ).label("id_op"),
                ).select_from(
                    join(
                        MeasurementModel,
                        DeviceModel,
                        and_(
                            DeviceModel.id == MeasurementModel.id_device,
                            DeviceModel.id_community == MeasurementModel.id_community,
                        ),
                    ).outerjoin(
                        window,
                        and_(
                            window.ean == DeviceModel.ean,
                            window.id_community == DeviceModel.id_community,
                            window.ambiguous.is_(False),
                            bucket_date.between(
                                window.valid_from, func.coalesce(window.valid_to, open_ended)
                            ),
                        ),
                    )
                ),
                MeasurementModel,
            )
            .where(MeasurementModel.ts > start, MeasurementModel.ts <= end)
            .group_by(
                MeasurementModel.id_device,
                MeasurementModel.ts,
                MeasurementModel.import_wh,
                MeasurementModel.export_wh,
                MeasurementModel.production_wh,
            )
            .subquery()
        )
        stmt = (
            select(
                attributed.c.id_op.label("id_sharing_operation"),
                attributed.c.ts.label("bucket"),
                func.sum(attributed.c.import_wh).label("import_wh"),
                func.sum(attributed.c.export_wh).label("export_wh"),
                func.sum(attributed.c.production_wh).label("production_wh"),
                func.least(
                    func.sum(attributed.c.export_wh), func.sum(attributed.c.import_wh)
                ).label("shared_wh"),
                func.count().label("n_devices"),
                func.count(attributed.c.production_wh).label("n_devices_production"),
            )
            .group_by(attributed.c.id_op, attributed.c.ts)
            .order_by(attributed.c.ts, attributed.c.id_op)
        )
        if operation is not None:
            stmt = stmt.where(attributed.c.id_op == operation)
        return (await self._session.execute(stmt)).all()

    async def latest_closed_operation_hour(self, operation: int) -> Row | None:
        """The operation's most recent CLOSED hour - the community summary's rule."""
        model = RollupOperationHourModel
        stmt = (
            _scoped(
                select(
                    model.bucket,
                    model.import_wh,
                    model.export_wh,
                    model.production_wh,
                    model.shared_wh,
                    model.n_devices,
                    model.n_members,
                ),
                model,
            )
            .where(
                model.id_sharing_operation == operation,
                model.computed_at >= model.bucket + text("INTERVAL '1 hour'"),
            )
            .order_by(model.bucket.desc())
            .limit(1)
        )
        return (await self._session.execute(stmt)).one_or_none()

    async def projected_operation_ids(self) -> list[int]:
        """Every operation a monitored meter of this community is, or was, in.

        From the ownership projection, so an operation appears as soon as a
        device's window names it - before any of its readings is rolled up.
        """
        window = DeviceOwnerWindowModel
        stmt = _scoped(select(distinct(window.id_sharing_operation)), window).where(
            window.id_sharing_operation.is_not(None)
        )
        ids = (await self._session.execute(stmt)).scalars()
        return sorted(int(op) for op in ids if op is not None)

    async def operation_device_counts(self, *, today: datetime.date) -> dict[int, int]:
        """Devices (not revoked) whose meter is in each operation TODAY - the
        numerator of the coverage line next to the shared estimate."""
        window = DeviceOwnerWindowModel
        stmt = (
            _scoped(
                select(
                    window.id_sharing_operation,
                    func.count(distinct(DeviceModel.id)).label("n_devices"),
                ).select_from(
                    join(
                        DeviceModel,
                        window,
                        and_(
                            window.ean == DeviceModel.ean,
                            window.id_community == DeviceModel.id_community,
                        ),
                    )
                ),
                DeviceModel,
            )
            .where(
                DeviceModel.status != int(DeviceStatus.REVOKED),
                window.ambiguous.is_(False),
                window.id_sharing_operation.is_not(None),
                window.valid_from <= today,
                or_(window.valid_to.is_(None), window.valid_to >= today),
            )
            .group_by(window.id_sharing_operation)
        )
        rows = (await self._session.execute(stmt)).all()
        return {int(row[0]): int(row[1]) for row in rows}


class EnrolmentRepository:
    """The PUBLIC leg's reads and writes. Separate class, deliberately.

    Nothing here may use `_scoped` or `tenant_id()`: on this leg
    the tenant ContextVar is unset and the headers that would populate it are
    attacker-supplied and blanked by nginx. Keeping these methods in their own
    class - rather than as `_unscoped` outliers among scoped ones - means a
    reviewer sees the boundary rather than having to notice its absence.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def claim_token(
        self, *, token_hash: str, now: datetime.datetime, lease_seconds: int
    ) -> EnrollmentTokenModel | None:
        """Take the claim lease in ONE conditional UPDATE. None when unclaimable.

        UNSCOPED, and safe: `token_hash` is UNIQUE over 256 bits of hash space,
        so this matches at most one row and the caller cannot steer which.

        ONE statement, so there is no SELECT-then-UPDATE window two concurrent
        enrolments could both pass through - which matters here because the
        window is exactly what would let a token be spent twice.

        The predicate is the whole state machine:
          * not consumed        - single use
          * not expired         - 72 hours
          * no live claim       - a claim within the lease is someone else's
        """
        deadline = now + datetime.timedelta(seconds=lease_seconds)
        stmt = (
            update(EnrollmentTokenModel)
            .where(
                EnrollmentTokenModel.token_hash == token_hash,
                EnrollmentTokenModel.consumed_at.is_(None),
                EnrollmentTokenModel.expires_at > now,
                (EnrollmentTokenModel.claimed_until.is_(None))
                | (EnrollmentTokenModel.claimed_until < now),
            )
            .values(claimed_until=deadline)
            .returning(EnrollmentTokenModel)
        )
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def has_live_claim(self, *, token_hash: str, now: datetime.datetime) -> bool:
        """Is a CONCURRENT enrolment holding this token's lease right now?

        NOT "does a row exist". That was the first implementation and it was a
        token oracle: any row - expired, consumed, anything - answered True, so
        the caller returned 409 instead of the opaque 400 and a guesser learned
        that their guess named a REAL token. The whole point of one opaque
        answer is that nothing distinguishes a hit from a miss.

        So the only state worth separating out is the genuinely concurrent one,
        where 409 + Retry-After is actionable and where the caller demonstrably
        already holds a valid token. Everything else - unknown, expired,
        consumed, malformed - collapses into the same 400.
        """
        stmt = select(EnrollmentTokenModel.id).where(
            EnrollmentTokenModel.token_hash == token_hash,
            EnrollmentTokenModel.consumed_at.is_(None),
            EnrollmentTokenModel.expires_at > now,
            EnrollmentTokenModel.claimed_until.is_not(None),
            EnrollmentTokenModel.claimed_until > now,
        )
        return (await self._session.execute(stmt)).first() is not None

    async def get_device(self, id_device: int) -> DeviceModel | None:
        """The device a claimed token points at. UNSCOPED for the same reason.

        The tenant is DISCOVERED here and passed explicitly from this point on;
        it is never read from a header on this leg.
        """
        return (
            await self._session.execute(select(DeviceModel).where(DeviceModel.id == id_device))
        ).scalar_one_or_none()
