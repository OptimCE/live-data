"""The read surface: summary, series, settings. Build step 6.

A separate service from `LiveDataService`, which owns the device LIFECYCLE. The
split is not filing: the lifecycle service drives the broker, commits, and writes
audit rows, and none of that belongs on a path a MEMBER can reach. Keeping them
apart means "can a member's request reach the dynsec control connection?" is a
question about a module rather than about a method.

The reads touch no broker and commit nothing except `PUT /settings`.
"""

from __future__ import annotations

import datetime
import logging
import uuid
from typing import TYPE_CHECKING

from api.live import visibility as vis
from api.live.repository import tenant_id
from api.live.schemas import (
    AbsentTerm,
    DeviceStatusOut,
    ForecastMethodOut,
    ForecastOut,
    LiveSettingsOut,
    LiveSettingsUpdate,
    MemberOperationOut,
    MemberOperationPointOut,
    MemberOperationSeriesOut,
    OperationOut,
    OperationSummaryOut,
    OpsHealthOut,
    SeriesOut,
    SeriesPointOut,
    SummaryOut,
)
from core.audit_log.actions import AuditActions
from core.audit_log.dtos import AuditLogInput
from core.audit_log.service import AuditLogService
from core.config import settings
from core.context_vars import current_user_id
from core.errors.errors import ErrorException
from domain import buckets, device_health, kanon, rollup_freshness
from domain.ownership import local_date_of
from domain.reasons import RejectReason, RejectScope, scope_of
from domain.windows import MAX_POINTS, Resolution, Window, WindowError, WindowProblem
from forecasting.registry import registry
from ports.crm_operations import CrmOperationsReadPort, OperationRef
from shared.const import DeviceStatus, DeviceType
from shared.custom_errors import errors

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import AsyncSession

    from api.live.repository import LiveRepository, RollupWatermarks

logger = logging.getLogger(__name__)

AUDIT_SOURCE = "live-data"

# WindowError -> the error the caller sees. A mapping rather than a chain of
# `if`s, so a new `WindowProblem` with no entry raises a KeyError in the tests
# rather than falling through to a 500 in production.
_WINDOW_ERRORS = {
    WindowProblem.UNKNOWN_RESOLUTION: errors.live.UNKNOWN_RESOLUTION,
    WindowProblem.NOT_SNAPPED: errors.live.WINDOW_NOT_SNAPPED,
    WindowProblem.TOO_LARGE: errors.live.WINDOW_TOO_LARGE,
    WindowProblem.INVERTED: errors.live.WINDOW_NOT_SNAPPED,
}


def window_error_to_http(exc: WindowError) -> ErrorException:
    """422 for every window problem. Never a silent correction - plan 9.3."""
    return ErrorException(_WINDOW_ERRORS[exc.problem], status_code=422)


# How far back `n_devices_readings_rejected_24h` looks - the same day the dead
# letter count covers, so the two lines of the Ops card describe one period.
REJECTION_WINDOW = datetime.timedelta(hours=24)


def _minutes(delta: datetime.timedelta | None) -> float | None:
    return None if delta is None else round(delta.total_seconds() / 60.0, 1)


def _freshness(marks: RollupWatermarks, *, now: datetime.datetime) -> rollup_freshness.Verdict:
    """The one verdict both `/summary` and `/ops/health` carry.

    `settings.ROLLUP_TICK_MINUTES` is the SCHEDULER's setting read by the API:
    the two containers must agree on it, or the limit is measured in the wrong
    ticks. See core/config.py.
    """
    return rollup_freshness.classify(
        now=now,
        newest_bucket=marks.newest_bucket,
        computed_at=marks.computed_at,
        pending_since=marks.pending_since,
        tick_minutes=settings.ROLLUP_TICK_MINUTES,
    )


def _is_revoked(row) -> bool:
    return DeviceStatus(row.DeviceModel.status) is DeviceStatus.REVOKED


def _drops_readings(reason: str | None) -> bool:
    """Whether a stored reject reason is MEASUREMENT-scoped: one reading dropped,
    the rest of its batch stored, and so no dead letter to count it by."""
    if reason is None:
        return False
    try:
        return scope_of(RejectReason(reason)) is RejectScope.MEASUREMENT
    except ValueError:
        # A reason this build does not know - the worker is deployed separately.
        # Not counted, and never a 500 on the page that would explain it.
        return False


def _readings_rejected(rows: Sequence, *, now: datetime.datetime) -> int:
    cutoff = now - REJECTION_WINDOW
    return sum(
        1
        for row in rows
        if row.last_reject_at is not None
        and row.last_reject_at >= cutoff
        and _drops_readings(row.last_reject_reason)
    )


# ---- the shared vocabulary of the read paths (D-14) ------------------------

_HOUR = datetime.timedelta(hours=1)


def _group(rows: Sequence) -> dict[datetime.datetime, list]:
    """Operation rows by bucket. Rows arrive ordered by bucket already."""
    grouped: dict[datetime.datetime, list] = {}
    for row in rows:
        grouped.setdefault(row.bucket, []).append(row)
    return grouped


def _hour_scopes(rows: Sequence) -> list[kanon.ScopeMembers]:
    return [
        kanon.ScopeMembers(row.id_sharing_operation, row.n_members, row.n_members) for row in rows
    ]


def _day_scopes(rows: Sequence) -> list[kanon.ScopeMembers]:
    return [
        kanon.ScopeMembers(row.id_sharing_operation, row.n_members_min, row.n_members)
        for row in rows
    ]


def _shared_sum(rows: Sequence) -> float:
    """The community's estimated shared energy: the sum over its operations,
    never a LEAST over the community - members of two operations share nothing
    with each other. The remainder row shares nothing by definition."""
    return float(
        sum(row.shared_wh or 0.0 for row in rows if row.id_sharing_operation != kanon.REMAINDER)
    )


def _verdict_key(bucket: datetime.datetime, resolution: Resolution) -> datetime.datetime:
    """The bucket a point is JUDGED in. A quarter (`bucket` = its `ts`, an
    interval end) inherits its hour's verdict; hours and days are their own."""
    return buckets.bucket_of(bucket) if resolution is Resolution.QUARTER else bucket


def _absent_grid(*, shared: bool) -> list[AbsentTerm]:
    terms = list(kanon.ABSENT_GRID) + ([kanon.ABSENT_SHARED] if shared else [])
    return [AbsentTerm(term=item.term, reason=item.reason.value) for item in terms]


def _series_out(
    window: Window, points: list[SeriesPointOut], suppressed: int, *, shared: bool
) -> SeriesOut:
    """Cap from the NEWEST end and name what is absent. Keeping the oldest would
    render "the last 30 days" as days 1-15 and look like a fleet that died."""
    cap = MAX_POINTS[window.resolution]
    truncated = len(points) > cap
    if truncated:
        points = points[-cap:]
    absent = [
        AbsentTerm(
            term=kanon.ABSENT_CONSUMPTION.term,
            reason=kanon.ABSENT_CONSUMPTION.reason.value,
        )
    ]
    if suppressed:
        absent.extend(_absent_grid(shared=shared))
    return SeriesOut(
        resolution=window.resolution.value,
        start=window.start,
        end=window.end,
        points=points,
        suppressed_buckets=suppressed,
        truncated=truncated,
        cap=cap,
        absent=absent,
    )


class LiveReadService:
    def __init__(
        self,
        *,
        local_session: AsyncSession,
        crm_session: AsyncSession,
        repository: LiveRepository,
        crm_operations: CrmOperationsReadPort,
    ) -> None:
        self._local = local_session
        self._crm = crm_session
        self._repo = repository
        self._crm_operations = crm_operations
        self._audit = AuditLogService(crm_session)

    # ---- visibility -----------------------------------------------------

    async def _visibility(self) -> vis.LiveVisibility:
        return vis.resolve(await self._repo.get_settings())

    # ---- summary --------------------------------------------------------

    async def summary(self, *, now: datetime.datetime) -> SummaryOut:
        """The community as of its last CLOSED hour. MANAGER only since D-14."""
        visibility = await self._visibility()
        vis.require_aggregate_visible(visibility)
        k = vis.k_for(visibility)

        marks = await self._repo.rollup_watermarks()
        verdict = _freshness(marks, now=now)
        latest = await self._repo.latest_closed_community_hour()
        counts = await self._repo.device_counts(now=now)

        absent: list[AbsentTerm] = [
            AbsentTerm(
                term=kanon.ABSENT_CONSUMPTION.term,
                reason=kanon.ABSENT_CONSUMPTION.reason.value,
            )
        ]

        out = SummaryOut(
            n_devices=counts.total or 0,
            n_devices_online=counts.online or 0,
            n_devices_never_seen=counts.never_seen or 0,
            n_devices_silent=counts.silent or 0,
            power_w=await self._power_w(k),
            rollup_freshness=verdict.state,
            rollup_lag_minutes=_minutes(verdict.lag),
            absent=absent,
        )
        if latest is None:
            # No CLOSED hour yet. Everything energy-shaped is absent and says so,
            # rather than being reported as zero - a community that has just been
            # created has not produced nothing, it has not been measured. And one
            # that HAS been measured, in its first hour, must not be told its
            # meters cannot see production: that hour simply has not ended.
            measured = marks.newest_bucket is not None or marks.pending_since is not None
            reason = (
                kanon.AbsentReason.NO_CLOSED_HOUR_YET
                if measured
                else kanon.AbsentReason.NOT_MEASURED
            )
            out.absent.extend(
                AbsentTerm(term=term, reason=reason.value)
                for term in ("production_wh", "import_wh", "export_wh", "shared_wh")
            )
            return out

        out.bucket = latest.bucket
        out.n_members = latest.n_members
        # ALWAYS, at any k. Decided 2026-09-16 - see domain/kanon.py.
        out.production_wh = latest.production_wh
        if latest.production_wh is None:
            out.absent.append(
                AbsentTerm(term="production_wh", reason=kanon.AbsentReason.NOT_MEASURED.value)
            )

        operations = await self._repo.operation_hours(latest.bucket, latest.bucket + _HOUR)
        if kanon.community_grid_is_visible(latest.n_members, _hour_scopes(operations), k):
            out.import_wh = latest.import_wh
            out.export_wh = latest.export_wh
            out.shared_wh = _shared_sum(operations)
        else:
            out.absent.extend(_absent_grid(shared=True))
        return out

    async def _power_w(self, k: int | None) -> float | None:
        """Watts from the last complete interval: production, or - for net meters
        that cannot see it - export, but only where the community's grid terms
        for that hour are visible. It used to publish export unconditionally."""
        power = await self._repo.instantaneous_power_w()
        if power is None:
            return None
        if power.production_w is not None:
            return float(power.production_w)
        hour = buckets.bucket_of(power.ts)
        community = await self._repo.community_hours(hour, hour + _HOUR)
        if not community:
            return None
        operations = await self._repo.operation_hours(hour, hour + _HOUR)
        if kanon.community_grid_is_visible(community[0].n_members, _hour_scopes(operations), k):
            return None if power.export_w is None else float(power.export_w)
        return None

    # ---- series ---------------------------------------------------------

    async def series(self, window: Window) -> SeriesOut:
        """The community series. MANAGER only since D-14.

        Grid terms (import, export and the shared estimate) are published per
        bucket only when the COMMUNITY verdict passes - its own k, and nothing
        identifiable left once its visible operations are subtracted from it.
        """
        visibility = await self._visibility()
        vis.require_aggregate_visible(visibility)
        k = vis.k_for(visibility)

        rows = await self._rows_for(window)
        verdicts, shared = await self._community_verdicts(window, rows, k)

        points: list[SeriesPointOut] = []
        suppressed = 0
        for row in rows:
            key = _verdict_key(row.bucket, window.resolution)
            point = SeriesPointOut(
                bucket=row.bucket,
                production_wh=row.production_wh,
                n_devices=row.n_devices or 0,
            )
            if verdicts.get(key, False):
                point.import_wh = row.import_wh
                point.export_wh = row.export_wh
                point.shared_wh = shared.get(row.bucket, 0.0)
            else:
                suppressed += 1
            points.append(point)
        return _series_out(window, points, suppressed, shared=True)

    async def _rows_for(self, window: Window) -> Sequence:
        """One mapping from resolution to source.

        `quarter` reads `measurement`; `hour` and `day` read the community
        rollups. A dict rather than a chain of `if`s, so adding a resolution to
        `domain.windows.Resolution` without a source here raises a KeyError in
        the tests rather than silently returning the wrong grid.
        """
        sources = {
            Resolution.QUARTER: self._repo.community_quarters,
            Resolution.HOUR: self._repo.community_hours,
            Resolution.DAY: self._repo.community_days,
        }
        return await sources[window.resolution](window.start, window.end)

    async def _community_verdicts(
        self, window: Window, rows: Sequence, k: int | None
    ) -> tuple[dict[datetime.datetime, bool], dict[datetime.datetime, float]]:
        """Per-bucket community verdicts, and the shared sum per point bucket.

        Keyed by HOUR for quarter resolution - a quarter inherits its hour's
        verdict, the conservative direction - and by the bucket itself otherwise.
        """
        if window.resolution is Resolution.DAY:
            operation_days = await self._repo.operation_days(window.start, window.end)
            by_bucket = _group(operation_days)
            verdicts = {
                row.bucket: kanon.community_grid_is_visible(
                    row.n_members_min, _day_scopes(by_bucket.get(row.bucket, [])), k
                )
                for row in rows
            }
            return verdicts, {b: _shared_sum(ops) for b, ops in by_bucket.items()}

        hours_start = buckets.hour_floor(window.start)
        if window.resolution is Resolution.HOUR:
            community_hours = rows
        else:
            community_hours = await self._repo.community_hours(hours_start, window.end)
        operation_hours = _group(await self._repo.operation_hours(hours_start, window.end))
        verdicts = {
            row.bucket: kanon.community_grid_is_visible(
                row.n_members, _hour_scopes(operation_hours.get(row.bucket, [])), k
            )
            for row in community_hours
        }
        if window.resolution is Resolution.HOUR:
            return verdicts, {b: _shared_sum(ops) for b, ops in operation_hours.items()}
        quarters = _group(await self._repo.operation_quarters(window.start, window.end))
        return verdicts, {ts: _shared_sum(ops) for ts, ops in quarters.items()}

    # ---- sharing operations: the manager's view (D-14) ------------------

    async def operations(self, *, now: datetime.datetime) -> list[OperationOut]:
        """The community's sharing operations that hold, or held, a monitored
        meter - with the coverage of the shared estimate."""
        ids = await self._repo.projected_operation_ids()
        if not ids:
            return []
        today = local_date_of(now)
        community = tenant_id()
        refs = await self._crm_operations.operations(id_community=community, ids=ids)
        devices = await self._repo.operation_device_counts(today=today)
        meters = await self._crm_operations.active_meter_counts(id_community=community, today=today)
        return [
            OperationOut(
                id=ref.id,
                name=ref.name,
                n_devices=devices.get(ref.id, 0),
                n_meters=meters.get(ref.id, 0),
            )
            for ref in refs
        ]

    async def _require_operation(self, operation: int) -> None:
        """404 unless this community's monitored meters are, or were, in it."""
        if operation not in await self._repo.projected_operation_ids():
            raise ErrorException(errors.live.OPERATION_NOT_FOUND, status_code=404)

    async def operation_summary(
        self, operation: int, *, now: datetime.datetime
    ) -> OperationSummaryOut:
        await self._require_operation(operation)
        k = vis.k_for(await self._visibility())
        out = OperationSummaryOut(id_sharing_operation=operation)
        today = local_date_of(now)
        out.n_meters = (
            await self._crm_operations.active_meter_counts(id_community=tenant_id(), today=today)
        ).get(operation, 0)
        latest = await self._repo.latest_closed_operation_hour(operation)
        if latest is None:
            out.absent.extend(
                AbsentTerm(term=term, reason=kanon.AbsentReason.NOT_MEASURED.value)
                for term in ("production_wh", "import_wh", "export_wh", "shared_wh")
            )
            return out
        out.bucket = latest.bucket
        out.n_devices = latest.n_devices or 0
        out.n_members = latest.n_members
        out.production_wh = latest.production_wh
        if latest.production_wh is None:
            out.absent.append(
                AbsentTerm(term="production_wh", reason=kanon.AbsentReason.NOT_MEASURED.value)
            )
        scope = kanon.ScopeMembers(operation, latest.n_members, latest.n_members)
        if kanon.operation_grid_is_visible(scope, k):
            out.import_wh = latest.import_wh
            out.export_wh = latest.export_wh
            out.shared_wh = latest.shared_wh
        else:
            out.absent.extend(_absent_grid(shared=True))
        return out

    async def operation_series(self, operation: int, window: Window) -> SeriesOut:
        """One operation's series, every term, each under the OPERATION's k."""
        await self._require_operation(operation)
        k = vis.k_for(await self._visibility())
        rows, verdicts = await self._operation_rows(operation, window, k)
        points: list[SeriesPointOut] = []
        suppressed = 0
        for row in rows:
            point = SeriesPointOut(
                bucket=row.bucket,
                production_wh=row.production_wh,
                n_devices=row.n_devices or 0,
            )
            if verdicts.get(_verdict_key(row.bucket, window.resolution), False):
                point.import_wh = row.import_wh
                point.export_wh = row.export_wh
                point.shared_wh = row.shared_wh
            else:
                suppressed += 1
            points.append(point)
        return _series_out(window, points, suppressed, shared=True)

    async def _operation_rows(
        self, operation: int, window: Window, k: int | None
    ) -> tuple[Sequence, dict[datetime.datetime, bool]]:
        """An operation's rows at the window's resolution, and its verdicts.

        Day: on its `n_members_min`. Hour: on its `n_members`. Quarter: the
        containing hour's verdict, like the community's.
        """
        if window.resolution is Resolution.DAY:
            rows = await self._repo.operation_days(window.start, window.end, operation=operation)
            verdicts = {
                row.bucket: kanon.operation_grid_is_visible(
                    kanon.ScopeMembers(operation, row.n_members_min, row.n_members), k
                )
                for row in rows
            }
            return rows, verdicts
        hours = await self._repo.operation_hours(
            buckets.hour_floor(window.start), window.end, operation=operation
        )
        verdicts = {
            row.bucket: kanon.operation_grid_is_visible(
                kanon.ScopeMembers(operation, row.n_members, row.n_members), k
            )
            for row in hours
        }
        if window.resolution is Resolution.HOUR:
            return hours, verdicts
        quarters = await self._repo.operation_quarters(
            window.start, window.end, operation=operation
        )
        return quarters, verdicts

    # ---- sharing operations: the member's view (D-14) -------------------

    async def my_operations(self, *, now: datetime.datetime) -> list[MemberOperationOut]:
        """The operations the caller's member(s) hold an ACTIVE meter in today.

        Behind the same visibility gate as every member-facing read, and 200
        with an empty list for a caller with no member link - a manager who
        holds no meter, typically. Never the community, never another operation.
        """
        vis.require_aggregate_visible(await self._visibility())
        held = await self._held(now)
        return [MemberOperationOut(id=ref.id, name=ref.name) for ref in held]

    async def my_operation_series(
        self, operation: int, window: Window, *, now: datetime.datetime
    ) -> MemberOperationSeriesOut:
        """One of the caller's own operations: production always, export only
        where the operation's meters cannot see production (protocol 3.4) and
        only under the operation's k. Never import, never the shared estimate."""
        visibility = await self._visibility()
        vis.require_aggregate_visible(visibility)
        if operation not in {ref.id for ref in await self._held(now)}:
            raise ErrorException(errors.live.OPERATION_NOT_FOUND, status_code=404)
        k = vis.k_for(visibility)
        rows, verdicts = await self._operation_rows(operation, window, k)
        points: list[MemberOperationPointOut] = []
        suppressed = 0
        for row in rows:
            point = MemberOperationPointOut(
                bucket=row.bucket,
                production_wh=row.production_wh,
                n_devices=row.n_devices or 0,
            )
            production_is_partial = (row.n_devices_production or 0) < (row.n_devices or 0)
            if production_is_partial:
                if verdicts.get(_verdict_key(row.bucket, window.resolution), False):
                    point.export_wh = row.export_wh
                else:
                    suppressed += 1
            points.append(point)

        cap = MAX_POINTS[window.resolution]
        truncated = len(points) > cap
        if truncated:
            points = points[-cap:]
        absent = []
        if suppressed:
            absent.append(
                AbsentTerm(term="export_wh", reason=kanon.AbsentReason.BELOW_K_THRESHOLD.value)
            )
        return MemberOperationSeriesOut(
            id_sharing_operation=operation,
            resolution=window.resolution.value,
            start=window.start,
            end=window.end,
            points=points,
            suppressed_buckets=suppressed,
            truncated=truncated,
            cap=cap,
            absent=absent,
        )

    async def _held(self, now: datetime.datetime) -> list[OperationRef]:
        auth_user_id = current_user_id.get()
        if auth_user_id is None:
            # require_min_role already refused an anonymous caller; this is the
            # second lock, never the first.
            raise ErrorException(errors.auth.FORBIDDEN, status_code=403)
        return await self._crm_operations.held_operations(
            id_community=tenant_id(), auth_user_id=auth_user_id, today=local_date_of(now)
        )

    # ---- forecast (build step 9) ----------------------------------------

    async def forecast(self) -> ForecastOut:
        """The seam. In phase 1 this is always empty, and says why.

        Gated by the SAME visibility check as the summary, and not by a new one.
        plan 9.1's warning applies here literally: this is the second read
        endpoint, and adding it with its own ad-hoc check is how the retrofit
        problem starts.
        """
        visibility = await self._visibility()
        vis.require_aggregate_visible(visibility)

        chains = await self._repo.production_chains()
        supported = [meta for chain in chains for meta in registry.for_chain(chain)]
        if not supported:
            # The criterion's exact reason string. It covers both shapes of
            # "nothing to forecast" - no method registered at all (phase 1), and
            # methods registered but none matching this community's chains -
            # because from the caller's side they are the same situation and the
            # same remediation.
            return ForecastOut(reason="no_method_for_production_chain")

        # Unreachable in phase 1: `methods_implemented/` is empty. Left explicit
        # rather than as a `pass`, so the shape of the answer is fixed before the
        # first method exists and the frontend is not written against a guess.
        chosen = supported[0]
        return ForecastOut(
            buckets=[],
            reason="no_forecast_computed",
            method=chosen.name,
            method_version=chosen.version,
        )

    async def forecast_methods(self) -> list[ForecastMethodOut]:
        """Every registered method. `[]` in phase 1, correctly."""
        return [
            ForecastMethodOut(
                name=meta.name,
                description=meta.description,
                version=meta.version,
                supports=[int(chain) for chain in meta.supports],
                required_weather_variables=list(meta.required_weather_variables),
                input_schema=meta.input_schema.model_json_schema(),
            )
            for meta in registry.list_all()
        ]

    # ---- ops (build step 11) --------------------------------------------

    def _to_status(
        self, row, *, now: datetime.datetime, ingest_healthy: bool = True
    ) -> DeviceStatusOut:
        """One row of the status join, with the coarsening applied.

        ---- the coarsening, and why it defaults ON for consumption ----
        plan 16: "`last_seen_at` bypasses `admin_can_view_individual`. 'This
        member's device has been offline for three days' is the absence signal
        the consent flag exists to withhold. For a consumption device whose
        member withheld individual visibility, coarsen the admin's view to
        online/offline with no timestamp."

        The `consent` table is phase 2 and inert, so there is no grant to read -
        which makes the safe default the only correct one: coarsen for EVERY
        consumption device. It also puts the protection in place BEFORE the path
        is reachable. `POST /devices` accepts `type: 2` today - "phase 1 enrols
        production only" is stated in three places and enforced in none - and the
        alternative was to add this at the moment somebody enrols the first
        consumption meter, which is the change that gets forgotten.
        """
        device = row.DeviceModel
        coarsen = DeviceType(device.type) is DeviceType.CONSUMPTION
        health = device_health.classify(
            now=now,
            status=DeviceStatus(device.status),
            last_seen_at=row.last_seen_at,
            online=row.online,
            energy_wh_recent=row.energy_recent_wh,
            ingest_healthy=ingest_healthy,
        )
        return DeviceStatusOut(
            device_id=device.public_id,
            name=device.name,
            type=DeviceType(device.type),
            status=DeviceStatus(device.status),
            ean=device.ean,
            connector_name=device.connector_name,
            connector_version=device.connector_version,
            health=health.value,
            hint=device_health.HINTS[health],
            online=row.online,
            diag=row.diag,
            diag_since=row.diag_since if not coarsen else None,
            # The timestamps are what disclose the household's rhythm. The
            # boolean does not.
            last_seen_at=row.last_seen_at if not coarsen else None,
            last_measurement_at=row.last_measurement_ts if not coarsen else None,
            last_reject_reason=row.last_reject_reason,
            last_reject_at=row.last_reject_at if not coarsen else None,
            energy_recent_wh=row.energy_recent_wh if not coarsen else None,
        )

    async def ops_health(self, *, now: datetime.datetime) -> OpsHealthOut:
        """The fleet, its rollup freshness, and what could not be stored."""
        rows = await self._repo.devices_with_status()
        # The fleet verdict FIRST, then every device read through it. A community
        # whose every reporting meter went quiet at the same moment did not lose
        # its meters; it lost the path they share, and calling forty devices
        # SILENT is a page-load's worth of false alarms pointing at the hardware.
        #
        # REVOKED devices are not evidence: revocation freezes `last_seen_at`, and
        # counting it made one silent meter plus one revoked device "two quiet
        # devices" - the floor of two was met and the dead meter read UNKNOWN.
        ingest_healthy = device_health.ingest_looks_healthy(
            now=now, last_seen=[row.last_seen_at for row in rows if not _is_revoked(row)]
        )
        # Every device is still LISTED, revoked ones included: their measurements
        # are still in the table and still need an owner (verify-live-ingest.sh L).
        devices = [self._to_status(row, now=now, ingest_healthy=ingest_healthy) for row in rows]

        by_health: dict[str, int] = {}
        for item in devices:
            by_health[item.health] = by_health.get(item.health, 0) + 1

        marks = await self._repo.rollup_watermarks()
        verdict = _freshness(marks, now=now)
        age = None
        if marks.newest_bucket is not None:
            age = round((now - marks.newest_bucket).total_seconds() / 60.0, 1)

        return OpsHealthOut(
            n_devices=len(devices),
            by_health=by_health,
            newest_rollup_bucket=marks.newest_bucket,
            rollup_age_minutes=age,
            rollup_freshness=verdict.state,
            rollup_lag_minutes=_minutes(verdict.lag),
            rollup_computed_at=marks.computed_at,
            rollup_pending_since=marks.pending_since,
            dead_letters_24h=await self._repo.dead_letter_count(),
            n_devices_readings_rejected_24h=_readings_rejected(rows, now=now),
            devices=devices,
        )

    async def diagnostics(self, public_id: uuid.UUID, *, now: datetime.datetime) -> DeviceStatusOut:
        """One device. 404 when it belongs to another community.

        404 rather than 403: telling a caller that a device exists elsewhere is
        itself a disclosure, and it is the same choice `get_device` already made.
        """
        row = await self._repo.device_with_status(public_id)
        if row is None:
            raise ErrorException(errors.live.DEVICE_NOT_FOUND, status_code=404)
        # One device cannot see its own fleet, and this page is exactly where an
        # operator decides whether to send an installer out. Answering SILENT
        # during an ingest outage sends them to a meter that is working.
        ingest_healthy = device_health.ingest_looks_healthy(
            now=now, last_seen=await self._repo.fleet_last_seen()
        )
        return self._to_status(row, now=now, ingest_healthy=ingest_healthy)

    # ---- settings -------------------------------------------------------

    async def get_settings(self) -> LiveSettingsOut:
        """The settings in force. DOES NOT INSERT when none exist."""
        visibility = vis.resolve(await self._repo.get_settings())
        return LiveSettingsOut(
            members_see_production=visibility.members_see_production,
            members_see_aggregate=visibility.members_see_aggregate,
            k=visibility.k,
            is_default=visibility.is_default,
        )

    async def update_settings(self, payload: LiveSettingsUpdate) -> LiveSettingsOut:
        """Full replacement, audited with the before AND after values.

        Both halves, because "the manager set k to 3" is not actionable on its
        own - the question a month later is always what it had been before.
        """
        before = vis.resolve(await self._repo.get_settings())
        row = await self._repo.upsert_settings(
            members_see_production=payload.members_see_production,
            members_see_aggregate=payload.members_see_aggregate,
            k=payload.k,
        )
        await self._local.commit()

        await self._audit_and_commit(
            AuditActions.SETTINGS_UPDATED,
            payload={
                "before": {
                    "members_see_production": before.members_see_production,
                    "members_see_aggregate": before.members_see_aggregate,
                    "k": before.k,
                    "is_default": before.is_default,
                },
                "after": {
                    "members_see_production": row.members_see_production,
                    "members_see_aggregate": row.members_see_aggregate,
                    "k": row.k,
                },
            },
        )
        return LiveSettingsOut(
            members_see_production=row.members_see_production,
            members_see_aggregate=row.members_see_aggregate,
            k=row.k,
            is_default=False,
        )

    async def _audit_and_commit(self, action: str, *, payload: dict) -> None:
        """Best effort, and it commits the CRM session ONLY.

        The business write is already committed above. An audit failure must not
        roll it back - and cannot, because the two sessions are separate - but it
        also must not surface as a 500 on an operation that succeeded.
        """
        try:
            await self._audit.log(
                AuditLogInput(
                    action=action,
                    source=AUDIT_SOURCE,
                    entity_type="live_settings",
                    entity_id=None,
                    payload=payload,
                )
            )
            await self._crm.commit()
        except Exception:
            logger.exception("audit commit failed for %s", action)
