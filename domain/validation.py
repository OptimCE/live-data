"""Validating a telemetry message. Pure: no session, no network, no clock of its own.

`now` and the device context are passed IN, so every rule is unit-testable
without a database, a broker or a frozen clock - and so that the worker and the
simulator can agree about what is valid without sharing anything but this module.

The shape of the result is the scope split from `domain/reasons.py`:

    MessageRejected      nothing is stored
    ValidatedBatch       the measurements that survived, plus per-reading
                         rejections and non-rejecting observations

Protocol 4.1: a rejection is silent, counted and logged. Nothing goes back to the
device - the flow is one-way and there is no command topic - so these counters
and `device_last.last_reject_reason` are the ONLY places a problem ever surfaces.
"""

import datetime
from dataclasses import dataclass, field
from typing import Final
from zoneinfo import ZoneInfo

from domain.partitions import is_aligned
from domain.protocol import MeasurementV1, TelemetryV1
from domain.reasons import ObservedCode, RejectReason

# Wh per kVA-hour. `capacity_kva * 1000` converts the AC injection ceiling to
# watts; multiplying by the interval in hours gives the energy ceiling.
_W_PER_KW: Final[int] = 1000
_SECONDS_PER_HOUR: Final[int] = 3600


@dataclass(frozen=True, slots=True)
class DeviceContext:
    """What validation needs to know about the publishing device.

    Loaded from the device row BEFORE any of this runs. `capacity_kva` is named
    with its unit because the CRM column it snapshots is not: it is the AC
    injection limit, not the DC panel peak, and a PV array is routinely
    oversized against its inverter (plan deviation 4).
    """

    id_device: int
    id_community: int
    max_wh_per_interval: float
    capacity_kva: float | None
    pure_injection: bool


@dataclass(frozen=True, slots=True)
class ValidationSettings:
    """The knobs, passed in rather than imported from `core.config`.

    Keeps this module importable by anything - including a test that wants a
    5-second acceptance window - and keeps `domain/` free of settings, which is
    the rule that lets the worker image stay lean.
    """

    max_future_seconds: int = 300
    max_age_days: int = 35
    interval_seconds: int = 900
    max_batch: int = 200
    capacity_tolerance: float = 1.1
    night_start_hour_local: int = 23
    night_end_hour_local: int = 4
    timezone: str = "Europe/Brussels"
    # The ceiling applied to a device whose own `max_wh_per_interval` is NULL.
    # `scripts/sql/schema.sql` documents the column as deferring to the setting,
    # and until this field existed that comment was false: ingest carried the
    # literal, so lowering INGEST_DEFAULT_MAX_WH_PER_INTERVAL moved nothing.
    default_max_wh_per_interval: float = 50_000.0


@dataclass(frozen=True, slots=True)
class RejectedMeasurement:
    ts: datetime.datetime | None
    reason: RejectReason
    detail: str


@dataclass(frozen=True, slots=True)
class MessageRejected:
    """The whole message is discarded."""

    reason: RejectReason
    detail: str


@dataclass(frozen=True, slots=True)
class ValidatedBatch:
    """What survived, and what did not."""

    accepted: list[MeasurementV1] = field(default_factory=list)
    rejected: list[RejectedMeasurement] = field(default_factory=list)
    # Counted, never rejected. Protocol 7's "ignored and counted" half, plus the
    # both-directions observation.
    observations: list[tuple[ObservedCode, str]] = field(default_factory=list)


def validate_batch(
    telemetry: TelemetryV1,
    device: DeviceContext,
    now: datetime.datetime,
    settings: ValidationSettings | None = None,
) -> MessageRejected | ValidatedBatch:
    """Apply protocol 4.2 to a parsed telemetry envelope.

    Parsing itself (and therefore `schema_invalid` and `unknown_field`) happens
    before this, at the Pydantic boundary: `TelemetryV1` is `extra="forbid"` and
    `MeasurementV1` is `extra="allow"`, which is protocol 7's asymmetry.
    """
    cfg = settings or ValidationSettings()

    # ---- message-scoped checks ------------------------------------------
    if len(telemetry.measurements) > cfg.max_batch:
        return MessageRejected(
            RejectReason.BATCH_TOO_LARGE,
            f"{len(telemetry.measurements)} measurements, limit {cfg.max_batch}",
        )
    if not telemetry.measurements:
        return MessageRejected(RejectReason.SCHEMA_INVALID, "empty measurements array")

    # Caught HERE rather than at the upsert: Postgres raises 21000 when one
    # statement touches the same row twice, which would abort the whole
    # transaction and leave the device retrying the identical batch for ever.
    seen: set[datetime.datetime] = set()
    for measurement in telemetry.measurements:
        if measurement.ts in seen:
            return MessageRejected(
                RejectReason.DUPLICATE_TS_IN_BATCH,
                f"{measurement.ts.isoformat()} appears more than once",
            )
        seen.add(measurement.ts)

    # ---- measurement-scoped checks --------------------------------------
    batch = ValidatedBatch()
    for measurement in telemetry.measurements:
        rejection = _check_measurement(measurement, device, now, cfg)
        if rejection is not None:
            batch.rejected.append(rejection)
            continue
        _observe(measurement, batch)
        batch.accepted.append(measurement)
    return batch


def _check_measurement(
    measurement: MeasurementV1,
    device: DeviceContext,
    now: datetime.datetime,
    cfg: ValidationSettings,
) -> RejectedMeasurement | None:
    ts = measurement.ts

    if ts > now + datetime.timedelta(seconds=cfg.max_future_seconds):
        return RejectedMeasurement(ts, RejectReason.TS_IN_FUTURE, f"{ts.isoformat()} > now")
    if ts < now - datetime.timedelta(days=cfg.max_age_days):
        # A drifted clock, not a backlog. The acceptance window and the raw
        # retention are coupled: retention MUST exceed this, or a legitimate
        # late message targets a dropped partition.
        return RejectedMeasurement(
            ts, RejectReason.TS_TOO_OLD, f"older than {cfg.max_age_days} days"
        )
    if not is_aligned(ts, cfg.interval_seconds):
        return RejectedMeasurement(
            ts, RejectReason.TS_NOT_ALIGNED, f"not on a {cfg.interval_seconds}s boundary"
        )

    for name, value in (
        ("import_wh", measurement.import_wh),
        ("export_wh", measurement.export_wh),
        ("production_wh", measurement.production_wh),
    ):
        if value is not None and value < 0:
            # A meter replacement resets the index. The connector is required to
            # mark that interval invalid and skip it, never to send the
            # difference across the reset (protocol 8.4).
            return RejectedMeasurement(ts, RejectReason.NEGATIVE_ENERGY, f"{name}={value}")

    # The ceiling applies to the SUM. This is what replaced
    # `both_directions_positive`: total energy through the connection in one
    # interval cannot exceed capacity x interval, whereas both registers
    # advancing is ordinary physics.
    total_wh = measurement.import_wh + measurement.export_wh
    if total_wh > device.max_wh_per_interval:
        return RejectedMeasurement(
            ts,
            RejectReason.OVER_DEVICE_CEILING,
            f"import+export={total_wh} > {device.max_wh_per_interval}",
        )

    implausible = _implausible_production(measurement, device, cfg)
    if implausible is not None:
        return RejectedMeasurement(ts, RejectReason.IMPLAUSIBLE_PRODUCTION, implausible)

    return None


def _implausible_production(
    measurement: MeasurementV1,
    device: DeviceContext,
    cfg: ValidationSettings,
) -> str | None:
    """protocol 4.2: production above capacity x interval, or production at night.

    Between them these catch a stolen credential injecting fabricated production
    into a signal members act on, a mis-set `pure_injection`, and a mis-wired
    inverter.
    """
    production = measurement.production_wh
    if production is None or production <= 0:
        return None

    if device.capacity_kva is not None:
        # CLIP, never trust: capacity_kva is the AC injection ceiling, so real
        # production can legitimately sit just under it and a DC-side model would
        # read high. The tolerance is what keeps a correctly-sized installation
        # out of the reject log.
        hours = measurement.interval_s / _SECONDS_PER_HOUR
        ceiling = device.capacity_kva * _W_PER_KW * hours * cfg.capacity_tolerance
        if production > ceiling:
            return f"production_wh={production} > capacity ceiling {ceiling:.1f}"

    if _is_night(measurement.ts, cfg):
        return f"production_wh={production} during the night window"
    return None


def _is_night(ts: datetime.datetime, cfg: ValidationSettings) -> bool:
    """A NAIVE local-hour window. Not a solar-position calculation, on purpose.

    Deliberately narrow (23:00-04:00 Europe/Brussels by default): Brussels has
    civil twilight past 22:00 in June, so a wider window would reject real
    midsummer production. It is a crude check for a crude failure - a device
    reporting output at 02:00 in January is not a borderline case.

    If this ever needs to be right rather than merely useful, it becomes a solar
    elevation calculation and a dependency, and that is a later decision.
    """
    local_hour = ts.astimezone(ZoneInfo(cfg.timezone)).hour
    start, end = cfg.night_start_hour_local, cfg.night_end_hour_local
    if start <= end:
        return start <= local_hour < end
    # The window wraps midnight, which is the normal case.
    return local_hour >= start or local_hour < end


def _observe(measurement: MeasurementV1, batch: ValidatedBatch) -> None:
    """Record what is worth counting but must never reject."""
    if measurement.import_wh > 0 and measurement.export_wh > 0:
        batch.observations.append(
            (
                ObservedCode.BOTH_DIRECTIONS_OBSERVED,
                f"import={measurement.import_wh} export={measurement.export_wh}",
            )
        )
    # Protocol 7's second rule. `MeasurementV1` is extra="allow" (never "ignore"),
    # which is what makes `model_extra` populated rather than None - and therefore
    # what makes this counter able to read anything but zero.
    for key in measurement.model_extra or {}:
        batch.observations.append((ObservedCode.UNKNOWN_MEASUREMENT_FIELD, key))
