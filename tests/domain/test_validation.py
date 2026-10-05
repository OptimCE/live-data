"""Validation, rule by rule - including the two the frozen protocol changed.

Every case here is a real failure mode from a real connector, not a
pathological input invented for coverage.
"""

import datetime

import pytest

from domain.protocol import TelemetryV1
from domain.reasons import ObservedCode, RejectReason
from domain.validation import (
    DeviceContext,
    MessageRejected,
    ValidatedBatch,
    ValidationSettings,
    validate_batch,
)

NOW = datetime.datetime(2026, 9, 15, 12, 0, tzinfo=datetime.UTC)
CFG = ValidationSettings()

DEVICE = DeviceContext(
    id_device=1,
    id_community=42,
    max_wh_per_interval=50_000.0,
    capacity_kva=10.0,
    pure_injection=True,
)


def _m(ts: datetime.datetime, **overrides) -> dict:
    payload = {
        "ts": ts.isoformat().replace("+00:00", "Z"),
        "interval_s": 900,
        "import_wh": 0.0,
        "export_wh": 0.0,
        "production_wh": None,
    }
    payload.update(overrides)
    return payload


def _telemetry(*measurements: dict) -> TelemetryV1:
    return TelemetryV1(v=1, measurements=list(measurements))


def _validate(*measurements: dict, device: DeviceContext = DEVICE, now=NOW):
    return validate_batch(_telemetry(*measurements), device, now, CFG)


class TestTheScopeSplit:
    """One bad reading must not destroy a backlog."""

    def test_a_drifted_reading_does_not_discard_the_rest_of_the_batch(self):
        """THE reason the scope column exists.

        Protocol 3.3 frames a 200-entry backlog as normal traffic and says the
        server accepts gaps. Whole-message rejection would discard 199 good
        readings for one bad one - repeatedly, since the device cannot learn it
        was rejected and re-sends the same batch.
        """
        good_a = _m(NOW - datetime.timedelta(minutes=30), import_wh=100.0)
        drifted = _m(NOW + datetime.timedelta(days=2), import_wh=100.0)
        good_b = _m(NOW - datetime.timedelta(minutes=15), import_wh=110.0)

        result = _validate(good_a, drifted, good_b)

        assert isinstance(result, ValidatedBatch)
        assert len(result.accepted) == 2
        assert [r.reason for r in result.rejected] == [RejectReason.TS_IN_FUTURE]

    def test_an_identity_fault_does_discard_the_whole_message(self):
        ts = NOW - datetime.timedelta(minutes=15)
        result = _validate(_m(ts), _m(ts))
        assert isinstance(result, MessageRejected)
        assert result.reason is RejectReason.DUPLICATE_TS_IN_BATCH

    def test_an_oversized_batch_is_refused_whole(self):
        many = [
            _m(NOW - datetime.timedelta(minutes=15 * (i + 1))) for i in range(CFG.max_batch + 1)
        ]
        result = validate_batch(_telemetry(*many), DEVICE, NOW, CFG)
        assert isinstance(result, MessageRejected)
        assert result.reason is RejectReason.BATCH_TOO_LARGE


class TestBothDirectionsIsNoLongerARejection:
    def test_a_partly_cloudy_quarter_hour_is_accepted(self):
        """The case that made the original rule wrong.

        10:00-10:15 at a PV site with broken cloud: the export register advances
        while the sun is out AND the import register advances while it is not.
        Both are positive, and the reading is entirely correct.
        """
        result = _validate(
            _m(NOW - datetime.timedelta(minutes=15), import_wh=140.0, export_wh=180.0)
        )
        assert isinstance(result, ValidatedBatch)
        assert len(result.accepted) == 1
        assert not result.rejected

    def test_but_it_is_still_counted(self):
        """A device reporting it on EVERY interval is a different story: that is
        a connector sending cumulative index readings rather than differences,
        which no bounds check can detect on its own."""
        result = _validate(
            _m(NOW - datetime.timedelta(minutes=15), import_wh=140.0, export_wh=180.0)
        )
        assert isinstance(result, ValidatedBatch)
        codes = [code for code, _ in result.observations]
        assert ObservedCode.BOTH_DIRECTIONS_OBSERVED in codes

    def test_the_ceiling_applies_to_the_sum(self):
        """What replaced it: total energy through the connection in one interval.

        Neither value alone exceeds the ceiling; together they do.
        """
        result = _validate(
            _m(NOW - datetime.timedelta(minutes=15), import_wh=30_000.0, export_wh=30_000.0)
        )
        assert isinstance(result, ValidatedBatch)
        assert [r.reason for r in result.rejected] == [RejectReason.OVER_DEVICE_CEILING]


class TestTimestamps:
    def test_more_than_five_minutes_ahead_is_rejected(self):
        result = _validate(_m(NOW + datetime.timedelta(minutes=10)))
        assert isinstance(result, ValidatedBatch)
        assert [r.reason for r in result.rejected] == [RejectReason.TS_IN_FUTURE]

    def test_a_few_minutes_of_clock_skew_is_tolerated(self):
        """The window exists so that ordinary NTP skew is not a rejection."""
        ahead = NOW + datetime.timedelta(minutes=2)
        aligned = ahead.replace(minute=(ahead.minute // 15) * 15, second=0, microsecond=0)
        result = _validate(_m(aligned))
        assert isinstance(result, ValidatedBatch)
        assert not result.rejected

    def test_older_than_the_acceptance_window_is_rejected(self):
        result = _validate(_m(NOW - datetime.timedelta(days=CFG.max_age_days + 1)))
        assert isinstance(result, ValidatedBatch)
        assert [r.reason for r in result.rejected] == [RejectReason.TS_TOO_OLD]

    def test_a_long_backlog_inside_the_window_is_accepted(self):
        """A device with days of local buffer is expected, not an error."""
        result = _validate(_m(NOW - datetime.timedelta(days=20)))
        assert isinstance(result, ValidatedBatch)
        assert len(result.accepted) == 1

    def test_an_unaligned_timestamp_is_rejected(self):
        result = _validate(_m(NOW - datetime.timedelta(minutes=14)))
        assert isinstance(result, ValidatedBatch)
        assert [r.reason for r in result.rejected] == [RejectReason.TS_NOT_ALIGNED]


class TestEnergies:
    @pytest.mark.parametrize("field", ["import_wh", "export_wh", "production_wh"])
    def test_a_negative_energy_is_rejected(self, field):
        """A meter replacement resets the index; the connector must skip that
        interval, never send the difference across the reset."""
        result = _validate(_m(NOW - datetime.timedelta(minutes=15), **{field: -1.0}))
        assert isinstance(result, ValidatedBatch)
        assert [r.reason for r in result.rejected] == [RejectReason.NEGATIVE_ENERGY]


class TestImplausibleProduction:
    def test_production_above_the_capacity_ceiling_is_rejected(self):
        """10 kVA over a quarter-hour is 2500 Wh; 1.1x tolerance allows 2750."""
        result = _validate(
            _m(NOW - datetime.timedelta(minutes=15), export_wh=3000.0, production_wh=3000.0)
        )
        assert isinstance(result, ValidatedBatch)
        assert [r.reason for r in result.rejected] == [RejectReason.IMPLAUSIBLE_PRODUCTION]

    def test_production_just_under_the_ceiling_is_accepted(self):
        """The tolerance exists so a correctly-sized installation running flat
        out stays out of the reject log. capacity_kva is the AC INJECTION limit,
        so real output legitimately sits just under it."""
        result = _validate(
            _m(NOW - datetime.timedelta(minutes=15), export_wh=2400.0, production_wh=2400.0)
        )
        assert isinstance(result, ValidatedBatch)
        assert not result.rejected

    def test_production_at_night_is_rejected(self):
        """02:00 Brussels in January. Catches a stolen credential injecting
        fabricated production into a signal members act on."""
        winter_night = datetime.datetime(2027, 1, 14, 1, 0, tzinfo=datetime.UTC)  # 02:00 CET
        result = _validate(
            _m(winter_night, export_wh=500.0, production_wh=500.0),
            now=winter_night + datetime.timedelta(minutes=1),
        )
        assert isinstance(result, ValidatedBatch)
        assert [r.reason for r in result.rejected] == [RejectReason.IMPLAUSIBLE_PRODUCTION]

    def test_late_midsummer_production_is_accepted(self):
        """The window is narrow ON PURPOSE. Brussels has civil twilight past
        22:00 in June, and a wider window would reject real production."""
        june_evening = datetime.datetime(2027, 6, 21, 19, 45, tzinfo=datetime.UTC)  # 21:45 CEST
        result = _validate(
            _m(june_evening, export_wh=200.0, production_wh=200.0),
            now=june_evening + datetime.timedelta(minutes=1),
        )
        assert isinstance(result, ValidatedBatch)
        assert not result.rejected

    def test_zero_production_at_night_is_fine(self):
        """Reporting nothing overnight is the normal case, not an anomaly."""
        winter_night = datetime.datetime(2027, 1, 14, 1, 0, tzinfo=datetime.UTC)
        result = _validate(
            _m(winter_night, import_wh=300.0, production_wh=0.0),
            now=winter_night + datetime.timedelta(minutes=1),
        )
        assert isinstance(result, ValidatedBatch)
        assert not result.rejected

    def test_a_device_with_no_declared_capacity_skips_the_ceiling_half(self):
        """NULL capacity is common - not every meter_data row declares one - and
        it must not become a blanket rejection."""
        no_capacity = DeviceContext(
            id_device=2,
            id_community=42,
            max_wh_per_interval=50_000.0,
            capacity_kva=None,
            pure_injection=True,
        )
        result = _validate(
            _m(NOW - datetime.timedelta(minutes=15), export_wh=9000.0, production_wh=9000.0),
            device=no_capacity,
        )
        assert isinstance(result, ValidatedBatch)
        assert not result.rejected


class TestUnknownMeasurementFields:
    def test_they_are_accepted_and_counted(self):
        """Protocol 7's second rule, which is what lets a newer connector add a
        field without every one of its messages being rejected by an older
        server - on a device with no OTA."""
        result = _validate(_m(NOW - datetime.timedelta(minutes=15), reactive_var=12.5))
        assert isinstance(result, ValidatedBatch)
        assert len(result.accepted) == 1
        assert (ObservedCode.UNKNOWN_MEASUREMENT_FIELD, "reactive_var") in result.observations
