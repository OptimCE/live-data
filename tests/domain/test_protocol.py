"""The frozen wire contract, and the one asymmetry that carries the whole of it.

protocol 7's compatibility rule has two halves that point in OPPOSITE directions:

    envelope    -> an unrecognised top-level key is MALFORMED (reject)
    measurement -> an unrecognised key is IGNORED AND COUNTED (accept)

Without the second half, the day a connector adds a field every message from that
firmware is rejected for ever - on a device in a basement with no OTA.

Both halves are asserted IN ONE TEST on purpose. They are expressed as two
`model_config`s on two nested models, and the natural tidy-up - hoisting
`model_config` onto a shared base - would break the frozen protocol silently. A
single test that fails on either direction is what makes that tidy-up impossible
to land by accident.

And `extra="allow"`, never `extra="ignore"`. They look interchangeable: both
accept the message. But `ignore` DISCARDS the unknown keys, leaving
`model_extra` as None - so the counter reads zero for ever while looking
implemented, and nobody ever learns that a connector started sending a field.
"""

import datetime

import pytest
from pydantic import ValidationError

from domain.protocol import DiagV1, MeasurementV1, StatusV1, TelemetryV1
from shared.const import DiagCode

_TS = "2026-09-12T10:15:00Z"


def _measurement(**overrides) -> dict:
    payload = {
        "ts": _TS,
        "interval_s": 900,
        "import_wh": 312.0,
        "export_wh": 0.0,
        "production_wh": None,
    }
    payload.update(overrides)
    return payload


class TestTheCompatibilityAsymmetry:
    def test_envelope_rejects_unknown_keys_and_measurement_keeps_them(self):
        """protocol 7, both halves, in one assertion block.

        If this test ever fails in only one direction, someone has unified the
        two `model_config`s - which is exactly the change that must not land.
        """
        # Half one: an unknown key at the ENVELOPE level is malformed.
        with pytest.raises(ValidationError):
            TelemetryV1(v=1, measurements=[_measurement()], surprise="x")

        # Half two: an unknown key INSIDE a measurement is accepted...
        telemetry = TelemetryV1(v=1, measurements=[_measurement(reactive_var=12.5)])
        measurement = telemetry.measurements[0]

        # ...and RECOVERABLE, which is what `allow` buys over `ignore`. With
        # `extra="ignore"` model_extra would be None here and the
        # unknown-field counter would read zero for ever.
        assert measurement.model_extra is not None
        assert measurement.model_extra == {"reactive_var": 12.5}

        # ...and the known fields still parsed.
        assert measurement.import_wh == 312.0

    def test_the_status_envelope_is_strict_too(self):
        with pytest.raises(ValidationError):
            StatusV1(v=1, online=True, surprise="x")

    def test_diag_tolerates_unknown_keys(self):
        """A connector may add diagnostic detail without being rejected."""
        diag = DiagV1(code=DiagCode.NO_TELEGRAM, extra_detail="port /dev/ttyUSB0")
        assert diag.code is DiagCode.NO_TELEGRAM
        assert diag.model_extra == {"extra_detail": "port /dev/ttyUSB0"}


class TestRequiredFields:
    @pytest.mark.parametrize(
        "missing", ["ts", "interval_s", "import_wh", "export_wh", "production_wh"]
    )
    def test_every_required_measurement_field_is_required(self, missing):
        """`production_wh` is REQUIRED but NULLABLE, and that is not an accident.

        The connector must STATE it. `null` is a legitimate statement meaning
        "unknown" - a P1 port sees only the exchange with the grid, so on a site
        that also consumes, production is invisible and only the export is known.
        Making it optional would let a connector omit it and leave the server
        unable to distinguish "no production" from "production not measured".
        """
        payload = _measurement()
        del payload[missing]
        with pytest.raises(ValidationError):
            MeasurementV1(**payload)

    def test_production_wh_accepts_an_explicit_null(self):
        assert MeasurementV1(**_measurement(production_wh=None)).production_wh is None

    def test_power_w_is_optional(self):
        """Optional because a P1 connector that only differences indexes has
        nothing sensible to put there. A display refinement, never a source of
        truth - watts are derived as wh * 3600 / interval_s."""
        assert MeasurementV1(**_measurement()).power_w is None
        assert MeasurementV1(**_measurement(power_w=1248)).power_w == 1248


class TestTheStatusPayload:
    def test_the_lwt_is_the_same_shape_with_online_false(self):
        """The Last Will and Testament is not a separate message type."""
        lwt = StatusV1(v=1, online=False)
        assert lwt.online is False
        assert lwt.diag is None

    def test_connector_and_version_survive_a_round_trip(self):
        """Read on EVERY status message, not only at enrolment - which is what
        makes "which devices run the broken 0.3.0?" answerable."""
        status = StatusV1(
            v=1,
            online=True,
            connector="optimce-connector",
            version="0.3.1",
            ts=_TS,
            diag={"code": "ok", "since": "2026-09-12T06:00:00Z"},
        )
        assert status.connector == "optimce-connector"
        assert status.version == "0.3.1"
        assert status.diag is not None
        assert status.diag.code is DiagCode.OK

    def test_the_five_diag_codes_of_protocol_3_2(self):
        """FIVE, not four. Plan 16's list omits `source_unreachable`; the
        protocol is the reference, because it is the document with three
        independent implementers."""
        assert {c.value for c in DiagCode} == {
            "ok",
            "no_telegram",
            "parse_error",
            "port_closed",
            "source_unreachable",
        }

    def test_an_unknown_diag_code_is_rejected(self):
        with pytest.raises(ValidationError):
            StatusV1(v=1, online=True, diag={"code": "invented"})


class TestTimestamps:
    def test_ts_parses_to_an_aware_utc_datetime(self):
        """`ts` is the END of the interval, in UTC. A naive datetime here is how
        a quarter-hour silently lands in the wrong bucket."""
        measurement = MeasurementV1(**_measurement())
        assert measurement.ts.tzinfo is not None
        assert measurement.ts == datetime.datetime(2026, 9, 12, 10, 15, tzinfo=datetime.UTC)
