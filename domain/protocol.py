"""The MQTT wire contract, v1. FROZEN at build step 5.

These models are `live-data-protocol.md` 3 expressed in Pydantic. The document is
a published interface with three independent implementers - the reference
connector `optimce-connector`, and the `optimce-p1-esp32` and
`optimce-inverter-esp32` firmwares - none of which live in this repository, and
the last two of which sit in basements with no remote update.

So: within v1, a field is never removed and never changes type. Fields may be
added, and must be optional. `v` increases only for a breaking change, and the
server accepts both versions for at least a year (protocol 7).

Nothing here imports from `api/`, touches a session, or reaches the network.
`worker/` imports this module and `Dockerfile.worker` installs no fastapi.

----------------------------------------------------------------------------
THE TWO `model_config`s BELOW ARE OPPOSITE ON PURPOSE, AND THAT IS THE WHOLE
OF PROTOCOL 7's COMPATIBILITY STORY.

  envelope     -> extra="forbid"   an unrecognised top-level key is malformed
  measurement  -> extra="allow"    an unrecognised key is ignored AND COUNTED

Without the second rule, the day a connector adds a field every message from that
firmware is rejected forever, on a device with no OTA.

`extra="allow"` and NOT `extra="ignore"`. They look interchangeable and are not:
with `ignore`, pydantic discards the unknown keys and `model_extra` is None, so
the counter reads zero for ever while looking implemented - and nobody learns
that a connector started sending a new field. `tests/domain/test_protocol.py`
asserts BOTH directions in one test, so a later tidy-up that hoists `model_config`
to a shared base cannot quietly break the frozen protocol.
----------------------------------------------------------------------------
"""

import datetime

from pydantic import BaseModel, ConfigDict, Field

from shared.const import DiagCode


class MeasurementV1(BaseModel):
    """One quarter-hour reading. protocol 3.1.

    Four things that are easy to get wrong and expensive to get wrong, restated
    here because this is where they are enforced:

    1. `ts` is the END of the interval. 10:00-10:15 carries 10:15:00Z.
    2. `ts` comes from the DATA SOURCE - the P1 telegram, the inverter - never
       from the device's clock. An ESP32 without NTP has no usable clock, and a
       drifted clock is rejected.
    3. `*_wh` are ENERGIES OVER THE INTERVAL, not cumulative index readings. If
       the source gives indexes the connector differences them itself. Sending an
       index is not an error the server can detect: it passes the bounds check on
       the first message and fails it thereafter, which reads as a flaky device.
    4. `power_w` is a display refinement, never a source of truth. It is optional
       precisely because a P1 connector that only differences indexes has nothing
       sensible to put there. Watts are derived as `wh * 3600 / interval_s`.
    """

    model_config = ConfigDict(extra="allow")  # see the module docstring. NOT "ignore".

    ts: datetime.datetime = Field(
        description="END of the measured interval, UTC, ISO 8601.",
    )
    interval_s: int = Field(
        description="Interval length in seconds. 900 in v1.",
    )
    import_wh: float = Field(
        description="Energy drawn from the grid over the interval, Wh, >= 0.",
    )
    export_wh: float = Field(
        description="Energy injected into the grid over the interval, Wh, >= 0.",
    )
    # Required but nullable - the connector must state it, and `null` is a
    # legitimate statement meaning "unknown". protocol 3.4: a P1 port sees only
    # the exchange with the grid, so on a site that also consumes, production is
    # invisible and only the export is known. A connector that guesses here
    # silently understates community production for ever, in a way that is
    # indistinguishable from a cloudy day.
    production_wh: float | None = Field(
        description="Energy produced over the interval, Wh, >= 0, or null when unknown.",
    )
    power_w: int | None = Field(
        default=None,
        description="Instantaneous power at send time. Stored only as a last value.",
    )


class TelemetryV1(BaseModel):
    """The `telemetry` payload. QoS 1, on `ce/{community_id}/{device_id}/telemetry`.

    A device that has been offline republishes its backlog as an array, in one
    message or several. This is NORMAL TRAFFIC, not an exception - the server is
    built around it, and the measurement-scoped rejections in `domain/reasons.py`
    exist so that one bad reading inside a backlog does not discard the rest.
    """

    model_config = ConfigDict(extra="forbid")  # see the module docstring.

    v: int = Field(description="Protocol version. 1.")
    measurements: list[MeasurementV1] = Field(
        description="1 to 200 entries. Each `ts` must appear at most once.",
    )


class DiagV1(BaseModel):
    """protocol 3.2 `diag`.

    This field exists because a P1 the DSO has not enabled, a bad cable, a wifi
    drop and a dead device all look identical from the server: silence. One field
    turns a support call into a diagnosis. Connectors should set it; it is
    optional so that a minimal implementation is still valid.
    """

    model_config = ConfigDict(extra="allow")

    code: DiagCode | None = Field(default=None)
    since: datetime.datetime | None = Field(default=None)


class StatusV1(BaseModel):
    """The `status` payload. QoS 0, retained, and also the Last Will and Testament.

    The LWT payload is the same shape with `"online": false`.

    `connector` and `version` are read on EVERY status message, not only at
    enrolment, so that a field upgrade is visible. "Which devices are running the
    broken 0.3.0?" is a question that only ever gets asked at the worst possible
    moment.
    """

    model_config = ConfigDict(extra="forbid")

    v: int = Field(description="Protocol version. 1.")
    online: bool
    connector: str | None = Field(default=None, max_length=64)
    version: str | None = Field(default=None, max_length=32)
    ts: datetime.datetime | None = Field(default=None)
    diag: DiagV1 | None = Field(default=None)


__all__ = [
    "DiagV1",
    "MeasurementV1",
    "StatusV1",
    "TelemetryV1",
]
