"""What state a device is actually in. Pure: no session, no clock of its own.

plan 16's register names three device states the schema can already describe -
never seen, online, offline - and one it cannot:

    "**'Online but reporting zeros for 24 h' is invisible**, and it is exactly
     what a connected-but-unenabled P1 looks like. A third device state alongside
     'never seen' and 'offline'."

That sentence is the reason this module exists. A P1 port that the DSO has not
activated behaves EXACTLY like a healthy device: the connector attaches, MQTT
connects, `status` is published with `online: true`, telemetry arrives on
schedule - and every register reads zero. `device_last.online` is true,
`last_seen_at` is seconds old, the device list is green, and the meter has never
measured anything.

§11.2 calls the DSO not having enabled the port the single most likely cause of a
silent device, so this is not a corner case: it is the FIRST failure a pilot
hits, and without this state nothing in the product says so.

----------------------------------------------------------------------------
THE SWEEP MUST NOT MISREAD THE WORKER'S OWN DISCONNECT.

`worker/main.py` disconnects from the broker deliberately after
`INGEST_DB_FAILURES_BEFORE_DISCONNECT` consecutive database failures - pushing
the backlog into the broker's persistent session, where it is bounded and logged,
rather than growing an unbounded in-memory deque.

While that is happening, EVERY device in the fleet stops being seen. A silence
sweep that reported them all as dead would turn one database incident into forty
false alarms, at the exact moment an operator needs the signal-to-noise. So
`classify` is given `ingest_healthy`, and answers `UNKNOWN` for the whole fleet
when the ingest path itself is down: no device can be judged on evidence that was
never collected.
----------------------------------------------------------------------------
"""

import datetime
from collections.abc import Sequence
from enum import StrEnum

from shared.const import DeviceStatus


class DeviceHealth(StrEnum):
    """Ordered roughly by how much attention each deserves."""

    OK = "ok"
    # Enrolled, credentials issued, and it has never published anything. Almost
    # always a device that was never powered on, or one whose broker host was
    # written before `BROKER_PUBLIC_HOST` was correct.
    NEVER_SEEN = "never_seen"
    # It has published before, and not recently.
    SILENT = "silent"
    # Seen recently, `online` false: a clean disconnect or the Last Will.
    OFFLINE = "offline"
    # THE THIRD STATE. Connected, reporting on schedule, and every reading is
    # zero. See the module docstring.
    REPORTING_ZEROS = "reporting_zeros"
    # The ingest path is down, so nothing can be concluded about any device.
    UNKNOWN = "unknown"
    # Revoked on purpose. It can never publish again, so no other state applies
    # and none of them would be actionable - see `classify`.
    REVOKED = "revoked"


def classify(
    *,
    now: datetime.datetime,
    status: DeviceStatus,
    last_seen_at: datetime.datetime | None,
    online: bool | None,
    energy_wh_recent: float | None,
    silent_after_hours: int = 24,
    ingest_healthy: bool = True,
) -> DeviceHealth:
    """One device's state.

    `status` FIRST, and REQUIRED. Revocation touches `device`, never
    `device_last`, so a revoked device classified from `device_last` alone read
    OFFLINE, then SILENT a day later, and counted as needing attention for ever.
    It is not defaulted because `ingest_healthy` was: for a while no caller
    passed it, and UNKNOWN was unreachable behind a green test.

    `energy_wh_recent` is the total energy this device reported - import plus
    export plus production - over a window THE CALLER defines, in its SQL
    (`repository.devices_with_status(zero_window_hours=...)`). None means no
    rollup covers that window at all, which is NOT the same as zero and must not
    be reported as the third state.

    This function once took a `zero_window_hours` of its own and never read it:
    the REPORTING_ZEROS branch gates on `silent_after_hours`. Both defaulted to
    24, so nothing was wrong - until a caller passed a different window and had
    it silently ignored while the SQL kept the old one. `ruff check` is clean
    over an unused argument unless ARG is selected, so nothing would have said so.
    """
    # Before the ingest verdict too: nothing about the collector changes what a
    # revoked device is.
    if status is DeviceStatus.REVOKED:
        return DeviceHealth.REVOKED
    if not ingest_healthy:
        return DeviceHealth.UNKNOWN
    if last_seen_at is None:
        return DeviceHealth.NEVER_SEEN

    age = now - last_seen_at
    if age > datetime.timedelta(hours=silent_after_hours):
        return DeviceHealth.SILENT
    if online is False:
        return DeviceHealth.OFFLINE

    # Only for a device that has been up long enough for the window to mean
    # something. A device enrolled twenty minutes ago has legitimately reported
    # almost nothing, and calling that "reporting zeros" would make every new
    # enrolment look broken on the day it is set up - which is the day someone is
    # standing next to it with a phone.
    if (
        energy_wh_recent is not None
        and energy_wh_recent == 0
        and age < datetime.timedelta(hours=silent_after_hours)
    ):
        return DeviceHealth.REPORTING_ZEROS
    return DeviceHealth.OK


def ingest_looks_healthy(
    *,
    now: datetime.datetime,
    last_seen: Sequence[datetime.datetime | None],
    silent_after_hours: int = 24,
) -> bool:
    """Is the COLLECTOR alive, judged from the fleet rather than from a device?

    ---------------------------------------------------------------------------
    WHAT THIS ANSWERS, AND WHY `classify` CANNOT.

    Step 11: "the silent-device sweep must not misread the worker's deliberate
    disconnect after INGEST_DB_FAILURES_BEFORE_DISCONNECT consecutive database
    failures as a dead device." `classify` has always had the `ingest_healthy`
    switch for it - and for a while NOTHING PASSED IT, so `UNKNOWN` was
    unreachable, the runbook's `unknown` triage row described a state that could
    not occur, and the test covering it exercised a path no caller reached.

    The signal has to come from the fleet, because no per-device fact
    distinguishes the two cases: a device whose readings stopped arriving and a
    device that stopped sending look identical from the database. What separates
    them is CORRELATION. One meter going quiet is a meter. Every meter in a
    community going quiet at the same moment is not forty simultaneous hardware
    failures, it is the path they share.
    ---------------------------------------------------------------------------

    THE FLOOR OF TWO IS THE LOAD-BEARING PART.

    With one reporting device, "the fleet is silent" and "this device is dead"
    are the SAME sentence. Answering `UNKNOWN` there would hide a genuinely dead
    meter behind "the ingest path is down" - turning the one state an installer
    can act on into the one they cannot. So below two, this returns True and the
    per-device classification stands.

    A device that has NEVER reported carries no evidence either way and is not
    counted: a community of forty never-enrolled devices must not read as an
    ingest outage.

    Nor is a REVOKED one - the CALLER leaves it out. Revocation freezes its
    `last_seen_at`, so counting it turned one silent meter into "two quiet
    devices", met the floor, and hid the dead meter behind UNKNOWN.
    """
    seen = [ts for ts in last_seen if ts is not None]
    if len(seen) < 2:
        return True
    return (now - max(seen)) <= datetime.timedelta(hours=silent_after_hours)


# What an operator should try, in order, for each state. Returned on
# `/devices/{id}/diagnostics` as a hint KEY - the frontend localises it.
#
# `no_telegram` and `reporting_zeros` both point at the DSO, because §11.2 says
# that is the single most likely cause and because it is the one thing an
# installer cannot see from the outside: the hardware is fine, the wiring is
# fine, and the port is closed.
HINTS: dict[DeviceHealth, str] = {
    DeviceHealth.NEVER_SEEN: "LIVE.HINT.NEVER_SEEN",
    DeviceHealth.SILENT: "LIVE.HINT.SILENT",
    DeviceHealth.OFFLINE: "LIVE.HINT.OFFLINE",
    DeviceHealth.REPORTING_ZEROS: "LIVE.HINT.P1_NOT_ENABLED",
    DeviceHealth.UNKNOWN: "LIVE.HINT.INGEST_DOWN",
    DeviceHealth.OK: "LIVE.HINT.OK",
    DeviceHealth.REVOKED: "LIVE.HINT.REVOKED",
}
