"""Query windows: resolutions, snapping, and the point cap. Pure.

`domain/buckets.py` decides which bucket a measurement belongs to.  This module
decides which buckets a CALLER may ask for, and it exists separately because the
answer is a privacy rule rather than an arithmetic one.

----------------------------------------------------------------------------
AN UNSNAPPED WINDOW IS REFUSED, NEVER SNAPPED.

plan 9.3: "Snap `from`/`to` to the grid, and 422 on an unsnapped request rather
than snapping silently. Free-form bounds are the differencing attack: vary the
window until the contributing set is one member."

The attack is worth stating concretely, because "we snap it for them" sounds
like a kindness. Given any k threshold, an attacker who can move a boundary by
one minute can request [09:00, 10:00) and [09:01, 10:00) and subtract: the
difference is one minute of one community, and repeated with a sliding boundary
it reconstructs a single household's curve from aggregates that each passed k.
Silently snapping answers BOTH requests - identically, which is precisely what
makes them subtractable.

So the bounds must already lie on the grid, and a request that does not is a 422
that says so. Both bounds are OPTIONAL, so a client sending only `resolution`
never trips it; the defaults are snapped by construction.
----------------------------------------------------------------------------

THE DAY GRID IS LOCAL MIDNIGHT, NOT A MULTIPLE OF 24 HOURS.

A day bound is snapped when it IS an instant of Europe/Brussels midnight. On the
two DST days those instants are 23 and 25 hours apart, so any rule of the form
`(ts - epoch) % 86400 == 0` rejects half the valid bounds in summer and the other
half in winter.
"""

import datetime
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from domain import buckets
from domain.partitions import INTERVAL_SECONDS


class Resolution(StrEnum):
    """The three grids a caller may ask for.

    `quarter` reads `measurement`; `hour` and `day` read the rollups. A fourth
    value would need a source in `RESOLUTION_SOURCES` and a snapping rule here,
    which is why this is an enum and not a free string.
    """

    QUARTER = "quarter"
    HOUR = "hour"
    DAY = "day"


class WindowProblem(StrEnum):
    UNKNOWN_RESOLUTION = "unknown_resolution"
    NOT_SNAPPED = "not_snapped"
    TOO_LARGE = "too_large"
    INVERTED = "inverted"


class WindowError(ValueError):
    """Raised by `resolve_window`. The API layer maps `problem` to an error code.

    A domain exception rather than an `ErrorException`: nothing under `domain/`
    imports `core/`, because `worker/` imports `domain/` and its image installs
    no HTTP stack.
    """

    def __init__(self, problem: WindowProblem, detail: str = "") -> None:
        super().__init__(f"{problem}: {detail}" if detail else str(problem))
        self.problem = problem
        self.detail = detail


# The point cap per resolution, and the default span when the caller gives none.
#
# The cap exists because the response crosses KrakenD, which parses and
# re-serialises every body inside a 3000 ms budget with no per-route override.
# `MeterMapDTO` already carries `truncated`/`cap` for the same reason.
MAX_POINTS: Final[dict[Resolution, int]] = {
    Resolution.QUARTER: 2_976,  # 31 days
    Resolution.HOUR: 1_464,  # 61 days
    Resolution.DAY: 731,  # 2 years
}

DEFAULT_SPAN: Final[dict[Resolution, datetime.timedelta]] = {
    Resolution.QUARTER: datetime.timedelta(hours=24),
    Resolution.HOUR: datetime.timedelta(days=7),
    Resolution.DAY: datetime.timedelta(days=30),
}


@dataclass(frozen=True, slots=True)
class Window:
    """A resolved, snapped, half-open window `[start, end)`."""

    start: datetime.datetime
    end: datetime.datetime
    resolution: Resolution


def parse_resolution(value: str | None) -> Resolution:
    if value is None:
        return Resolution.HOUR
    try:
        return Resolution(value)
    except ValueError as exc:
        raise WindowError(WindowProblem.UNKNOWN_RESOLUTION, str(value)) from exc


def is_snapped(moment: datetime.datetime, resolution: Resolution) -> bool:
    """Whether `moment` lies on `resolution`'s grid."""
    if moment.tzinfo is None:
        raise ValueError("is_snapped requires an aware datetime")
    utc = moment.astimezone(datetime.UTC)
    if resolution is Resolution.QUARTER:
        return int(utc.timestamp()) % INTERVAL_SECONDS == 0 and utc.microsecond == 0
    if resolution is Resolution.HOUR:
        return utc == buckets.hour_floor(utc)
    # DAY: an instant of LOCAL midnight. Compared by round trip rather than by
    # arithmetic, because the 25-hour and 23-hour days make every modulo rule
    # wrong for half the year.
    return utc == buckets.local_day_start(utc)


def snap_down(moment: datetime.datetime, resolution: Resolution) -> datetime.datetime:
    """The grid point at or below `moment`. Used for DEFAULTS, never for input.

    Snapping a caller-supplied bound is the differencing attack; snapping `now`
    to build a default window is not, because the caller did not choose it.
    """
    utc = moment.astimezone(datetime.UTC)
    if resolution is Resolution.QUARTER:
        epoch = int(utc.timestamp())
        return datetime.datetime.fromtimestamp(epoch - (epoch % INTERVAL_SECONDS), tz=datetime.UTC)
    if resolution is Resolution.HOUR:
        return buckets.hour_floor(utc)
    return buckets.local_day_start(utc)


def step_after(moment: datetime.datetime, resolution: Resolution) -> datetime.datetime:
    """The next grid point after `moment`.

    For DAY this is `local_day_end`, not `+ 24 h` - see the module docstring.
    """
    if resolution is Resolution.QUARTER:
        return moment + datetime.timedelta(seconds=INTERVAL_SECONDS)
    if resolution is Resolution.HOUR:
        return moment + datetime.timedelta(hours=1)
    return buckets.local_day_end(moment)


# The LONGEST a single grid step can be. 25 hours for a day, because the last
# Sunday in October is one. Used only for the cheap guard below.
_MAX_STEP_SECONDS: Final[dict[Resolution, int]] = {
    Resolution.QUARTER: INTERVAL_SECONDS,
    Resolution.HOUR: 3600,
    Resolution.DAY: 25 * 3600,
}


def exceeds_cap(window: Window) -> bool:
    """Whether `window` holds more points than its resolution allows.

    Two stages, and the first one is not an optimisation.

    `count_points` walks the grid, so a caller asking for a thousand years at
    quarter-hour resolution would spin through 35 million iterations before being
    told no - an unauthenticated-shaped denial of service reachable from a query
    string. The guard uses the LONGEST possible step to derive a LOWER bound on
    the point count: if even that lower bound exceeds the cap, the answer is no
    without walking anything. It can only reject windows that are genuinely too
    large, so it never refuses a valid one.
    """
    span = (window.end - window.start).total_seconds()
    if span / _MAX_STEP_SECONDS[window.resolution] > MAX_POINTS[window.resolution]:
        return True
    return count_points(window) > MAX_POINTS[window.resolution]


def count_points(window: Window) -> int:
    """How many grid points `window` contains.

    Counted by walking the grid rather than by dividing the span, because the
    DAY grid is not uniform: a 30-day window containing the October change has
    one 25-hour day in it, and `span / 24 h` is then off by one.
    """
    total = 0
    cursor = window.start
    while cursor < window.end:
        cursor = step_after(cursor, window.resolution)
        total += 1
    return total


def resolve_window(
    *,
    now: datetime.datetime,
    resolution: str | None = None,
    start: datetime.datetime | None = None,
    end: datetime.datetime | None = None,
) -> Window:
    """Validate and default a caller's window. Raises `WindowError`.

    Both bounds are optional. Supplying neither yields the default span ending at
    the current grid point, which is what the SPA sends.
    """
    grid = parse_resolution(resolution)

    # The default `end` is the NEXT grid point, so the period in progress is
    # included - the rollup publishes it with `n_samples` saying it is partial,
    # and withholding it would leave the chart up to a day behind.
    resolved_end = end if end is not None else step_after(snap_down(now, grid), grid)
    resolved_start = start if start is not None else resolved_end - DEFAULT_SPAN[grid]
    if start is None:
        resolved_start = snap_down(resolved_start, grid)

    for label, bound in (("from", start), ("to", end)):
        if bound is not None and not is_snapped(bound, grid):
            raise WindowError(WindowProblem.NOT_SNAPPED, f"{label} is not on the {grid} grid")

    if resolved_end <= resolved_start:
        raise WindowError(WindowProblem.INVERTED, "to must be after from")

    window = Window(start=resolved_start, end=resolved_end, resolution=grid)
    if exceeds_cap(window):
        raise WindowError(
            WindowProblem.TOO_LARGE,
            f"at most {MAX_POINTS[grid]} points at resolution {grid}",
        )
    return window
