"""Bucket algebra for the rollups. Pure: no I/O, no session, and NO CLOCK.

Step 8's highest-risk module, and it is a separate file for the same reason
`partitions.py` is: the arithmetic has to be unit-testable without a database,
and the SQL has to be pinned against it (`tests/test_bucket_sql_parity.py`).

----------------------------------------------------------------------------
`measurement.ts` IS THE END OF THE INTERVAL. THIS IS THE WHOLE MODULE.

protocol 3.1 rule 1: "`ts` is the END of the interval, not its start. A
measurement covering 10:00-10:15 carries `10:15:00Z`."

So a reading stamped 11:00:00Z belongs to the hour **10:00**, not 11:00. The
expression every implementation reaches for first -

    date_trunc('hour', ts)

- puts it in hour 11. That moves a quarter of every hour's energy into the
following hour, for every device, for ever, and nothing errors: the totals stay
plausible, the chart stays smooth, and the only symptom is that the numbers are
wrong. It is the single highest-frequency silent defect available in step 8.

The correct form subtracts one second before truncating:

    date_trunc('hour', (ts - INTERVAL '1 second') AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'

The obvious alternative - `ts - make_interval(secs => interval_s)` - is REJECTED.
`domain/validation.py` checks that `ts` is aligned; it never checks `interval_s`
at all, so a device can send `interval_s: 86400` today and have it stored. The
`- 1 second` form trusts no field, is correct for every interval that divides the
hour, and degrades to "attributed to the last hour it touches" for a rogue value
rather than "shifted by a whole hour for everyone".

Because `ts` is guaranteed aligned (`ts_not_aligned` rejects otherwise, and
`is_aligned` also requires `microsecond == 0`), this equivalence is EXACT:

    bucket(ts) in [lo, hi)   <=>   ts in (lo, hi]

which is what lets the sweep put a sargable range on `ts` - hitting
`ix_measurement_community_ts` - while stating its target purely in buckets.
----------------------------------------------------------------------------

THE `AT TIME ZONE 'UTC'` ROUND TRIP IS NOT CEREMONY.

`date_trunc` on a `timestamptz` truncates in the SESSION's timezone, and
`provision.sh` sets none. For an hour boundary in a whole-hour-offset zone the
answer coincidentally agrees; for a DAY boundary it does not, and the day rollup
lands at 23:00 or 01:00 depending on which connection ran it. `partitions.py`
documents the same trap for partition bounds and `tests/test_partitions.py`
already pins it under `TimeZone='Europe/Brussels'`.

THE DAY IS EUROPE/BRUSSELS, AND IT IS A CONSTANT RATHER THAN A SETTING.

A UTC day splits the Belgian day at 01:00 or 02:00 local, so every daily total a
manager reads would be wrong by an hour or two of energy. And because closed
periods are never recomputed (plan 6.3), changing this later silently
invalidates every historical day row with nothing anywhere to detect it - which
is exactly why it must not be reachable from the environment. See
`shared/const.ROLLUP_DAY_TIMEZONE`.

The consequence to keep in mind: the last Sunday in October is a **25-hour**
local day and the last Sunday in March a **23-hour** one. `day + 24 hours` and
`bucket::date` are both wrong on those two days, by exactly one hour of
community energy, and neither raises.
"""

import datetime
from typing import Final
from zoneinfo import ZoneInfo

from shared.const import ROLLUP_DAY_TIMEZONE, ROLLUP_WINDOW_HOURS

_DAY_TZ: Final[ZoneInfo] = ZoneInfo(ROLLUP_DAY_TIMEZONE)
_ONE_SECOND: Final[datetime.timedelta] = datetime.timedelta(seconds=1)
_ONE_HOUR: Final[datetime.timedelta] = datetime.timedelta(hours=1)


def bucket_sql(ts_expression: str) -> str:
    """The SQL form of `bucket_of`, for the statements that cannot call Python.

    Two of them need it and they are in different processes: `worker/ingest.py`
    marks a bucket dirty in the same statement that stores the measurement, and
    `worker/rollups.py` derives the bucket it aggregates into. If those two ever
    disagreed, ingest would mark one bucket and the tick would recompute another
    - so the marked one would stay dirty for ever while the chart stayed wrong,
    and no statement would fail.

    Hence one function rather than two string literals, and
    `tests/test_rollups.py` asserts both statements contain what it returns.

    The `- 1 second` is the module's whole subject; the `AT TIME ZONE 'UTC'`
    round trip is because `date_trunc` on a `timestamptz` truncates in the
    SESSION's timezone, and `provision.sh` sets none.
    """
    return (
        f"date_trunc('hour', ({ts_expression} - INTERVAL '1 second') AT TIME ZONE 'UTC')"
        " AT TIME ZONE 'UTC'"
    )


def _require_aware(moment: datetime.datetime, name: str) -> datetime.datetime:
    """Every entry point takes an aware datetime and says so.

    A naive datetime here is the timezone bug one layer up: Python would treat it
    as local time on whatever machine happens to run the job.
    """
    if moment.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware")
    return moment.astimezone(datetime.UTC)


def hour_floor(moment: datetime.datetime) -> datetime.datetime:
    """The start of the UTC hour containing `moment`."""
    utc = _require_aware(moment, "moment")
    return utc.replace(minute=0, second=0, microsecond=0)


def bucket_of(ts: datetime.datetime) -> datetime.datetime:
    """The UTC hour bucket a measurement belongs to.

    `ts` is the END of the interval, so 11:00:00Z belongs to the 10:00 bucket.
    See the module docstring - this one line is the module.
    """
    return hour_floor(_require_aware(ts, "ts") - _ONE_SECOND)


def window(now: datetime.datetime) -> tuple[datetime.datetime, datetime.datetime]:
    """The recompute window `[lo, hi)`, ALIGNED AT BOTH ENDS.

    `hi` is the end of the hour in progress, so the current partial hour is
    inside the window and is rewritten every tick until it closes - published
    with `n_samples` saying it is partial, rather than withheld for up to an
    hour.

    Alignment is the point. With an unaligned `lo = now - 48h` and `now = 10:37`,
    the oldest bucket is computed from its 10:37-11:00 fragment alone, and that
    partial value is written over a row that was previously correct. Every hour
    is eroded on its way out of the window, silently, for ever.
    """
    hi = hour_floor(now) + _ONE_HOUR
    return hi - datetime.timedelta(hours=ROLLUP_WINDOW_HOURS), hi


def local_day_start(bucket: datetime.datetime) -> datetime.datetime:
    """The instant of local midnight opening the day that contains `bucket`."""
    local = _require_aware(bucket, "bucket").astimezone(_DAY_TZ)
    midnight = local.replace(hour=0, minute=0, second=0, microsecond=0)
    return midnight.astimezone(datetime.UTC)


def local_day_end(day_start: datetime.datetime) -> datetime.datetime:
    """The instant of the NEXT local midnight.

    Computed by adding a day to the local WALL CLOCK and re-localising, never by
    adding 24 hours: the last Sunday in October is 25 hours long and the last
    Sunday in March 23. `day_start + timedelta(days=1)` on an aware datetime
    adds exact elapsed time and therefore lands at 23:00 or 01:00 local on those
    two days.
    """
    local = _require_aware(day_start, "day_start").astimezone(_DAY_TZ)
    naive_next = local.replace(tzinfo=None) + datetime.timedelta(days=1)
    return naive_next.replace(tzinfo=_DAY_TZ).astimezone(datetime.UTC)


def is_day_closed(day_start: datetime.datetime, now: datetime.datetime) -> bool:
    """Whether every hour of this local day has completed.

    Gated on `hour_floor(now)` rather than on `now`: a day must never be derived
    from an hour that is still in progress. plan 6.3 - "Day rollups are derived
    from whole days only. Deriving them from a partial day freezes a partial
    total forever, because closed periods are never recomputed."
    """
    return local_day_end(day_start) <= hour_floor(now)


def day_targets(
    buckets: list[datetime.datetime], now: datetime.datetime
) -> list[datetime.datetime]:
    """The distinct CLOSED local days touched by these hour buckets, sorted.

    The filter is what stops a single dirty bucket ten days old producing a
    one-hour "day" total that then freezes for ever.
    """
    days = {local_day_start(bucket) for bucket in buckets}
    return sorted(day for day in days if is_day_closed(day, now))
