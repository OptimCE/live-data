"""Monthly partition naming and bounds.

Pure. Plan 13's sequencing note makes this a step-2 deliverable precisely so that
step 8's create-ahead job and `scripts/sql/schema.sql` cannot disagree: "2 before
3, with partition bounds from the same helper step 8 uses".

----------------------------------------------------------------------------
THE SQL IS THE SOURCE OF TRUTH. THIS IS THE MIRROR.

Partitions are actually created by `live_ensure_monthly_partitions()`, a plpgsql
function inside `schema.sql`. That is not where you would naturally put it, and
the reason is plan 6.2: "the same helper is used by the schema and by the
scheduler". That is only LITERALLY true if the helper is SQL. A Python helper
that emits DDL, mirrored by static `FOR VALUES FROM (...)` literals in
schema.sql, is two helpers - and they will disagree the first time someone edits
one, in a way that surfaces months later as a failed ATTACH at 00:00 UTC on the
first of a month.

So this module exists to NAME and PREDICT bounds - for tests, for the ops view of
which partition a timestamp lands in, and for step 8's retention job, which has
to identify a partition before it can DETACH it. It never issues DDL.

`tests/test_partitions.py` pins the two against each other by reading
`pg_get_expr(c.relpartbound, c.oid)` back out of Postgres.
----------------------------------------------------------------------------

EVERYTHING HERE IS UTC, EXPLICITLY.

Bounds are written as explicit `+00` literals. A `timestamptz` bound is rendered
by Postgres in the SESSION's timezone, so a bound created under
`TimeZone='Europe/Brussels'` and read back under UTC is the same instant printed
two different ways - and a naive string comparison between them fails, or worse,
passes for the wrong reason. `provision.sh` does not set a timezone, so the
session's is whatever the server defaults to.
"""

import datetime
from typing import Final

# Every partitioned table this service owns, and the column it is ranged on.
# `schema.sql` holds the same list in the `live_partitioned_table` registry
# table; step 8 adds `rollup_device_hour` to BOTH. Plan 6.2 names the failure
# this registry exists to prevent: "a job that names only `measurement` lets the
# rollups freeze about four months in while ingestion goes on looking perfectly
# healthy".
PARTITIONED_TABLES: Final[tuple[str, ...]] = (
    "measurement",
    "rollup_device_hour",
    "rollup_device_day",
    # Migration 0003 (D-14): an operation of two households is two households,
    # so its rollups are personal data and get the device rollups' retention.
    "rollup_operation_hour",
    "rollup_operation_day",
)

# How many months ahead the create-ahead job keeps partitions available. Six is
# not arbitrary: with `INGEST_MAX_FUTURE_SECONDS` at 5 minutes nothing legitimate
# lands more than a few minutes ahead, so this is entirely a margin against the
# job not running - and six months is long enough that a stopped job is noticed
# by the DEFAULT partition's row count rather than by an outage.
MONTHS_AHEAD: Final[int] = 6

# How many months BACK the create-ahead job also keeps available.
#
# Not symmetry with MONTHS_AHEAD - it is `INGEST_MAX_AGE_DAYS` rounded up. A
# legitimate message may be 35 days old (`ts_too_old` rejects beyond that), which
# reaches two calendar months back. Without this, a freshly provisioned or
# restored database has no partition for that month, the message lands in the
# DEFAULT partition, and /health/readiness goes red on day one looking exactly
# like a bug in ingest.
#
# `core/config.py` asserts at boot that this covers INGEST_MAX_AGE_DAYS.
MONTHS_BACK: Final[int] = 2

# protocol 3.1 / plan 2: the product's granularity. A `ts` not on this boundary is
# `ts_not_aligned`.
INTERVAL_SECONDS: Final[int] = 900


def month_start(moment: datetime.datetime) -> datetime.datetime:
    """The UTC midnight starting `moment`'s month.

    Requires an aware datetime: a naive one here is how a boundary silently moves
    by an hour twice a year.
    """
    if moment.tzinfo is None:
        raise ValueError("month_start requires an aware datetime (UTC)")
    utc = moment.astimezone(datetime.UTC)
    return datetime.datetime(utc.year, utc.month, 1, tzinfo=datetime.UTC)


def next_month_start(moment: datetime.datetime) -> datetime.datetime:
    """The UTC midnight starting the month AFTER `moment`'s."""
    start = month_start(moment)
    if start.month == 12:
        return start.replace(year=start.year + 1, month=1)
    return start.replace(month=start.month + 1)


def previous_month_start(moment: datetime.datetime) -> datetime.datetime:
    """The UTC midnight starting the month BEFORE `moment`'s."""
    start = month_start(moment)
    if start.month == 1:
        return start.replace(year=start.year - 1, month=12)
    return start.replace(month=start.month - 1)


def month_bounds(moment: datetime.datetime) -> tuple[str, str]:
    """The `FROM`/`TO` literals for `moment`'s monthly partition.

    Half-open: `[from, to)`, which is what `PARTITION OF ... FOR VALUES FROM (a)
    TO (b)` means. The explicit `+00` is the point of the function.
    """
    lower = month_start(moment)
    upper = next_month_start(moment)
    return (_literal(lower), _literal(upper))


def _literal(moment: datetime.datetime) -> str:
    """`2026-09-01 00:00:00+00` - the form Postgres renders under UTC."""
    return moment.strftime("%Y-%m-%d %H:%M:%S+00")


def partition_name(table: str, moment: datetime.datetime) -> str:
    """`measurement_2026_09`.

    Underscores rather than a bare `YYYYMM` so the month is readable in a
    `\\dt` listing, and lowercase because an unquoted identifier is folded.
    """
    start = month_start(moment)
    return f"{table}_{start.year:04d}_{start.month:02d}"


def default_partition_name(table: str) -> str:
    """`measurement_default`.

    A DEFAULT partition exists so that a row outside every range is STORED rather
    than raising - the difference between a late message kept and a late message
    lost. The cost is that a stopped create-ahead job is invisible at write time,
    which is why `api/health/routes.py` reports this table's row count and fails
    readiness above zero, and why step 8's job must DRAIN the default before it
    attaches a partition over that range (`ATTACH PARTITION` fails once the
    default holds rows there).
    """
    return f"{table}_default"


def months_to_provision(
    now: datetime.datetime,
    months_ahead: int = MONTHS_AHEAD,
    months_back: int = MONTHS_BACK,
) -> list[datetime.datetime]:
    """Month starts from `months_back` behind through `months_ahead` ahead.

    Inclusive at both ends, ascending. Used by tests and by step 8's job.
    `schema.sql` computes the same series in plpgsql; `tests/test_partitions.py`
    asserts the two agree.
    """
    if months_ahead < 0:
        raise ValueError("months_ahead must not be negative")
    if months_back < 0:
        raise ValueError("months_back must not be negative")
    start = month_start(now)
    for _ in range(months_back):
        start = previous_month_start(start)
    months = [start]
    for _ in range(months_back + months_ahead):
        months.append(next_month_start(months[-1]))
    return months


def is_aligned(moment: datetime.datetime, interval_s: int = INTERVAL_SECONDS) -> bool:
    """Whether `moment` sits on an `interval_s` boundary.

    Checked against the UTC epoch, not against local midnight: Europe/Brussels is
    a whole number of hours from UTC in both DST states, so for 900 s the two
    agree - but that is a property of this timezone and this interval, not a rule,
    and pinning it to the epoch means the check survives both changing.
    """
    if moment.tzinfo is None:
        raise ValueError("is_aligned requires an aware datetime")
    epoch_seconds = int(moment.astimezone(datetime.UTC).timestamp())
    return epoch_seconds % interval_s == 0 and moment.microsecond == 0
