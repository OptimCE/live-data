"""Drop partitions older than the retention window, and prune old dead letters.
Build step 8.

Detached and dropped, never mass-deleted: a `DELETE FROM measurement WHERE ts <
...` on thirteen months of quarter-hourly telemetry writes as much WAL as the
data itself and leaves the space to autovacuum. Dropping a partition is a file
unlink.

----------------------------------------------------------------------------
NOT `DETACH PARTITION CONCURRENTLY`, AND THE SPEC CANNOT HAVE BOTH.

plan 6.2 rule 2 requires a DEFAULT partition on every registry table. plan 6.4
requires `DETACH CONCURRENTLY`. PostgreSQL's ALTER TABLE reference settles it:

    "CONCURRENTLY cannot be run in a transaction block and is not allowed if the
     partitioned table contains a default partition."

Both halves of that sentence apply here, so the retention job AS SPECIFIED can
never run - it would raise on the first partition it tried to detach, every
night, for ever. Recorded in docs/live-data-decisions.md.

So: the plain form, inside a transaction, under `SET LOCAL lock_timeout`. The
plain DETACH takes ACCESS EXCLUSIVE on the parent, which conflicts with
everything - including the ingest worker's inserts. The timeout is what turns
"the whole service stalls behind a retention job at 02:00" into "the retention
job gave up and will try again tomorrow", and the retry budget is deliberately
small for the same reason.

DETACH and DROP are two statements in ONE transaction. Two statements because
dropping an attached partition is a different, heavier operation; one transaction
because the alternative the plan floated - separate transactions, leaving a
droppable orphan if interrupted - trades a clean rollback for a stray table that
nothing ever looks for again. An interruption here simply undoes itself.

`inhdetachpending` recovery still lives in `worker/partitions.py`, because a
human running a CONCURRENTLY detach by hand can still produce that state and it
blocks all partition DDL until finalised.
----------------------------------------------------------------------------

SELECTED BY PARSED BOUND, NEVER BY NAME.

`measurement_2026_01` is a convention, not a contract, and a partition attached
by hand with a different name would be invisible to a name-driven job - it would
survive every retention run and quietly keep personal data for ever. The bound
comes from `pg_get_expr(relpartbound)`, and the DEFAULT partition is excluded
explicitly rather than by failing to parse it.

----------------------------------------------------------------------------
`ingest_dead_letter` IS DELETED FROM, NOT DROPPED - AND BOUNDED TWICE.

It is not partitioned, and nothing used to prune it: every message-scoped
rejection, unknown device and unparseable topic writes a row, so a connector
that keeps publishing what the worker cannot store - misconfigured, or revoked
with its broker client still alive - grew it every fifteen minutes, for ever.
`prune_dead_letters` deletes rows older than RETENTION_DEAD_LETTER_DAYS, under
the retention job's own advisory lock, and it is bounded twice:

  - per BATCH. Each DELETE takes at most `_DEAD_LETTER_BATCH_ROWS` rows and
    commits on its own. One statement over a backlog of millions would be one
    transaction for as long as it ran, holding its row locks and pinning the
    vacuum horizon of the whole database;
  - per RUN. At most `_DEAD_LETTER_MAX_BATCHES` batches. Maintenance runs in the
    scheduler's single loop, so an unbounded prune would be rollup ticks not
    taken, with the retention lock held throughout. The rest waits for the next
    night, and the run says so.

Oldest first, so a capped run leaves the stretch just past the edge behind and
`min(received_at)` says how far behind it is. Selected on `received_at` ALONE:
every per-community read of this table must go through `device`, but a prune
that did would never touch the rows with no device - which are exactly the
ones a connector publishing for ever writes.
----------------------------------------------------------------------------
"""

import datetime
import logging
import re

from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import async_sessionmaker

from core.config import settings
from domain.partitions import month_start, previous_month_start

logger = logging.getLogger(__name__)

# `FOR VALUES FROM ('2026-01-01 00:00:00+00') TO ('2026-02-01 00:00:00+00')`
_BOUND_RE = re.compile(r"FOR VALUES FROM \('([^']+)'\) TO \('([^']+)'\)")

_LOCK_TIMEOUT = "5s"
_MAX_ATTEMPTS = 3

# The dead-letter prune's two bounds (see the module docstring). A night is
# capped at 500,000 rows - the whole output of some five thousand devices each
# failing every 15 minutes.
#
# MEASURED on Postgres 18 over a million-row table, 775,000 of them expired: a
# full capped run took 5.1 s, its batches 15 ms at first, 51 ms on average and
# 230 ms at worst - later batches walk the index entries earlier ones deleted.
# So no transaction here is long, and a capped night costs the scheduler's loop
# seconds, not ticks.
_DEAD_LETTER_BATCH_ROWS = 5_000
_DEAD_LETTER_MAX_BATCHES = 100


# Which table keeps what. `rollup_community_day` is ABSENT ON PURPOSE and has no
# setting: one row per community per day, carrying no device and no member, so
# nothing personal survives the aggregation - and it is the only series that can
# answer "how did we do last year". plan 16 caps the per-device dailies because
# per-device daily energy IS personal data; a community total is not.
def retention_months() -> dict[str, int]:
    return {
        "measurement": settings.RETENTION_RAW_MONTHS,
        "rollup_device_hour": settings.RETENTION_ROLLUP_DEVICE_HOUR_MONTHS,
        "rollup_device_day": settings.RETENTION_ROLLUP_DEVICE_DAY_MONTHS,
        "rollup_community_hour": settings.RETENTION_ROLLUP_COMMUNITY_HOUR_MONTHS,
        "rollup_operation_hour": settings.RETENTION_ROLLUP_OPERATION_HOUR_MONTHS,
        "rollup_operation_day": settings.RETENTION_ROLLUP_OPERATION_DAY_MONTHS,
    }


_PARTITIONS_SQL = text(
    """
    SELECT child.relname                                AS child,
           pg_get_expr(child.relpartbound, child.oid)   AS bound
      FROM pg_inherits i
      JOIN pg_class child  ON child.oid  = i.inhrelid
      JOIN pg_class parent ON parent.oid = i.inhparent
     WHERE parent.relname = :parent
       AND NOT i.inhdetachpending
     ORDER BY child.relname
    """
)


# `id = ANY(ARRAY(...))` rather than `id IN (SELECT ...)`, for a plan whose
# SHAPE is fixed: the array is built once, as an InitPlan, and the DELETE is one
# primary-key index scan over at most `:batch_rows` ids. The IN form is a
# semi-join whose method the planner picks from its estimates. It picked a sane
# nested loop on the benchmark above and cost the same - this is about not
# depending on that choice, not about the other form being slow.
#
# `ORDER BY received_at` is served by `ix_ingest_dead_letter_received_at`,
# scanned backwards (it is DESC), and stops after `:batch_rows` entries.
_PRUNE_DEAD_LETTERS_SQL = text(
    """
    DELETE FROM ingest_dead_letter
     WHERE id = ANY(ARRAY(
               SELECT id
                 FROM ingest_dead_letter
                WHERE received_at < :cutoff
                ORDER BY received_at
                LIMIT :batch_rows
           ))
    RETURNING id
    """
)

_DEAD_LETTERS_LEFT_SQL = text(
    "SELECT EXISTS (SELECT 1 FROM ingest_dead_letter WHERE received_at < :cutoff)"
)


def cutoff(now: datetime.datetime, months: int) -> datetime.datetime:
    """The month start `months` calendar months before `now`'s month.

    Calendar months rather than `months * 30 days`: a partition covers a calendar
    month, so a day-based cutoff lands mid-partition and either keeps an extra
    month or - much worse - makes the boundary drift until it eventually clips a
    month that still holds acceptable late data.
    """
    if months < 1:
        raise ValueError("retention months must be at least 1")
    start = month_start(now)
    for _ in range(months):
        start = previous_month_start(start)
    return start


def raw_floor(now: datetime.datetime) -> datetime.datetime:
    """The oldest hour bucket that can still be recomputed from raw readings.

    Below it `measurement` has been, or is about to be, dropped, so a rollup
    there must never be re-derived: recomputing from nothing DELETES it. One
    expression for the three places that ask - the startup backfill, the
    ownership refresh's dirty marks and the tick's claims.
    """
    return cutoff(now, settings.RETENTION_RAW_MONTHS)


def droppable(
    partitions: list[tuple[str, str]], *, before: datetime.datetime
) -> list[tuple[str, datetime.datetime]]:
    """The partitions entirely older than `before`, by their parsed upper bound.

    A partition is droppable only when its UPPER bound is at or below the cutoff -
    i.e. every row it can hold is older than the window. Testing the lower bound
    would drop the month the cutoff falls inside, taking live data with it.

    The DEFAULT partition has the bound `DEFAULT`, does not match the regex, and
    is skipped. That is the intended path, not a parse failure: dropping the
    default would turn every out-of-range insert from "stored somewhere visible"
    into an error on the ingest hot path.
    """
    out: list[tuple[str, datetime.datetime]] = []
    for name, bound in partitions:
        match = _BOUND_RE.search(bound or "")
        if match is None:
            continue
        upper = datetime.datetime.fromisoformat(match.group(2))
        if upper.tzinfo is None:
            upper = upper.replace(tzinfo=datetime.UTC)
        if upper <= before:
            out.append((name, upper))
    return sorted(out, key=lambda row: row[1])


async def apply_retention(
    sessions: async_sessionmaker,
    *,
    now: datetime.datetime,
    dry_run: bool = False,
) -> list[str]:
    """Detach and drop every partition past its table's retention. Returns names.

    One transaction PER PARTITION, so a lock timeout on one month does not
    abandon the others, and a partition dropped is a partition committed.
    """
    dropped: list[str] = []
    for table, months in retention_months().items():
        before = cutoff(now, months)
        async with sessions() as session:
            rows = (await session.execute(_PARTITIONS_SQL, {"parent": table})).all()
        candidates = droppable([(row.child, row.bound) for row in rows], before=before)

        for name, upper in candidates:
            if dry_run:
                logger.info("would drop %s (upper bound %s, cutoff %s)", name, upper, before)
                dropped.append(name)
                continue
            if await _detach_and_drop(sessions, parent=table, child=name):
                dropped.append(name)
    return dropped


async def _detach_and_drop(sessions: async_sessionmaker, *, parent: str, child: str) -> bool:
    """One partition, with a bounded retry on the lock.

    Retries only `OperationalError`, which is what a `lock_timeout` surfaces as.
    Anything else is a real fault and must not be swallowed into "we will try
    again tomorrow" - that is how a retention job appears to run for a year while
    dropping nothing.
    """
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            async with sessions() as session:
                await session.execute(text(f"SET LOCAL lock_timeout = '{_LOCK_TIMEOUT}'"))
                await session.execute(text(f'ALTER TABLE "{parent}" DETACH PARTITION "{child}"'))
                await session.execute(text(f'DROP TABLE "{child}"'))
                await session.commit()
            logger.info("dropped partition %s", child)
        except OperationalError:
            logger.warning(
                "could not take the lock to detach %s (attempt %d/%d)",
                child,
                attempt,
                _MAX_ATTEMPTS,
            )
            continue
        else:
            return True
    logger.error("giving up on %s for this run - the next one will retry", child)
    return False


def dead_letter_cutoff(now: datetime.datetime, days: int) -> datetime.datetime:
    """The instant a dead letter becomes prunable: exactly `days` before `now`.

    Days, not the calendar months of `cutoff`: the table is not partitioned, so
    there is no boundary for a day-based cutoff to land in the middle of. At
    least one, because `/ops/health` counts the last 24 hours - a shorter window
    would delete what it reads, and a zero or negative one would take every row.
    """
    if days < 1:
        raise ValueError("dead-letter retention must be at least 1 day")
    return now - datetime.timedelta(days=days)


async def prune_dead_letters(
    sessions: async_sessionmaker,
    *,
    now: datetime.datetime,
    days: int | None = None,
    batch_rows: int = _DEAD_LETTER_BATCH_ROWS,
    max_batches: int = _DEAD_LETTER_MAX_BATCHES,
) -> int:
    """Delete dead letters older than the window, oldest first. Returns the count.

    One transaction PER BATCH, as there is one per partition above: a batch
    deleted is a batch committed, and a failure in the next one does not undo
    it. The caller holds the retention advisory lock.

    Strictly older than the cutoff: a row exactly `days` old is kept.
    """
    before = dead_letter_cutoff(
        now, days if days is not None else settings.RETENTION_DEAD_LETTER_DAYS
    )
    pruned = 0
    for _ in range(max_batches):
        async with sessions() as session:
            result = await session.execute(
                _PRUNE_DEAD_LETTERS_SQL, {"cutoff": before, "batch_rows": batch_rows}
            )
            deleted = len(result.all())
            await session.commit()
        pruned += deleted
        if deleted < batch_rows:
            break
    else:
        # Every batch was full, which does not mean anything is LEFT - the last
        # one may have taken the last expired row. The warning is the backlog
        # signal, so it asks rather than assumes.
        async with sessions() as session:
            left = await session.scalar(_DEAD_LETTERS_LEFT_SQL, {"cutoff": before})
        if left:
            logger.warning(
                "dead-letter prune stopped at its per-run cap (%d batches of %d) with rows "
                "received before %s still present - the next run continues",
                max_batches,
                batch_rows,
                before,
            )
    if pruned:
        logger.info("pruned %d dead letter(s) received before %s", pruned, before)
    return pruned
