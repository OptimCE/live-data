"""Create monthly partitions ahead of time, draining the DEFAULT first.

`schema.sql` already carries `live_ensure_monthly_partitions()`, and this module
does NOT replace it: the plpgsql function is the one that names and bounds a
partition, so the schema and the scheduler cannot disagree (plan 6.2 - "the same
helper is used by the schema and by the scheduler", which is only literally true
if the helper is SQL).

What this module adds is the step that function cannot do for itself.

----------------------------------------------------------------------------
THE DRAIN, AND WHY THE JOB IS NOT JUST A CALL TO THE FUNCTION.

Every partitioned table here has a DEFAULT partition, so a row outside every
range is STORED rather than raising - the difference between a late message kept
and a late message lost.

The cost lands months later. `CREATE TABLE ... PARTITION OF ... FOR VALUES FROM
(a) TO (b)` must prove the default holds no row in [a, b). Postgres does that by
SCANNING the default, and if it finds one the statement FAILS. So once the
create-ahead job has been broken long enough for rows to accumulate in the
default, every subsequent attempt to create the partition that would fix it also
fails - at 00:00 UTC on the first of a month, for every community at once.

`schema.sql` says so at the call site, and names this module. The sequence here
is the one that breaks the cycle:

    LOCK the default ACCESS EXCLUSIVE   -- no new rows can land mid-flight
    COPY the in-range rows aside
    DELETE them from the default
    CREATE the partition                -- the default is now provably empty there
    RE-INSERT, and the parent routes them into the new partition

all in ONE transaction, so an interruption rolls back to "rows still in the
default" rather than to "rows nowhere".
----------------------------------------------------------------------------

AND `inhdetachpending` RECOVERY RUNS FIRST.

An interrupted `DETACH PARTITION CONCURRENTLY` leaves the child marked pending
detach, and a parent in that state REFUSES ALL FURTHER PARTITION DDL with an
error naming neither the child nor the reason. Retention here never uses
CONCURRENTLY (see `worker/retention.py`), but a human running one by hand can
still produce the state, and then every night's create-ahead fails until someone
knows to look. Finalising costs nothing when there is nothing to finalise.
"""

import datetime
import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from domain.partitions import (
    MONTHS_AHEAD,
    MONTHS_BACK,
    default_partition_name,
    month_bounds,
    month_start,
    months_to_provision,
    next_month_start,
    partition_name,
)

logger = logging.getLogger(__name__)

_REGISTRY_SQL = text("SELECT table_name FROM live_partitioned_table ORDER BY table_name")

# The partition key column, read from the catalog rather than from a second list
# in Python. `measurement` ranges on `ts` and the rollups on `bucket`, and a
# hard-coded mapping here would be a third place to keep in step with schema.sql.
_PARTITION_KEY_SQL = text(
    """
    SELECT a.attname
      FROM pg_partitioned_table p
      JOIN pg_class c ON c.oid = p.partrelid
      JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = p.partattrs[0]
     WHERE c.relname = :t
    """
)

_PARTITION_EXISTS_SQL = text(
    """
    SELECT 1
      FROM pg_class c
      JOIN pg_inherits i ON i.inhrelid = c.oid
      JOIN pg_class parent ON parent.oid = i.inhparent
     WHERE parent.relname = :parent AND c.relname = :child
    """
)

_PENDING_DETACH_SQL = text(
    """
    SELECT parent.relname AS parent, child.relname AS child
      FROM pg_inherits i
      JOIN pg_class child ON child.oid = i.inhrelid
      JOIN pg_class parent ON parent.oid = i.inhparent
     WHERE i.inhdetachpending
    """
)


async def finalise_pending_detaches(session: AsyncSession) -> list[str]:
    """Clear any half-finished DETACH. Returns the children finalised.

    Runs before anything else touches partition DDL - a parent with a pending
    detach refuses all of it.
    """
    rows = (await session.execute(_PENDING_DETACH_SQL)).all()
    finalised = []
    for row in rows:
        logger.warning("partition %s is pending detach from %s - finalising", row.child, row.parent)
        await session.execute(
            text(f'ALTER TABLE {row.parent} DETACH PARTITION "{row.child}" FINALIZE')
        )
        finalised.append(row.child)
    return finalised


async def partition_key(session: AsyncSession, table: str) -> str:
    key = await session.scalar(_PARTITION_KEY_SQL, {"t": table})
    if key is None:
        raise RuntimeError(f"{table} is in live_partitioned_table but is not partitioned")
    return str(key)


async def create_partition_draining_default(
    session: AsyncSession, *, table: str, month: datetime.datetime
) -> int:
    """Create one monthly partition, moving any blocking rows out of the default.

    Returns True when a partition was created, False when it already existed.

    The caller supplies the transaction. Everything below must commit or roll
    back together: a drain without the matching create leaves rows deleted from
    the default and inserted nowhere.
    """
    child = partition_name(table, month)
    if await session.scalar(_PARTITION_EXISTS_SQL, {"parent": table, "child": child}):
        return -1

    key = await partition_key(session, table)
    default_child = default_partition_name(table)
    # TWO representations of the same two instants, and both are needed.
    # `month_bounds` returns the `+00`-suffixed STRING literals the DDL wants -
    # partition bounds cannot be parameterised, so they are interpolated. asyncpg
    # refuses those same strings as bind parameters, so the drain's WHERE clause
    # takes the datetimes.
    lower, upper = month_bounds(month)
    lower_ts, upper_ts = month_start(month), next_month_start(month)

    # ACCESS EXCLUSIVE on the DEFAULT only, never on the parent: the parent's
    # other partitions keep taking inserts throughout. Rows that would have gone
    # to the default block for the duration, which is the correct trade - they
    # are the rows being moved.
    await session.execute(text(f'LOCK TABLE "{default_child}" IN ACCESS EXCLUSIVE MODE'))
    in_range = {"lower": lower_ts, "upper": upper_ts}
    # `key` comes from pg_partitioned_table and `default_child` from the registry
    # via `default_partition_name`. Neither is reachable from user input, which is
    # what the S608 suppressions below assert; the VALUES are bind parameters.
    where_month = f"{key} >= :lower AND {key} < :upper"
    count_sql = f'SELECT count(*) FROM "{default_child}" WHERE {where_month}'  # noqa: S608
    drained = await session.scalar(text(count_sql), in_range) or 0

    # Counted under the lock and only THEN materialised. The count is what makes
    # the temp table conditional, and the temp table has to be conditional:
    # `ensure_partitions` calls this once per (table, month) inside ONE
    # transaction, and `_drain` is a single name - creating it unconditionally
    # fails on the second call with "relation already exists", which would take
    # out the whole nightly run the first time two partitions were missing.
    if drained:
        logger.warning(
            "draining %d row(s) out of %s for %s - the create-ahead job had fallen behind",
            drained,
            default_child,
            child,
        )
        await session.execute(text("DROP TABLE IF EXISTS _drain"))
        select_rows = f'SELECT * FROM "{default_child}" WHERE {where_month}'  # noqa: S608
        await session.execute(
            text(f"CREATE TEMP TABLE _drain ON COMMIT DROP AS {select_rows}"), in_range
        )
        delete_sql = f'DELETE FROM "{default_child}" WHERE {where_month}'  # noqa: S608
        await session.execute(text(delete_sql), in_range)

    await session.execute(
        text(
            f'CREATE TABLE IF NOT EXISTS "{child}" PARTITION OF "{table}" '
            f"FOR VALUES FROM ('{lower}') TO ('{upper}')"
        )
    )
    if drained:
        # Into the PARENT, so routing puts them in the partition just made.
        await session.execute(text(f'INSERT INTO "{table}" SELECT * FROM _drain'))  # noqa: S608
        await session.execute(text("DROP TABLE _drain"))
    # The ROW COUNT, not a flag. -1 means "already existed, nothing done"; 0 and
    # above mean the partition was created and that many rows had to be moved out
    # of the default first. The caller needs both facts and a bool carries one.
    return drained


async def ensure_partitions(
    session: AsyncSession,
    *,
    now: datetime.datetime,
    months_ahead: int = MONTHS_AHEAD,
    months_back: int = MONTHS_BACK,
) -> tuple[list[str], int]:
    """Create every missing partition, and say how many rows had to be MOVED.

    Returns `(created, drained)`. The drain count is the one that matters most
    and used to be discarded inside `create_partition_draining_default`: a
    non-zero value means rows had already landed in a DEFAULT partition and were
    moved out so the real one could be attached - the create-ahead job had
    fallen behind and this run repaired it. Left uncounted, the repair is
    invisible and the cycle looks like it never happened.

    Driven by `live_partitioned_table`, NOT by a list in this file. plan 6.2 names
    the failure that prevents: "a job that names only `measurement` lets the
    rollups freeze about four months in while ingestion goes on looking perfectly
    healthy."
    """
    await finalise_pending_detaches(session)
    tables = list((await session.execute(_REGISTRY_SQL)).scalars().all())
    created: list[str] = []
    drained_total = 0
    for table in tables:
        for month in months_to_provision(now, months_ahead=months_ahead, months_back=months_back):
            drained = await create_partition_draining_default(session, table=table, month=month)
            if drained < 0:
                continue  # the partition already existed
            created.append(partition_name(table, month))
            drained_total += drained
    if created:
        logger.info("created %d partition(s): %s", len(created), ", ".join(created))
    return created, drained_total


async def default_partition_counts(session: AsyncSession) -> dict[str, int]:
    """Rows sitting in each default partition. Zero is the healthy answer.

    Read by `/health/readiness` and by the scheduler's own logging. A non-empty
    default means the create-ahead job has stopped; nothing fails at write time
    when that happens, so this count is the only signal there is.
    """
    tables = list((await session.execute(_REGISTRY_SQL)).scalars().all())
    counts: dict[str, int] = {}
    for table in tables:
        default_child = default_partition_name(table)
        counts[default_child] = int(
            await session.scalar(text(f'SELECT count(*) FROM "{default_child}"')) or 0  # noqa: S608
        )
    return counts


def month_after(month: datetime.datetime) -> datetime.datetime:
    """Re-exported so retention can express a partition's upper bound."""
    return next_month_start(month)
