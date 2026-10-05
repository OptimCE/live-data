"""The rollup tick. Build step 8, and the riskiest thing in this service.

Recomputed on a 15-minute tick rather than incremented at insert time, because
late messages are the norm: a device with store-and-forward reconnects after a
week and delivers the week.

NO `NOW()` APPEARS IN ANY STATEMENT HERE. `now` is a parameter, everywhere, and
that is what makes the T / T+49 h erosion test possible at all. A single
`now()` in the SQL and the test can only ever assert against the wall clock.

----------------------------------------------------------------------------
MARK AND SWEEP: ONE PREDICATE, USED TWICE.

`_TARGET` is the string `bucket = ANY(:targets)`, and it appears in BOTH the
DELETE and the INSERT of every pass. `tests/test_rollups.py` asserts that
literal is present in both statements of each pair.

The reason it is a constant rather than two hand-written clauses: if the DELETE
is NARROWER than the INSERT, the INSERT hits the primary key and the transaction
aborts - loud, immediate, fixed the same day. If the DELETE is WIDER, rows are
removed and never reinserted, and the only symptom is a hole in a chart that
nobody can date. The failure modes are wildly asymmetric and only one of them is
survivable, so the predicate is written once.

The target set is computed in PYTHON, as an explicit array of bucket instants:
the 48 aligned hours of the window, plus whatever was claimed from
`rollup_dirty`. Expressing it as an array rather than as a range-plus-exceptions
is what keeps the two statements textually identical; the separate `:scan_lo` /
`:scan_hi` bounds exist ONLY to let the planner use `ix_measurement_community_ts`
and never define membership.
----------------------------------------------------------------------------

THE CLAIM IS THE FIRST STATEMENT OF THE TRANSACTION.

`DELETE FROM rollup_dirty ... RETURNING bucket` claims work by removing it. The
claim and the recompute therefore commit together: either both happen or neither
does, and no bucket is ever marked clean without being recomputed. That is safe
at READ COMMITTED and ONLY there - it is the isolation level this runs at, and
changing it is not a tuning decision.

----------------------------------------------------------------------------
THE THREE TRAPS THAT LOOK LIKE CORRECT CODE.

1.  `date_trunc('hour', ts)`. `measurement.ts` is the END of its interval, so
    that expression moves a quarter of every hour's energy into the next hour,
    for ever, silently. See `domain/buckets.py`; the `- 1 second` form is used
    here and nothing else is acceptable.

2.  AN UNALIGNED WINDOW. `lo = now - 48h` truncates the oldest hour as it leaves
    the window and overwrites a correct row with a partial one. `buckets.window`
    returns a pair aligned at both ends and is the only source of them.

3.  DAY ROLLUPS FROM PARTIAL DAYS. One dirty bucket ten days old would otherwise
    produce a one-hour "day" total that then freezes for ever, because closed
    periods are never revisited. `buckets.day_targets` filters to CLOSED days and
    the day pass reads `rollup_device_hour`, never the hours just recomputed.
"""

import datetime
import logging
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from domain import buckets
from shared.const import NO_SHARING_OPERATION, ROLLUP_DAY_TIMEZONE
from worker import retention

logger = logging.getLogger(__name__)

# The predicate. Written once, substituted into both halves of every pass.
_TARGET = "bucket = ANY(:targets)"

_COMMUNITIES_SQL = text("SELECT DISTINCT id_community FROM device ORDER BY id_community")

# FIRST statement of the transaction. See the module docstring.
_CLAIM_SQL = text("DELETE FROM rollup_dirty WHERE id_community = :c RETURNING bucket")

_DELETE_DEVICE_HOUR_SQL = text(
    f"DELETE FROM rollup_device_hour WHERE id_community = :c AND {_TARGET}"  # noqa: S608
)

# The inner SELECT derives `bucket` from `ts`; the outer WHERE applies the same
# `_TARGET` the DELETE used. `m.ts > :scan_lo AND m.ts <= :scan_hi` is HALF-OPEN
# AT THE BOTTOM AND CLOSED AT THE TOP, which is not a typo: because `ts` is an
# interval end, `bucket(ts) in [lo, hi)` is exactly `ts in (lo, hi]`.
_INSERT_DEVICE_HOUR_SQL = text(
    f"""
    INSERT INTO rollup_device_hour
        (id_device, bucket, id_community, import_wh, export_wh, production_wh,
         n_samples, n_production_samples, computed_at)
    SELECT src.id_device,
           src.bucket,
           src.id_community,
           SUM(src.import_wh),
           SUM(src.export_wh),
           -- SUM ignores NULLs, so an hour where every reading is NULL yields
           -- NULL rather than 0. That is the whole point: NULL means "this
           -- device does not measure production", and 0 would assert "produced
           -- nothing" - a different and false statement.
           SUM(src.production_wh),
           COUNT(*),
           COUNT(src.production_wh),
           :now
      FROM (
            SELECT m.id_device,
                   m.id_community,
                   m.import_wh,
                   m.export_wh,
                   m.production_wh,
                   {buckets.bucket_sql("m.ts")} AS bucket
              FROM measurement m
             WHERE m.id_community = :c
               AND m.ts > :scan_lo
               AND m.ts <= :scan_hi
           ) src
     WHERE {_TARGET}
     GROUP BY src.id_device, src.bucket, src.id_community
    """
)

_DELETE_COMMUNITY_HOUR_SQL = text(
    f"DELETE FROM rollup_community_hour WHERE id_community = :c AND {_TARGET}"  # noqa: S608
)

# `energy` and `membership` are computed in SEPARATE CTEs over the same rows, and
# that separation is load-bearing rather than tidy.
#
# `membership` JOINs `device_owner_window`. A join against that table can fan out
# - one device with two windows covering the same day produces two rows - and a
# fan-out inside the energy aggregation MULTIPLIES THE COMMUNITY TOTAL. Keeping
# the join in a CTE that carries no energy column makes that impossible to write
# by accident.
#
# The join condition itself already excludes ambiguous and member-less windows,
# so an unattributed device produces a NULL `w` row rather than disappearing:
# `n_devices` must keep counting it, because its energy is still in the total.
_INSERT_COMMUNITY_HOUR_SQL = text(
    f"""
    INSERT INTO rollup_community_hour
        (id_community, bucket, import_wh, export_wh, production_wh,
         n_devices, n_devices_production, n_members, n_devices_unattributed, computed_at)
    WITH target AS (
        SELECT r.id_device, r.bucket, r.id_community,
               r.import_wh, r.export_wh, r.production_wh
          FROM rollup_device_hour r
         WHERE r.id_community = :c AND r.{_TARGET}
    ),
    energy AS (
        SELECT t.bucket,
               SUM(t.import_wh)      AS import_wh,
               SUM(t.export_wh)      AS export_wh,
               SUM(t.production_wh)  AS production_wh,
               COUNT(*)              AS n_devices,
               COUNT(t.production_wh) AS n_devices_production
          FROM target t
         GROUP BY t.bucket
    ),
    membership AS (
        SELECT t.bucket,
               COUNT(DISTINCT w.id_member) AS n_members,
               COUNT(DISTINCT t.id_device) FILTER (WHERE w.id_member IS NOT NULL)
                   AS n_attributed
          FROM target t
          JOIN device d ON d.id = t.id_device
          LEFT JOIN device_owner_window w
                 ON w.ean = d.ean
                AND w.id_community = d.id_community
                AND NOT w.ambiguous
                AND w.id_member IS NOT NULL
                AND (t.bucket AT TIME ZONE :tz)::date
                    BETWEEN w.valid_from AND COALESCE(w.valid_to, 'infinity'::date)
         GROUP BY t.bucket
    )
    SELECT :c,
           e.bucket,
           e.import_wh,
           e.export_wh,
           e.production_wh,
           e.n_devices,
           e.n_devices_production,
           m.n_members,
           e.n_devices - m.n_attributed,
           :now
      FROM energy e
      JOIN membership m ON m.bucket = e.bucket
    """
)

_DELETE_DEVICE_DAY_SQL = text(
    f"DELETE FROM rollup_device_day WHERE id_community = :c AND {_TARGET}"  # noqa: S608
)

# `date_trunc('day', bucket AT TIME ZONE tz) AT TIME ZONE tz` and never
# `bucket::date` or `day + 24h`: the last Sunday in October is a 25-hour local day
# and the last Sunday in March a 23-hour one. Truncating the LOCAL timestamp and
# converting back is the only form that gets both right, and `n_hours` then
# reports 25 and 23 - which is correct data, not an anomaly.
_DAY_EXPR = "date_trunc('day', r.bucket AT TIME ZONE :tz) AT TIME ZONE :tz"

_INSERT_DEVICE_DAY_SQL = text(
    f"""
    INSERT INTO rollup_device_day
        (id_device, bucket, id_community, import_wh, export_wh, production_wh,
         n_hours, n_production_hours, computed_at)
    SELECT src.id_device,
           src.bucket,
           src.id_community,
           SUM(src.import_wh),
           SUM(src.export_wh),
           SUM(src.production_wh),
           COUNT(*),
           COUNT(src.production_wh),
           :now
      FROM (
            SELECT r.id_device, r.id_community, r.import_wh, r.export_wh, r.production_wh,
                   {_DAY_EXPR} AS bucket
              FROM rollup_device_hour r
             WHERE r.id_community = :c
               AND r.bucket >= :scan_lo
               AND r.bucket < :scan_hi
           ) src
     WHERE {_TARGET}
     GROUP BY src.id_device, src.bucket, src.id_community
    """
)

_DELETE_COMMUNITY_DAY_SQL = text(
    f"DELETE FROM rollup_community_day WHERE id_community = :c AND {_TARGET}"  # noqa: S608
)

# `n_members` is MAX over the day's hours, NEVER SUM. SUM gives a five-member
# community an `n_members` of 120 and k then passes on a day that should have
# been suppressed. MAX is a valid lower bound on the day's distinct union, so it
# fails closed.
#
# `n_members_min` is the day's least-populated hour (migration 0003). Judging a
# day on its MAX let "day minus its published hours" reveal the withheld hours;
# the read side now publishes a day only if every hour of it passed.
_INSERT_COMMUNITY_DAY_SQL = text(
    f"""
    INSERT INTO rollup_community_day
        (id_community, bucket, import_wh, export_wh, production_wh,
         n_devices, n_devices_production, n_members, n_devices_unattributed,
         n_hours, computed_at, n_members_min)
    SELECT :c,
           src.bucket,
           SUM(src.import_wh),
           SUM(src.export_wh),
           SUM(src.production_wh),
           MAX(src.n_devices),
           MAX(src.n_devices_production),
           MAX(src.n_members),
           MAX(src.n_devices_unattributed),
           COUNT(*),
           :now,
           MIN(src.n_members)
      FROM (
            SELECT r.import_wh, r.export_wh, r.production_wh, r.n_devices,
                   r.n_devices_production, r.n_members, r.n_devices_unattributed,
                   {_DAY_EXPR} AS bucket
              FROM rollup_community_hour r
             WHERE r.id_community = :c
               AND r.bucket >= :scan_lo
               AND r.bucket < :scan_hi
           ) src
     WHERE {_TARGET}
     GROUP BY src.bucket
    """
)

_DELETE_OPERATION_HOUR_SQL = text(
    f"DELETE FROM rollup_operation_hour WHERE id_community = :c AND {_TARGET}"  # noqa: S608
)

# One row per (operation, bucket), plus the REMAINDER row `:remainder` (0) for
# every device in no operation at that bucket - so a bucket's rows sum to the
# community hour EXACTLY, which domain/kanon.py's global verdict depends on.
#
# ---------------------------------------------------------------------------
# `attribution` RESOLVES EACH (device, bucket) TO EXACTLY ONE OPERATION.
#
# Rollup invariant 4: a join against time-sliced ownership multiplies totals. So
# the window join lives in its own CTE, GROUPED back to one row per device-bucket
# - `COUNT(w.id) = 1` takes the window's operation and member; zero windows, or
# more than one (which `NOT ambiguous` should already have excluded), send the
# device to the remainder with no member. Every later CTE joins `attribution`
# 1:1, so a fan-out cannot reach an energy sum. The window is matched on the
# BUCKET's local date, the rule worker/ownership.py documents and the community
# membership uses - and it is what makes an operation's hour the sum of its
# quarters: a meter changes operation only at a date boundary.
#
# Energies come from the `rollup_device_hour` rows just written, NOT from a second
# read of `measurement`, so the operation rows and the community hour are sums of
# the very same numbers.
#
# ---------------------------------------------------------------------------
# `shared_wh`: PER QUARTER, THEN SUMMED. NEVER FROM HOURLY SUMS.
#
# LEAST(sum of export, sum of import) over the operation's devices at one `ts`,
# then summed into the hour. Computed from hourly sums it would let a 10:15
# surplus cover a 10:45 offtake - energy that was never shared. An ESTIMATE among
# monitored meters (D-14), NULL exactly on the remainder row, as the CHECK on the
# table requires. Postgres LEAST ignores NULLs, which is safe only because
# `measurement.import_wh`/`export_wh` are NOT NULL.
# ---------------------------------------------------------------------------
_INSERT_OPERATION_HOUR_SQL = text(
    f"""
    INSERT INTO rollup_operation_hour
        (id_community, id_sharing_operation, bucket, import_wh, export_wh, production_wh,
         shared_wh, n_devices, n_devices_production, n_members, computed_at)
    WITH target AS (
        SELECT r.id_device, r.bucket, r.import_wh, r.export_wh, r.production_wh
          FROM rollup_device_hour r
         WHERE r.id_community = :c AND r.{_TARGET}
    ),
    attribution AS (
        SELECT t.id_device,
               t.bucket,
               CASE WHEN COUNT(w.id) = 1
                    THEN COALESCE(MIN(w.id_sharing_operation), :remainder)
                    ELSE :remainder
               END AS id_op,
               CASE WHEN COUNT(w.id) = 1 THEN MIN(w.id_member) END AS id_member
          FROM target t
          JOIN device d ON d.id = t.id_device
          LEFT JOIN device_owner_window w
                 ON w.ean = d.ean
                AND w.id_community = d.id_community
                AND NOT w.ambiguous
                AND (t.bucket AT TIME ZONE :tz)::date
                    BETWEEN w.valid_from AND COALESCE(w.valid_to, 'infinity'::date)
         GROUP BY t.id_device, t.bucket
    ),
    energy AS (
        SELECT a.id_op,
               t.bucket,
               SUM(t.import_wh)            AS import_wh,
               SUM(t.export_wh)            AS export_wh,
               SUM(t.production_wh)        AS production_wh,
               COUNT(*)                    AS n_devices,
               COUNT(t.production_wh)      AS n_devices_production,
               COUNT(DISTINCT a.id_member) AS n_members
          FROM target t
          JOIN attribution a ON a.id_device = t.id_device AND a.bucket = t.bucket
         GROUP BY a.id_op, t.bucket
    ),
    quarter AS (
        SELECT a.id_op,
               q.bucket,
               LEAST(SUM(q.export_wh), SUM(q.import_wh)) AS shared_wh
          FROM (
                SELECT m.id_device, m.ts, m.import_wh, m.export_wh,
                       {buckets.bucket_sql("m.ts")} AS bucket
                  FROM measurement m
                 WHERE m.id_community = :c AND m.ts > :scan_lo AND m.ts <= :scan_hi
               ) q
          JOIN attribution a ON a.id_device = q.id_device AND a.bucket = q.bucket
         WHERE a.id_op <> :remainder
         GROUP BY a.id_op, q.bucket, q.ts
    ),
    shared AS (
        SELECT id_op, bucket, SUM(shared_wh) AS shared_wh
          FROM quarter
         GROUP BY id_op, bucket
    )
    SELECT :c,
           e.id_op,
           e.bucket,
           e.import_wh,
           e.export_wh,
           e.production_wh,
           CASE WHEN e.id_op = :remainder THEN NULL ELSE COALESCE(s.shared_wh, 0) END,
           e.n_devices,
           e.n_devices_production,
           e.n_members,
           :now
      FROM energy e
      LEFT JOIN shared s ON s.id_op = e.id_op AND s.bucket = e.bucket
    """  # noqa: S608 - interpolates module constants only
)

_DELETE_OPERATION_DAY_SQL = text(
    f"DELETE FROM rollup_operation_day WHERE id_community = :c AND {_TARGET}"  # noqa: S608
)

# From the operation hours, exactly like the community day: SUM the energies, MAX
# the counts. Plus `n_members_min`, the day's least-populated hour - a day's grid
# terms are published only on it (domain/kanon.py), or "day minus its published
# hours" would be the withheld ones. SUM(shared_wh) stays NULL on the remainder.
_INSERT_OPERATION_DAY_SQL = text(
    f"""
    INSERT INTO rollup_operation_day
        (id_community, id_sharing_operation, bucket, import_wh, export_wh, production_wh,
         shared_wh, n_devices, n_devices_production, n_members, n_members_min, n_hours,
         computed_at)
    SELECT :c,
           src.id_sharing_operation,
           src.bucket,
           SUM(src.import_wh),
           SUM(src.export_wh),
           SUM(src.production_wh),
           SUM(src.shared_wh),
           MAX(src.n_devices),
           MAX(src.n_devices_production),
           MAX(src.n_members),
           MIN(src.n_members),
           COUNT(*),
           :now
      FROM (
            SELECT r.id_sharing_operation, r.import_wh, r.export_wh, r.production_wh,
                   r.shared_wh, r.n_devices, r.n_devices_production, r.n_members,
                   {_DAY_EXPR} AS bucket
              FROM rollup_operation_hour r
             WHERE r.id_community = :c
               AND r.bucket >= :scan_lo
               AND r.bucket < :scan_hi
           ) src
     WHERE {_TARGET}
     GROUP BY src.id_sharing_operation, src.bucket
    """
)


@dataclass(frozen=True, slots=True)
class TickResult:
    communities: int = 0
    hour_buckets: int = 0
    day_buckets: int = 0
    claimed: int = 0


def hour_targets(
    lo: datetime.datetime, hi: datetime.datetime, claimed: list[datetime.datetime]
) -> list[datetime.datetime]:
    """Every bucket this tick recomputes: the aligned window, plus the claims.

    A sorted list rather than a set, so the `:targets` array is deterministic and
    a failing test names the same buckets twice.
    """
    hours: set[datetime.datetime] = set(claimed)
    cursor = lo
    while cursor < hi:
        hours.add(cursor)
        cursor += datetime.timedelta(hours=1)
    return sorted(hours)


async def tick_community(
    session: AsyncSession, *, id_community: int, now: datetime.datetime
) -> tuple[int, int, int]:
    """Recompute one community. Returns (hour_buckets, day_buckets, claimed).

    Does NOT commit - the caller owns the transaction boundary, which is what
    lets the scheduler commit per community and a test run inside its rolled-back
    one.
    """
    claimed_rows = await session.execute(_CLAIM_SQL, {"c": id_community})
    claimed = [row.bucket for row in claimed_rows]

    # A claim below the raw retention edge is consumed and NOT recomputed: its
    # readings are gone, and recomputing from nothing would delete the hour and
    # re-derive its day from nothing - erasing `rollup_community_day`, the one
    # series kept for ever. The ownership refresh no longer marks such buckets;
    # this is the second guard, for any other writer of `rollup_dirty`.
    floor = retention.raw_floor(now)
    recomputable = [bucket for bucket in claimed if bucket >= floor]

    lo, hi = buckets.window(now)
    targets = hour_targets(lo, hi, recomputable)

    # Sargability only. `ix_measurement_community_ts` leads on (id_community, ts),
    # and without a bounded range on `ts` this is a sequential scan of the largest
    # table in the service. The array is what defines membership.
    scan_lo = targets[0]
    scan_hi = targets[-1] + datetime.timedelta(hours=1)
    hour_params = {
        "c": id_community,
        "targets": targets,
        "scan_lo": scan_lo,
        "scan_hi": scan_hi,
        "now": now,
        "tz": ROLLUP_DAY_TIMEZONE,
    }

    await session.execute(_DELETE_DEVICE_HOUR_SQL, hour_params)
    await session.execute(_INSERT_DEVICE_HOUR_SQL, hour_params)
    await session.execute(_DELETE_COMMUNITY_HOUR_SQL, hour_params)
    await session.execute(_INSERT_COMMUNITY_HOUR_SQL, hour_params)
    # After the device hours it reads, in the same transaction as the community
    # hour it must sum to.
    await session.execute(_DELETE_OPERATION_HOUR_SQL, hour_params)
    await session.execute(
        _INSERT_OPERATION_HOUR_SQL, {**hour_params, "remainder": NO_SHARING_OPERATION}
    )

    # CLOSED days only, derived from the hour rollups that now exist rather than
    # from the measurements just read.
    days = buckets.day_targets(targets, now)
    if days:
        day_params = {
            "c": id_community,
            "targets": days,
            "scan_lo": days[0],
            "scan_hi": buckets.local_day_end(days[-1]),
            "now": now,
            "tz": ROLLUP_DAY_TIMEZONE,
        }
        await session.execute(_DELETE_DEVICE_DAY_SQL, day_params)
        await session.execute(_INSERT_DEVICE_DAY_SQL, day_params)
        await session.execute(_DELETE_COMMUNITY_DAY_SQL, day_params)
        await session.execute(_INSERT_COMMUNITY_DAY_SQL, day_params)
        await session.execute(_DELETE_OPERATION_DAY_SQL, day_params)
        await session.execute(_INSERT_OPERATION_DAY_SQL, day_params)

    return len(targets), len(days), len(claimed)


async def run_tick(session: AsyncSession, *, now: datetime.datetime) -> TickResult:
    """Recompute every community, in the caller's single transaction.

    Used by tests and by the one-shot path. The scheduler uses
    `worker.scheduler.run_rollups`, which opens a transaction per community so
    one poisoned community cannot abort the rest.

    BLIND TO THE LIVE-DATA SUBSCRIPTION, on purpose. The scheduler's
    `run_rollups` is what drains and then skips a switched-off community (D-12);
    this ticks every community with a device, because its callers - the verify
    script's rollup section and ad-hoc diagnostics - want the recompute itself,
    not the policy. Do not filter it.
    """
    ids = list((await session.execute(_COMMUNITIES_SQL)).scalars().all())
    hours = days = claimed = 0
    for id_community in ids:
        community_hours, community_days, community_claimed = await tick_community(
            session, id_community=id_community, now=now
        )
        hours += community_hours
        days += community_days
        claimed += community_claimed
    return TickResult(communities=len(ids), hour_buckets=hours, day_buckets=days, claimed=claimed)
