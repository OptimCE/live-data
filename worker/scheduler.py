"""What the scheduler runs, and when. Build step 8.

Four jobs, four advisory locks (`shared/const.py`), so one slow job cannot
exclude another - plan 6.3. The locks also make a second replica a no-op, which
is why this container, unlike the ingest worker, is not pinned to one.

----------------------------------------------------------------------------
WHY THIS IS NOT IN THE INGEST WORKER.

`worker/main.py` touches `/tmp/worker.alive` ONLY while the broker connection is
up, and `Dockerfile.worker`'s HEALTHCHECK kills the container when that file is
60 s stale. Both are deliberate and documented at both ends: an ingest worker
that cannot reach the broker is doing nothing and should be restarted.

Co-hosting these jobs there would mean a one-minute broker hiccup SIGKILLs a
partition ATTACH or a retention DROP mid-flight. So the scheduler is its own
container, with its own UNGATED heartbeat on `/tmp/scheduler.alive`, a
compose-level healthcheck override pointing at that file, and no MQTT
credentials at all - it has no reason to hold any.
----------------------------------------------------------------------------

THE TICK RUNS ON THE WALL CLOCK, NOT ON A SLEEP INTERVAL.

`asyncio.sleep(900)` drifts: every restart re-phases the grid, and after a few
days the tick that closes an hour lands at a different offset than it did on
Monday. `next_tick` computes the next slot from the clock, so the phase is a
property of the deployment rather than of its last restart.

----------------------------------------------------------------------------
A SWITCHED-OFF COMMUNITY IS DRAINED, THEN SKIPPED (D-12).

`run_rollups` takes the set of communities whose live-data subscription is
active. A community outside it is still ticked while it has PENDING work - dirty
buckets, or a reading inside the 48-hour window - so what the worker accepted
before the switch-off still reaches the rollups; after that it is skipped. The
skip is exact, not an approximation: with nothing dirty and nothing in the
window, the tick's DELETE/INSERT pairs would rewrite nothing. An UNKNOWN set
(`None`, the CRM has never answered) ticks everyone: recomputing stored data is
never wrong, and failing closed would freeze every community through a CRM
outage.

The ownership projection refreshes ACTIVE communities only, and refuses to run
without the set. Partitions, retention and the backfill take no set at all:
they are table-wide, and retention in particular is a GDPR cap that must keep
reaching a switched-off community's history.
----------------------------------------------------------------------------
"""

import contextlib
import datetime
import logging
import time
from collections.abc import AsyncIterator, Sequence
from collections.abc import Set as AbstractSet
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from core import metrics as app_metrics
from core.config import settings
from domain import buckets
from ports.crm_core import SqlAlchemyCrmCoreRead
from shared.const import (
    ADVISORY_LOCK_OWNERSHIP,
    ADVISORY_LOCK_PARTITIONS,
    ADVISORY_LOCK_RETENTION,
    ADVISORY_LOCK_ROLLUPS,
)
from worker import partitions, retention, rollups
from worker.context import advisory_lock
from worker.ownership import refresh_ownership
from worker.subscriptions import SubscriptionsUnavailable

logger = logging.getLogger(__name__)

_COMMUNITIES_SQL = text("SELECT DISTINCT id_community FROM device ORDER BY id_community")

# The communities with rollup work still to do, whatever their subscription says.
#
# `device_last.ts` is the newest reading's DEVICE clock - status never writes it -
# and `ts > :lo` holds exactly when some reading lands in a window bucket, because
# `ts` is an interval END and `bucket(ts) in [lo, hi)` is `ts in (lo, hi]`.
_PENDING_SQL = text(
    """
    SELECT id_community FROM rollup_dirty
    UNION
    SELECT id_community FROM device_last WHERE ts > :lo
    """
)

# Bootstrap. Marks every community-hour that HAS measurements and has no
# community rollup row, so a deployment onto a database with existing telemetry
# rolls its history up instead of publishing the last 48 hours and nothing else.
#
# Bounded by the raw retention window, because a bucket older than that is about
# to be dropped and recomputing it would be work thrown away. Run ONCE at
# startup, and self-limiting: after the first successful pass it matches nothing.
_BACKFILL_SQL = text(
    f"""
    INSERT INTO rollup_dirty (id_community, bucket)
    SELECT s.id_community, s.bucket
      FROM (
            SELECT DISTINCT m.id_community, {buckets.bucket_sql("m.ts")} AS bucket
              FROM measurement m
             WHERE m.ts > :floor
           ) s
     WHERE NOT EXISTS (
           SELECT 1 FROM rollup_community_hour r
            WHERE r.id_community = s.id_community AND r.bucket = s.bucket
     )
    ON CONFLICT DO NOTHING
    RETURNING bucket
    """  # noqa: S608
)

# The operation rollups' bootstrap (migration 0003, D-14). Every community hour
# already rolled up but with NO operation row is marked, so the first ticks after
# the migration recompute history into `rollup_operation_hour` rather than
# publishing per-operation figures for the last 48 hours only.
#
# SELF-LIMITING by construction, not by a flag: every recomputed community hour
# gets at least one operation row - the remainder row 0 when its devices belong
# to no operation - so after one pass this matches nothing. And it does not wait
# for the ownership refresh to notice a change: a community whose windows carry
# no operation never "changes", and would otherwise never be backfilled.
#
# Bounded by the raw retention edge like the backfill above: below it the
# readings are gone, and a recompute would delete what it cannot rebuild.
_BACKFILL_OPERATIONS_SQL = text(
    """
    INSERT INTO rollup_dirty (id_community, bucket)
    SELECT r.id_community, r.bucket
      FROM rollup_community_hour r
     WHERE r.bucket >= :floor
       AND NOT EXISTS (
           SELECT 1 FROM rollup_operation_hour o
            WHERE o.id_community = r.id_community AND o.bucket = r.bucket
       )
    ON CONFLICT DO NOTHING
    RETURNING bucket
    """
)


@dataclass
class _JobRun:
    """Mutable, so a job can report that it never actually ran."""

    outcome: str = "ok"


@contextlib.asynccontextmanager
async def _observed(job: str) -> AsyncIterator[_JobRun]:
    """Count and time one scheduler job.

    ---------------------------------------------------------------------------
    WHY THE INSTRUMENTATION IS HERE AND NOT IN `scheduler_main`.

    `run_maintenance` CALLS `run_partitions` and `run_retention`. Wrapping all
    three at the call sites in the loop would make one nightly event increment
    three series, and would report an exception raised inside retention as two
    failed jobs - the one that failed and the one that contains it. Wrapping the
    four leaf jobs where they are defined counts each event exactly once, and
    `maintenance` is deliberately NOT a value of `job`.

    It also catches what the loop cannot see at all: a run that was SKIPPED
    because another replica held the advisory lock. That path returns normally
    with an empty result, so from `scheduler_main` it is indistinguishable from a
    run that had nothing to do - which is exactly the case where a second replica
    silently does no work for ever.
    ---------------------------------------------------------------------------

    Re-raises. The caller's error handling is unchanged; this only observes.
    """
    started = time.perf_counter()
    run = _JobRun()
    try:
        yield run
    except BaseException:
        # Set on the record, not just on the counter, so the `finally` below
        # labels the duration with the same outcome.
        run.outcome = "failed"
        app_metrics.scheduler_job_runs.add(1, {"job": job, "outcome": "failed"})
        raise
    else:
        app_metrics.scheduler_job_runs.add(1, {"job": job, "outcome": run.outcome})
    finally:
        # Recorded on EVERY path, including the failed one: a job that fails
        # after ten minutes and one that fails in a millisecond are different
        # incidents, and the failure counter alone cannot tell them apart.
        #
        # LABELLED BY OUTCOME TOO, because a run that skipped on a held lock
        # returns in microseconds. Folding those into the same distribution as
        # the work drags every percentile toward zero in exactly the deployment
        # that has a second replica - which is the one where the duration matters.
        app_metrics.scheduler_job_duration.record(
            time.perf_counter() - started, {"job": job, "outcome": run.outcome}
        )


def next_tick(
    now: datetime.datetime,
    *,
    minutes: int | None = None,
    offset_seconds: int | None = None,
) -> datetime.datetime:
    """The next instant on the wall-clock grid, `offset_seconds` into its slot.

    The offset exists because a device publishing at :00 has to arrive, validate
    and commit before the tick that closes its interval reads the table. Ticking
    exactly on the grid races the whole fleet at once, and the loser's readings
    simply land in the next tick - which is harmless for the current hour and
    wrong-looking for exactly 15 minutes, every 15 minutes.
    """
    minutes = minutes if minutes is not None else settings.ROLLUP_TICK_MINUTES
    offset = offset_seconds if offset_seconds is not None else settings.ROLLUP_TICK_OFFSET_SECONDS
    base = now.replace(second=0, microsecond=0)
    candidate = base.replace(minute=(base.minute // minutes) * minutes) + datetime.timedelta(
        seconds=offset
    )
    while candidate <= now:
        candidate += datetime.timedelta(minutes=minutes)
    return candidate


async def backfill_dirty(session: AsyncSession, *, now: datetime.datetime) -> int:
    """Mark historical buckets that have never been rolled up. Returns the count.

    Takes a session and does NOT commit - the house pattern here, and the reason
    is testability rather than taste: a function that opens its own session
    commits outside the suite's rolled-back transaction and leaves rows behind
    for every later test.
    """
    floor = retention.raw_floor(now)
    result = await session.execute(_BACKFILL_SQL, {"floor": floor})
    marked = len(result.all())
    operations = await session.execute(_BACKFILL_OPERATIONS_SQL, {"floor": floor})
    marked += len(operations.all())
    if marked:
        logger.info("backfill marked %d historical bucket(s) for recompute", marked)
    return marked


def rollup_targets(
    with_devices: Sequence[int],
    *,
    active: AbstractSet[int] | None,
    pending: AbstractSet[int],
) -> tuple[list[int], int]:
    """The communities this tick recomputes, and how many it skipped.

    `None` is "the set is unknown" and returns everyone: see the module
    docstring for why rollups fail open. Otherwise a community is ticked while
    it is active OR still has pending work, which is what drains a switched-off
    community before it is skipped.
    """
    if active is None:
        return list(with_devices), 0
    ids = [c for c in with_devices if c in active or c in pending]
    return ids, len(with_devices) - len(ids)


async def run_rollups(
    sessions: async_sessionmaker,
    *,
    now: datetime.datetime,
    active: frozenset[int] | None,
    engine=None,
) -> int:
    """The 15-minute tick. One transaction PER COMMUNITY.

    Per community because a community whose data trips a constraint must not
    abort the other forty - and because the claim from `rollup_dirty` has to
    commit with the recompute it paid for, which it can only do inside the same
    transaction.

    `active` is required and has no default: `None` is a decision (tick everyone)
    and a caller has to make it out loud.
    """
    async with (
        _observed("rollups") as run,
        advisory_lock(ADVISORY_LOCK_ROLLUPS, engine=engine, name="rollups") as acquired,
    ):
        if not acquired:
            run.outcome = "lock_held"
            return 0
        pending: frozenset[int] = frozenset()
        async with sessions() as reader:
            with_devices = list((await reader.execute(_COMMUNITIES_SQL)).scalars().all())
            if active is not None:
                lo, _hi = buckets.window(now)
                pending = frozenset(
                    (await reader.execute(_PENDING_SQL, {"lo": lo})).scalars().all()
                )

        ids, skipped = rollup_targets(with_devices, active=active, pending=pending)
        if skipped:
            logger.info(
                "rollup tick: %d community/communities skipped - live-data subscription "
                "inactive and nothing left to drain",
                skipped,
            )

        done = 0
        for id_community in ids:
            try:
                async with sessions() as session:
                    await rollups.tick_community(session, id_community=id_community, now=now)
                    await session.commit()
                done += 1
                app_metrics.rollup_communities.add(1, {"outcome": "ok"})
            except Exception:
                # Logged and skipped, never re-raised: one community's failure
                # must not stop the tick for the rest, and the next tick retries
                # it anyway because the window is recomputed unconditionally.
                #
                # Which is right, and is why the counter matters: the run as a
                # whole then reports SUCCESS. One community frozen among forty
                # has no aggregate signal at all without this line.
                app_metrics.rollup_communities.add(1, {"outcome": "failed"})
                logger.exception("rollup tick failed for community %s", id_community)

        await _refresh_rollup_lag(sessions, only=active)
        return done


# TWO QUESTIONS, TWO SERIES, because one number cannot answer both.
#
# `scope="data"` is the age of the newest ROLLED-UP HOUR for the community
# furthest behind. `scope="tick"` is the age of the most recent RECOMPUTE,
# fleet-wide, read from `computed_at`, which every tick rewrites across its whole
# 48-hour window.
#
# They were one series and it was the wrong one. A `rollup_community_hour` row
# exists for bucket B only if measurements landed in it, so a community whose
# only meter goes quiet - an unactivated P1 port, which the runbook calls the
# commonest real incident - freezes its own MAX(bucket) and pins the gauge
# upward while the scheduler is perfectly healthy. On a forty-community platform
# that is permanent. `computed_at` moves whenever the tick runs at all, so the
# pair separates "the data is stale" from "the job stopped".
#
# The lag of the community that is FURTHEST BEHIND, not the fleet's newest
# bucket.
#
# `MAX(bucket)` over the whole table is what /ops/health reports, per community,
# to a manager of that community. Fleet-wide it is actively misleading: thirty-
# nine healthy communities hold the number at a few minutes while the fortieth is
# frozen, which is the failure `docs/runbooks/live-data.md` describes and the one
# nobody is watching for. Grouping first and taking the worst inverts that.
#
# Communities with no rollup row at all are absent rather than infinite: a
# community created this morning has not fallen behind, and reporting it as
# maximally stale would make every new community an alert.
#
# ACTIVE communities only, when the set is known (D-12). A switched-off
# community's newest bucket freezes by design, so left in it would pin
# `scope="data"` upward for ever and mask a real stall in an active one.
_WORST_LAG_SQL = text(
    """
    SELECT EXTRACT(EPOCH FROM MIN(newest))    AS oldest_newest_epoch,
           EXTRACT(EPOCH FROM MAX(computed))  AS newest_computed_epoch
      FROM (
            SELECT id_community,
                   MAX(bucket)      AS newest,
                   MAX(computed_at) AS computed
              FROM rollup_community_hour
             WHERE CAST(:unfiltered AS boolean) OR id_community = ANY(:ids)
             GROUP BY id_community
           ) s
    """
)


async def _refresh_rollup_lag(sessions: async_sessionmaker, *, only: frozenset[int] | None) -> None:
    """Publish both freshness instants: the DATA's and the TICK's.

    The INSTANT, not the lag. `core/metrics._rollup_lag_callback` subtracts it
    from the clock on every collection cycle, so the gauge ages by itself - which
    is the whole point, because this function is the gauge's only writer and the
    failure it exists to show is this function no longer running. Publishing a
    lag would freeze the number at its last healthy value exactly then.

    A dict rather than a query in the callback: an observable gauge's callback is
    SYNCHRONOUS and runs on the exporter's own thread, so it cannot await a
    database round trip.

    Failure here must not fail the tick - the rollups themselves are already
    committed by the time this runs, and losing one observation is not worth
    losing them. The gauge keeps ageing through the failure, which is the correct
    reading: nothing has confirmed freshness since the last successful refresh.

    `only` is the active set, or None for every community. An EMPTY set clears
    the snapshot: with no active community there is nothing whose freshness can
    be judged, and a frozen value would climb as though the scheduler had died.
    """
    if only is not None and not only:
        app_metrics.rollup_newest_bucket_epoch.clear()
        return
    params = {"unfiltered": only is None, "ids": sorted(only or ())}
    try:
        async with sessions() as session:
            row = (await session.execute(_WORST_LAG_SQL, params)).one_or_none()
    except Exception:
        logger.warning("could not refresh the rollup lag gauge", exc_info=True)
        return
    if row is None or row.oldest_newest_epoch is None:
        # No community has a rollup yet. Leave the snapshot untouched rather than
        # writing "now", which would read as perfectly fresh.
        return
    app_metrics.rollup_newest_bucket_epoch["data"] = float(row.oldest_newest_epoch)
    if row.newest_computed_epoch is not None:
        app_metrics.rollup_newest_bucket_epoch["tick"] = float(row.newest_computed_epoch)


async def run_ownership(
    local_sessions: async_sessionmaker,
    crm_sessions: async_sessionmaker,
    *,
    now: datetime.datetime,
    active: frozenset[int] | None,
    engine=None,
) -> int:
    """Refresh the ownership projection. Its own lock: it crosses a database
    boundary and can be slow, and sharing the rollup lock would let it delay
    every tick behind it.

    ACTIVE communities only (D-12). Unlike the rollups this does NOT fail open:
    an unknown set (`None`) raises inside `_observed`, so it is counted `failed`
    and the loop retries next tick. The subscription read and this job's own
    CRM read go to the same database, so the first failing says the second is
    about to. A switched-off community's windows are left as they were, and
    `changed_date_span` repairs them on the first refresh after it is back on.
    """
    async with _observed("ownership") as run:
        if active is None:
            raise SubscriptionsUnavailable(
                "the ownership refresh runs for subscribed communities only, and the "
                "live-data subscription set has not loaded"
            )
        async with advisory_lock(
            ADVISORY_LOCK_OWNERSHIP, engine=engine, name="ownership"
        ) as acquired:
            if not acquired:
                run.outcome = "lock_held"
                return 0
            async with local_sessions() as local, crm_sessions() as crm:
                result = await refresh_ownership(
                    local, SqlAlchemyCrmCoreRead(crm), now=now, communities=active
                )
                await local.commit()
            # The FULL SIZE of the recomputed projection, not a delta - the
            # refresh deletes a community's windows and rewrites them. So this is
            # a steady positive number whenever any active community's device has
            # an owner, and ZERO IS THE ALARM: a CRM read that legitimately comes
            # back empty (a revoked grant on live_data_svc, `meter.id_community`
            # nulled) wipes the projection and then reports a successful run.
            app_metrics.ownership_windows_written.add(result.windows_written)
            return result.windows_written


async def run_partitions(
    sessions: async_sessionmaker, *, now: datetime.datetime, engine=None
) -> list[str]:
    async with (
        _observed("partitions") as run,
        advisory_lock(ADVISORY_LOCK_PARTITIONS, engine=engine, name="partitions") as acquired,
    ):
        if not acquired:
            run.outcome = "lock_held"
            return []
        async with sessions() as session:
            created, drained = await partitions.ensure_partitions(session, now=now)
            await session.commit()
        app_metrics.partitions_created.add(len(created))
        # Should stay at zero for ever. Above zero means rows had ALREADY landed
        # in a DEFAULT partition and had to be moved before the real one could be
        # attached - the create-ahead job had fallen behind and this run repaired
        # it. That repair is otherwise invisible: nothing fails at write time, and
        # by the next morning the default is empty again.
        app_metrics.partition_default_rows_drained.add(drained)
        async with sessions() as session:
            counts = await partitions.default_partition_counts(session)
        for name, rows in counts.items():
            if rows:
                logger.warning("%s still holds %d row(s) after the drain - investigate", name, rows)
        return created


@dataclass(frozen=True, slots=True)
class RetentionResult:
    partitions_dropped: list[str]
    dead_letters_pruned: int


async def run_retention(
    sessions: async_sessionmaker, *, now: datetime.datetime, engine: AsyncEngine | None = None
) -> RetentionResult:
    """Both retention passes, under ONE lock and one job label.

    The dead-letter prune shares the retention lock rather than taking a key of
    its own because it is the same job: a second replica must skip both, and
    `job` stays the advisory-lock set (core/metrics.py) - a separate label would
    count one nightly event twice.

    Partitions first, and COUNTED before the prune starts. They are the job this
    was, and a prune that raises must not leave partitions that were dropped
    uncounted.
    """
    async with (
        _observed("retention") as run,
        advisory_lock(ADVISORY_LOCK_RETENTION, engine=engine, name="retention") as acquired,
    ):
        if not acquired:
            run.outcome = "lock_held"
            return RetentionResult(partitions_dropped=[], dead_letters_pruned=0)
        dropped = await retention.apply_retention(sessions, now=now)
        app_metrics.partitions_dropped.add(len(dropped))
        pruned = await retention.prune_dead_letters(sessions, now=now)
        app_metrics.dead_letters_pruned.add(pruned)
        return RetentionResult(partitions_dropped=dropped, dead_letters_pruned=pruned)


@dataclass(frozen=True, slots=True)
class MaintenanceResult:
    partitions_created: list[str]
    partitions_dropped: list[str]
    dead_letters_pruned: int


async def run_maintenance(
    sessions: async_sessionmaker, *, now: datetime.datetime, engine: AsyncEngine | None = None
) -> MaintenanceResult:
    """The nightly pair, and IT ALWAYS SAYS IT RAN.

    ---------------------------------------------------------------------------
    THE LOG LINE IS THE POINT OF THIS FUNCTION.

    `run_partitions` logs only when it creates something and `run_retention`
    only when it drops something, so a healthy night - which is every night,
    because partitions are created six months ahead - produced NO OUTPUT AT ALL.

    Found by reading 40 hours of real scheduler logs: the rollup tick is visible
    every 15 minutes and the ownership refresh every hour, and between them there
    was no way to tell "maintenance ran and had nothing to do" from "maintenance
    has never run". `docs/runbooks/live-data.md` sends an operator to these logs
    for exactly that question, so the runbook was asking for something that was
    not there.

    Which matters more than tidiness: the create-ahead job failing silently is
    the failure that becomes a platform-wide outage three months later, at 00:00
    UTC on the first of a month. A nightly line saying it ran is the cheapest
    possible early warning, and its ABSENCE is now itself the signal.
    ---------------------------------------------------------------------------
    """
    created = await run_partitions(sessions, now=now, engine=engine)
    retained = await run_retention(sessions, now=now, engine=engine)
    logger.info(
        "maintenance: %d partition(s) created, %d dropped, %d dead letter(s) pruned",
        len(created),
        len(retained.partitions_dropped),
        retained.dead_letters_pruned,
    )
    return MaintenanceResult(
        partitions_created=created,
        partitions_dropped=retained.partitions_dropped,
        dead_letters_pruned=retained.dead_letters_pruned,
    )


def maintenance_is_due(now: datetime.datetime, last_run: datetime.datetime | None) -> bool:
    """Daily, at `MAINTENANCE_HOUR_UTC`, and at most once a day.

    Compared on the UTC DATE rather than on elapsed hours: "more than 24 h ago"
    drifts forward by the tick interval every day until the job eventually runs
    at a different hour than the one chosen.
    """
    if now.hour < settings.MAINTENANCE_HOUR_UTC:
        return False
    return last_run is None or last_run.date() < now.date()


# Every (community, EAN) that has ever had a device, revoked ones included - the
# set `worker/ownership.py` projects, for the reason its docstring gives.
_DEVICE_EANS_SQL = text("SELECT DISTINCT id_community, ean FROM device")

DeviceEans = frozenset[tuple[int, str]]


async def device_eans(sessions: async_sessionmaker) -> DeviceEans:
    """The (community, EAN) pairs the ownership projection is responsible for."""
    async with sessions() as session:
        rows = (await session.execute(_DEVICE_EANS_SQL)).all()
    return frozenset((int(row.id_community), str(row.ean)) for row in rows)


def ownership_is_due(
    now: datetime.datetime,
    last_run: datetime.datetime | None,
    *,
    known: DeviceEans,
    current: DeviceEans | None,
) -> bool:
    """Hourly - and on the first tick after a device appears on a new EAN.

    ---------------------------------------------------------------------------
    WHY A NEW DEVICE CANNOT WAIT FOR THE HOUR.

    A device's buckets count toward `n_members` only once its EAN has a window
    in `device_owner_window`, and until then every grid term of every bucket it
    is in is withheld below k. On the hourly cadence alone that took up to an
    hour and a quarter after `POST /devices`: found 2026-10-04, when three
    prosumers created just after a refresh read `n_members = 0` and the
    dashboard showed nothing at all. Ownership itself still changes a few times
    a year; what changes the moment a manager adds a device is the SET OF EANs
    the projection has to cover.

    "New since the last refresh", and NOT "an EAN with no window". An EAN whose
    meter has no active CRM `meter_data` never gets a window, so that test would
    be true on every tick for ever - a CRM read every 15 minutes because one
    meter was deactivated. `known` is the set as it stood when the last refresh
    STARTED (`scheduler_main` keeps it), so a device created during a refresh is
    still new on the next tick.

    `current is None` means the set could not be read: the hourly rule alone
    decides, as it did before.
    ---------------------------------------------------------------------------
    """
    if last_run is None:
        return True
    if (now - last_run).total_seconds() >= settings.OWNERSHIP_REFRESH_MINUTES * 60:
        return True
    return current is not None and bool(current - known)
