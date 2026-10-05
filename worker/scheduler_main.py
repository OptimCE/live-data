"""The scheduler entrypoint: `python -m worker.scheduler_main`.

A THIRD CONTAINER, sharing `Dockerfile.worker`'s image with the ingest worker and
sharing nothing else. `worker/scheduler.py` explains why they cannot be the same
process; the two differences that live here are:

  - THE HEARTBEAT IS UNGATED. `worker/main.py` touches its file only while the
    broker connection is up, because an ingest worker without a broker is doing
    nothing. This process has no broker and never will: it touches
    `/tmp/scheduler.alive` on a timer, and the only thing that stops it is the
    process dying.

  - THE HEALTHCHECK MUST BE OVERRIDDEN IN COMPOSE. The image bakes one that reads
    `/tmp/worker.alive`, which this process never writes. Without the override
    the container is unhealthy from the first probe onwards, for ever, while
    doing its job perfectly.

No MQTT credentials are read here, and none are set on the container.
"""

import asyncio
import contextlib
import datetime
import logging
import pathlib
import signal
import sys
from dataclasses import dataclass, field

from core.config import settings
from core.database.database import crm_engine, local_engine
from core.logging import configure_logging
from core.tracing import setup_tracer_provider, shutdown_telemetry
from worker import scheduler
from worker.context import crm_sessionmaker, local_sessionmaker
from worker.subscriptions import (
    SubscriptionCache,
    SubscriptionsUnavailable,
    crm_subscription_loader,
)

logger = logging.getLogger(__name__)

_HEARTBEAT_INTERVAL_SECONDS = 15
_HEARTBEAT_PATH = pathlib.Path("/tmp/scheduler.alive")  # noqa: S108


async def _heartbeat(shutdown: asyncio.Event) -> None:
    """Touch the liveness file unconditionally.

    Deliberately NOT gated on anything. There is no dependency whose loss should
    restart this process: a database outage makes every job fail and log, and the
    next tick retries - whereas a restart loop during an outage would mean the
    scheduler is down at the moment the database comes back.
    """
    while not shutdown.is_set():
        _HEARTBEAT_PATH.touch()
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(shutdown.wait(), timeout=_HEARTBEAT_INTERVAL_SECONDS)


async def _active_communities(subscriptions: SubscriptionCache) -> frozenset[int] | None:
    """The live-data subscription set for this tick, or None if it never loaded.

    None is an answer both jobs understand (see worker/scheduler.py): the rollups
    tick everyone, the ownership refresh refuses. Unlike the ingest worker, the
    scheduler does not wait for the CRM - a tick missed for want of a set would
    freeze every community's rollups, which is worse than recomputing a few that
    were switched off.
    """
    try:
        return await subscriptions.get()
    except SubscriptionsUnavailable:
        logger.warning(
            "scheduler:subscriptions-unavailable - the live-data subscription set cannot "
            "be read from the CRM; rollups tick every community, ownership waits",
            exc_info=True,
            extra={"operation": "scheduler:subscriptions-unavailable"},
        )
        return None


async def _current_eans(local_sessions) -> scheduler.DeviceEans | None:
    """The device EAN set for this tick, or None if it could not be read.

    None makes `ownership_is_due` fall back to its hourly rule - and never stops
    the rollup tick, which does not need the set at all.
    """
    try:
        return await scheduler.device_eans(local_sessions)
    except Exception:
        logger.warning(
            "scheduler:device-eans-unavailable - the device EAN set cannot be read; "
            "the ownership refresh falls back to its hourly cadence",
            exc_info=True,
            extra={"operation": "scheduler:device-eans-unavailable"},
        )
        return None


@dataclass
class _LoopState:
    """What one wake of the loop leaves for the next."""

    last_ownership: datetime.datetime | None = None
    # The device EANs as they stood when the last successful ownership refresh
    # STARTED - see `scheduler.ownership_is_due`.
    known_eans: scheduler.DeviceEans = field(default_factory=frozenset)
    last_maintenance: datetime.datetime | None = None


async def _run_once(
    local_sessions,
    crm_sessions,
    subscriptions: SubscriptionCache,
    state: _LoopState,
    *,
    now: datetime.datetime,
) -> None:
    """One wake of the loop: ownership when due, the rollup tick, maintenance.

    OWNERSHIP RUNS BEFORE THE ROLLUPS. The tick recomputes the 48-hour window
    unconditionally and reads `n_members` from `device_owner_window`, so a
    refresh that lands after it leaves a new device's buckets withheld until the
    NEXT tick - fifteen more minutes of a dashboard showing nothing, on top of
    the hour `ownership_is_due` no longer waits. The refresh's dirty marks below
    the window are drained by the same tick too. It costs the tick a CRM read on
    the wakes that refresh: once an hour, or after a new device.
    """
    active = await _active_communities(subscriptions)

    current = await _current_eans(local_sessions)
    if scheduler.ownership_is_due(
        now, state.last_ownership, known=state.known_eans, current=current
    ):
        try:
            await scheduler.run_ownership(local_sessions, crm_sessions, now=now, active=active)
            state.last_ownership = now
            if current is not None:
                state.known_eans = current
        except Exception:
            logger.exception("ownership refresh failed")

    try:
        done = await scheduler.run_rollups(local_sessions, now=now, active=active)
        logger.info("rollup tick: %d community/communities recomputed", done)
    except Exception:
        logger.exception("rollup tick failed")

    if scheduler.maintenance_is_due(now, state.last_maintenance):
        try:
            # One call, and it logs unconditionally - see its docstring. A
            # nightly line saying maintenance ran is what makes its ABSENCE
            # a signal.
            await scheduler.run_maintenance(local_sessions, now=now)
            state.last_maintenance = now
        except Exception:
            logger.exception("maintenance failed")


async def _run(shutdown: asyncio.Event) -> None:
    local_sessions = local_sessionmaker()
    crm_sessions = crm_sessionmaker()
    # The scheduler's own cache. It reads the set once per 15-minute tick, so at
    # any TTL under that every tick reads it fresh; the dev compose's 5 s is for
    # the ingest worker and is not set here.
    subscriptions = SubscriptionCache(
        crm_subscription_loader(crm_sessions),
        ttl_seconds=settings.SUBSCRIPTION_CACHE_TTL_SECONDS,
    )

    now = datetime.datetime.now(datetime.UTC)

    # Once, at startup, before the first tick. A deployment onto a database that
    # already holds telemetry - which is every deployment after the first - would
    # otherwise publish the last 48 hours and nothing else, and the gap would
    # never fill because closed buckets are not revisited.
    try:
        async with local_sessions() as session:
            await scheduler.backfill_dirty(session, now=now)
            await session.commit()
    except Exception:
        logger.exception("backfill failed at startup - the tick will still run")

    # Partitions before the first tick too: the tick writes into
    # `rollup_device_hour`, and on a database provisioned months ago the current
    # month's partition may not exist yet.
    with contextlib.suppress(Exception):
        await scheduler.run_partitions(local_sessions, now=now)

    state = _LoopState()

    while not shutdown.is_set():
        wake = scheduler.next_tick(datetime.datetime.now(datetime.UTC))
        delay = (wake - datetime.datetime.now(datetime.UTC)).total_seconds()
        if delay > 0:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(shutdown.wait(), timeout=delay)
        if shutdown.is_set():
            break

        await _run_once(
            local_sessions,
            crm_sessions,
            subscriptions,
            state,
            now=datetime.datetime.now(datetime.UTC),
        )


def _install_signal_handlers(shutdown: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, shutdown.set)


async def main() -> None:
    configure_logging()
    # FIRST, and it was missing entirely: this container installed no meter
    # provider and no OTLP log handler, so in staging and production the
    # scheduler exported NOTHING - not under the wrong name, under no name. Its
    # instruments would have rebound to a proxy that discards everything, and
    # every chart would have read zero while the jobs ran perfectly.
    #
    # Before the heartbeat and before the first job, because an instrument that
    # records before the provider exists is not replayed when it arrives.
    setup_tracer_provider("scheduler")
    shutdown = asyncio.Event()
    _install_signal_handlers(shutdown)
    heartbeat = asyncio.create_task(_heartbeat(shutdown))
    logger.info(
        "scheduler starting: tick every %d min at +%d s, maintenance at %02d:00 UTC",
        settings.ROLLUP_TICK_MINUTES,
        settings.ROLLUP_TICK_OFFSET_SECONDS,
        settings.MAINTENANCE_HOUR_UTC,
    )
    try:
        await _run(shutdown)
    finally:
        shutdown.set()
        heartbeat.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat
        await local_engine.dispose()
        await crm_engine.dispose()
        logger.info("scheduler stopped")
        # Flushes the log provider AND the metric provider, each bounded to 5 s
        # so a slow collector cannot hold the container past Docker's 10-second
        # stop grace - see shutdown_telemetry.
        #
        # The log half is what matters here. `run_maintenance`'s unconditional
        # "maintenance: N created, M dropped" line is what the runbook sends an
        # operator to look for, precisely because its absence is the alarm - and
        # maintenance runs ONCE A DAY, so a container stopped while that line is
        # still in a batch loses the only record that it ran.
        shutdown_telemetry()


if __name__ == "__main__":
    if sys.platform == "win32":
        # Same reason as worker/main.py: paho's asyncio integration needs the
        # selector loop. Confined to __main__ so importing this module never
        # mutates a caller's event-loop policy.
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
