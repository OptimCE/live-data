"""Which communities are subscribed to live data, as the worker and scheduler see it.

D-12: switching live data off on the Annex services page stops INGESTION, not
just the API. The API has always been gated (`require_feature`, 403 code 1003),
but the ingest worker and the scheduler never looked: a switched-off community's
devices kept being stored and rolled up for ever. This is what they look at now.

----------------------------------------------------------------------------
LAZY, NOT A BACKGROUND TASK.

The set is refreshed on ACCESS, at most once per `SUBSCRIPTION_CACHE_TTL_SECONDS`.
Freshness only matters at the moment a message is judged, so a task refreshing
on its own timer would add a lifecycle to manage and buy nothing. The cost is one
bounded CRM round trip per TTL, taken inline on one message.

The next refresh is due one TTL after the clock reading taken BEFORE the load,
so a flip at time t is seen by the first message handled after t + TTL. That is
the contract `scripts/verify-live-ingest.sh` in the monorepo waits on. A FAILED
refresh is the exception: it is retried one TTL after the load gave up, because
a hung CRM holds each attempt for the full timeout, and at a TTL no longer than
that timeout (the dev compose runs 5 s) the next attempt would already be due -
every message would wait out another timeout, and nothing would count it.

No lock. Each process has one consumer - the ingest loop, or the scheduler loop -
and it calls `get()` sequentially. A second concurrent caller would only double a
refresh, but add an `asyncio.Lock` if one ever appears.
----------------------------------------------------------------------------

COLD AND WARM FAIL DIFFERENTLY, ON PURPOSE.

  * COLD - no set has ever loaded. A failed load RAISES `SubscriptionsUnavailable`,
    and the worker does not dial the broker until the CRM answers; the broker's
    persistent session holds the backlog meanwhile. Guessing instead would either
    ingest what the owner switched off or discard every reading in the fleet.

  * WARM - `get()` NEVER RAISES. A failed refresh keeps the last set that loaded,
    counts `failed` and logs `subscription:stale`. That is load-bearing: the
    worker calls `get()` OUTSIDE its per-message `try` (see worker/main.py), so
    anything escaping from here would end the MQTT session for the whole fleet.
    The load, the diff, its logging and the store therefore all sit inside ONE
    `except` - the "total by construction" rule `worker.main._topic_kind` follows.
"""

import asyncio
import contextlib
import logging
import time
from collections.abc import Awaitable, Callable

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

# Aliased to `app_metrics` like every other emitter, and not for style:
# `tests/test_metrics.py`'s cardinality scan only sees calls rooted at that name.
from core import metrics as app_metrics
from ports.crm_read import SqlAlchemyCrmRead
from shared.const import FeatureName

logger = logging.getLogger(__name__)

# A hung CRM must not stall ingestion behind one message. Not a tuning knob.
_LOAD_TIMEOUT_SECONDS = 5.0

Loader = Callable[[], Awaitable[frozenset[int]]]


class SubscriptionsUnavailable(RuntimeError):  # noqa: N818  # a state, not a fault; the name the plan and docs use
    """No set has EVER loaded, and loading one just failed."""


class SubscriptionCache:
    """The last subscription set that loaded, refreshed at most once per TTL."""

    def __init__(
        self,
        load: Loader,
        *,
        ttl_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._load = load
        self._ttl = ttl_seconds
        self._clock = clock
        self._active: frozenset[int] | None = None
        self._next_refresh = 0.0

    async def get(self) -> frozenset[int]:
        """The active community ids. Raises only while COLD - see the module."""
        now = self._clock()
        previous = self._active
        if previous is not None and now < self._next_refresh:
            return previous
        # Timed from BEFORE the load: the module's flip contract. A failure
        # re-times it below. While cold it is ignored: every call retries, and
        # the callers' own backoff bounds the rate.
        self._next_refresh = now + self._ttl
        try:
            fresh = await asyncio.wait_for(self._load(), _LOAD_TIMEOUT_SECONDS)
            self._log_change(previous, fresh)
            self._active = fresh
        except Exception as exc:
            # A literal-attribute `.add()` cannot raise.
            app_metrics.subscription_refreshes.add(1, {"outcome": "failed"})
            if previous is None:
                raise SubscriptionsUnavailable(
                    "the live-data subscription set could not be read from the CRM, "
                    "and none has loaded yet"
                ) from exc
            # From when the load GAVE UP, not when it began - see the module. The
            # clock is `time.monotonic` in production, which cannot raise.
            self._next_refresh = self._clock() + self._ttl
            with contextlib.suppress(Exception):
                logger.warning(
                    "subscription:stale - could not refresh the live-data subscription "
                    "set; keeping the last one that loaded (%d active)",
                    len(previous),
                    exc_info=True,
                    extra={"operation": "subscription:stale", "retry_in_seconds": self._ttl},
                )
            return previous
        app_metrics.subscription_refreshes.add(1, {"outcome": "ok"})
        return fresh

    @staticmethod
    def _log_change(previous: frozenset[int] | None, fresh: frozenset[int]) -> None:
        """One line on the first load and on every change; nothing otherwise.

        The operation name leads the MESSAGE as well as sitting in `extra`,
        because the dev formatter prints the message alone and these are the
        lines an operator greps for after flipping a switch.
        """
        if previous is None:
            logger.info(
                "subscription:loaded - %d community/communities subscribed to live data: %s",
                len(fresh),
                sorted(fresh),
                extra={"operation": "subscription:loaded", "active": sorted(fresh)},
            )
            return
        if fresh == previous:
            return
        activated = sorted(fresh - previous)
        deactivated = sorted(previous - fresh)
        # A non-empty set becoming EMPTY is the one change worth a warning. A
        # wrong CRM_DATABASE_URL or a renamed feature key answers exactly that,
        # without an error, and the worker then discards every reading it gets.
        level = logging.WARNING if previous and not fresh else logging.INFO
        logger.log(
            level,
            "subscription:changed - live data switched on for %s, off for %s%s",
            activated,
            deactivated,
            (
                " - EVERY community is now off; if nobody did that, the CRM read is wrong"
                if level == logging.WARNING
                else ""
            ),
            extra={
                "operation": "subscription:changed",
                "activated": activated,
                "deactivated": deactivated,
            },
        )


def crm_subscription_loader(crm_sessions: async_sessionmaker[AsyncSession]) -> Loader:
    """The production loader: one short CRM session per refresh."""

    async def load() -> frozenset[int]:
        async with crm_sessions() as session:
            return await SqlAlchemyCrmRead(session).active_communities_unscoped(
                feature=FeatureName.LIVE_DATA.value
            )

    return load
