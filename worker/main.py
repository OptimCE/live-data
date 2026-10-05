"""The ingest worker. `python -m worker.main`.

ONE REPLICA, and the comment is the enforcement. Two workers sharing a fixed MQTT
client id kick each other off the broker continuously, and both keep reporting
healthy - the broker evicts the older connection on every connect, for ever. The
client id must stay a literal (see core/config.py): derived from hostname, pid or
uuid, every container recreate would start a BRAND NEW session and the entire
queued backlog would be silently gone, with `session_present=False` that nobody
is reading.

NO NATS. Nothing publishes and nothing consumes (plan 4), so there is no bounded
connect-retry against a second broker and no JetStream subscription. What
replaces it is below: a reconnect loop around a single MQTT session.

----------------------------------------------------------------------------
THE HEARTBEAT IS GATED ON THE BROKER CONNECTION, AND THAT IS THE POINT.

`Dockerfile.worker`'s HEALTHCHECK fails when /tmp/worker.alive is more than 60 s
stale. Touching it unconditionally - which is what the sibling services do,
because their liveness genuinely is "the event loop runs" - would report a worker
that cannot reach the broker as healthy for ever. It ingests nothing; it should
be restarted.

This is the exact OPPOSITE of the decision in api/health/routes.py, where
readiness deliberately does NOT gate on the broker: compose's healthcheck reads
that endpoint, and a broker restart must not roll the API - and with it the whole
admin surface, which needs no broker - out of service. Both halves are commented
in both places, because each looks like a mistake from the other's side.
----------------------------------------------------------------------------

AND WHY A FAILING DATABASE DISCONNECTS US FROM THE BROKER ON PURPOSE: see the
module docstring of worker/ingest.py. In short, aiomqtt 2.x PUBACKs before the
write, so the in-memory deque is a lossy buffer with no bound, while the broker's
persistent session is durable and bounded. When the database keeps failing, the
right move is to stop taking delivery.

AND WHY A CRM THAT NEVER ANSWERED KEEPS US OFF THE BROKER ENTIRELY (D-12): the
worker discards the telemetry of communities whose live-data subscription is not
active (see worker/ingest.py, step 4), so it cannot judge a single message until
it has read that set once. It reads it BEFORE dialling, and until the first read
succeeds it does not connect at all - the broker's persistent session holds the
backlog meanwhile, exactly as it does for a database outage. `_connected` is never
set, so the heartbeat stops and the container goes unhealthy: a worker that
cannot decide what to ingest ingests nothing. Once a set has loaded, a failing
CRM costs nothing but freshness - see worker/subscriptions.py.
"""

import asyncio
import contextlib
import datetime
import logging
import pathlib
import signal
import sys
import time

import aiomqtt

from core import metrics as app_metrics
from core.config import settings
from core.database.database import (
    AsyncSessionCRMFactory,
    AsyncSessionLocalFactory,
    crm_engine,
    local_engine,
)
from core.logging import configure_logging
from core.tracing import setup_tracer_provider, shutdown_telemetry
from domain.topics import parse_device_topic
from domain.validation import ValidationSettings
from shared.const import TOPIC_STATUS_WILDCARD, TOPIC_TELEMETRY_WILDCARD
from shared.mqtt_tls import tls_params
from worker.ingest import handle_message
from worker.subscriptions import (
    SubscriptionCache,
    SubscriptionsUnavailable,
    crm_subscription_loader,
)

configure_logging()
logger = logging.getLogger(__name__)

_HEARTBEAT_INTERVAL_SECONDS = 15
_HEARTBEAT_PATH = pathlib.Path("/tmp/worker.alive")  # noqa: S108
_RECONNECT_BASE_DELAY_SECONDS = 2
_RECONNECT_MAX_DELAY_SECONDS = 30

# Set while an MQTT session is up. The heartbeat waits on it.
_connected = asyncio.Event()


def _validation_settings() -> ValidationSettings:
    return ValidationSettings(
        max_future_seconds=settings.INGEST_MAX_FUTURE_SECONDS,
        max_age_days=settings.INGEST_MAX_AGE_DAYS,
        interval_seconds=settings.INGEST_INTERVAL_SECONDS,
        max_batch=settings.INGEST_MAX_BATCH,
        capacity_tolerance=settings.INGEST_CAPACITY_TOLERANCE,
        night_start_hour_local=settings.INGEST_NIGHT_START_HOUR_LOCAL,
        night_end_hour_local=settings.INGEST_NIGHT_END_HOUR_LOCAL,
        timezone=settings.INGEST_TIMEZONE,
        default_max_wh_per_interval=settings.INGEST_DEFAULT_MAX_WH_PER_INTERVAL,
    )


async def _heartbeat(shutdown: asyncio.Event) -> None:
    """Touch the liveness file, but ONLY while the broker connection is up.

    `_connected` is cleared on every disconnect, so a worker that cannot reach
    the broker stops touching the file and the image's HEALTHCHECK fails it
    within ~60 s.
    """
    while not shutdown.is_set():
        if _connected.is_set():
            _HEARTBEAT_PATH.touch()
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(shutdown.wait(), timeout=_HEARTBEAT_INTERVAL_SECONDS)


def _client() -> aiomqtt.Client:
    """A FRESH client per connection attempt.

    An `aiomqtt.Client` is not reliably reusable after a connection timeout, so
    the reconnect loop constructs a new one each time rather than retrying the
    same object.

    `identifier`, not `client_id`: aiomqtt 2.x renamed it. `clean_session`, not
    `clean_start`: the former is the MQTT 3.1.1 knob and the latter is MQTT 5,
    which protocol 1 forbids for devices and which we therefore do not speak
    either.

    `clean_session=False` with a FIXED identifier is what makes the broker hold a
    session for us across a restart - which, with `persistence true` and a raised
    `max_queued_messages`, is the only durable buffer in the whole design.
    """
    return aiomqtt.Client(
        hostname=settings.MQTT_HOST,
        port=settings.MQTT_PORT,
        # The INGEST identity, not the admin one: the admin can drive dynsec and
        # can receive nothing on ce/#. See core/config.py.
        username=settings.MQTT_INGEST_USERNAME or None,
        password=settings.MQTT_INGEST_PASSWORD or None,
        identifier=settings.MQTT_INGEST_CLIENT_ID,
        tls_params=tls_params(),
        protocol=aiomqtt.ProtocolVersion.V311,
        clean_session=False,
        keepalive=60,
    )


async def _consume(shutdown: asyncio.Event, subscriptions: SubscriptionCache) -> None:
    """One MQTT session: subscribe, then process until the connection ends."""
    cfg = _validation_settings()
    consecutive_db_failures = 0

    # BEFORE dialling, and it is the only call here allowed to raise. Cold, a
    # CRM that does not answer raises SubscriptionsUnavailable and `_run` backs
    # off without ever connecting - the broker's persistent session holds the
    # backlog until this worker can judge it. Warm, it never raises.
    await subscriptions.get()

    async with _client() as client:
        # BOTH topics in the same call, so they cannot drift apart. The status
        # topic is half the ingest surface: without it `online` is never true,
        # `connector`/`version` are never refreshed, and revocation's retained
        # clear has nothing to clear.
        #
        # qos=1 on telemetry: subscribing at QoS 0 DOWNGRADES delivery no matter
        # what the publisher used.
        await client.subscribe(TOPIC_TELEMETRY_WILDCARD, qos=1)
        await client.subscribe(TOPIC_STATUS_WILDCARD, qos=0)
        _connected.set()
        logger.info(
            "ingest subscribed",
            extra={
                "operation": "ingest:subscribe",
                "client_id": settings.MQTT_INGEST_CLIENT_ID,
            },
        )

        async for message in client.messages:
            topic = str(message.topic)
            if shutdown.is_set():
                # THIS MESSAGE IS ALREADY GONE. paho PUBACKed it before the
                # handler saw it, so breaking here loses it: not in the database,
                # not in the dead-letter table, not in the broker's session. One
                # per SIGTERM, on every deploy, and until now on no instrument at
                # all - `ingest.messages.total` simply never got a point.
                app_metrics.ingest_messages.add(
                    1, {"kind": _topic_kind(topic), "outcome": "dropped"}
                )
                break
            payload = message.payload if isinstance(message.payload, bytes) else b""

            kind = _topic_kind(topic)
            started = time.perf_counter()
            # OUTSIDE the `try`, like `_topic_kind`, and total for the same
            # reason: the set is warm by now, and a warm `get()` never raises.
            # Inside, a refresh - its CRM read, its diff logging, its counter -
            # would be one more thing the handler below miscounts as `db_error`.
            active = await subscriptions.get()

            # try/except/ELSE, and the `else` is load-bearing.
            #
            # `_log_outcome` used to sit at the end of the `try`. Putting the
            # metric emission beside it there would make ANY error from the
            # instrumentation - a bad attribute type, an unexpected None on a new
            # IngestOutcome field - indistinguishable from a database failure:
            # caught below, logged as `ingest:error` with a stack trace, and
            # counted against `consecutive_db_failures`, while the outcome it was
            # meant to record is lost. An operator would be reading "ingest
            # failed" per message against a database that is perfectly healthy.
            #
            # It would NOT trip the backpressure disconnect today, because
            # `consecutive_db_failures = 0` runs before the emission and the
            # count would oscillate 0 -> 1 for ever. That is an accident of
            # statement order, not a safeguard: move the reset one line down - a
            # plausible tidy-up - and five bad `.add()` calls disconnect the
            # worker from the broker to shed an outage that is not happening.
            #
            # The `else` block runs only when the transaction succeeded, and its
            # own exceptions are NOT caught here.
            try:
                async with AsyncSessionLocalFactory() as session:
                    outcome = await handle_message(
                        session,
                        topic,
                        payload,
                        datetime.datetime.now(datetime.UTC),
                        cfg,
                        active_communities=active,
                    )
                    await session.commit()
                consecutive_db_failures = 0
            except Exception:
                # FIRST, before anything that can branch. This message was
                # PUBACKed by paho before application code saw it and its dead
                # letter was in the transaction that just rolled back, so it
                # exists nowhere else - not in the database, not in the broker.
                # A `.add()` with a literal attribute dict cannot raise.
                app_metrics.ingest_messages.add(1, {"kind": kind, "outcome": "db_error"})
                app_metrics.ingest_message_duration.record(
                    time.perf_counter() - started, {"kind": kind}
                )
                consecutive_db_failures += 1
                logger.exception(
                    "ingest failed",
                    extra={
                        "operation": "ingest:error",
                        "topic": topic,
                        "consecutive_failures": consecutive_db_failures,
                    },
                )
                if consecutive_db_failures >= settings.INGEST_DB_FAILURES_BEFORE_DISCONNECT:
                    # DELIBERATE DISCONNECT. See the module docstring: paho has
                    # already PUBACKed everything sitting in aiomqtt's in-memory
                    # deque, so staying connected grows an unbounded volatile
                    # queue until an OOM kill discards it. Dropping the
                    # connection hands the backlog back to the broker, where it
                    # is durable and where max_queued_messages bounds it with a
                    # log line.
                    logger.error(
                        "disconnecting from the broker after repeated database failures "
                        "- the backlog is safer in the broker's persistent session than "
                        "in this process's memory",
                        extra={
                            "operation": "ingest:backpressure",
                            "consecutive_failures": consecutive_db_failures,
                        },
                    )
                    app_metrics.ingest_backpressure_disconnects.add(1)
                    return
            else:
                _log_outcome(topic, outcome)
                _record_outcome(kind, outcome, time.perf_counter() - started)


def _topic_kind(topic: str) -> str:
    """telemetry | status | unparsed - a THREE-VALUE label, from the topic alone.

    Re-parses rather than reading a field off `IngestOutcome`, and that is the
    cheaper mistake of the two available. Carrying `kind` on the outcome would
    mean every early return in `handle_message` - the dead-letter paths for an
    unparseable topic, an unknown device, a revoked device, a community mismatch
    - has to remember to set it, and the one that forgets mislabels a whole class
    of failure with nothing failing. `parse_device_topic` is pure and allocates
    one small dataclass.
    """
    # TOTAL BY CONSTRUCTION, because this runs OUTSIDE the per-message `try`.
    #
    # `handle_message` calls `parse_device_topic` too, but from inside the guard,
    # and its own docstring is built on never raising for a bad message "so that
    # a poison message cannot take the connection down". Calling it out here for
    # a label moved that computation upstream of the guard - so a parser that
    # raises stops the whole MQTT session, for every device, with no counter and
    # no dead letter. The parser is total now; this `except` is the belt to that
    # brace, because the next person to add a check inside it will not be
    # thinking about which side of the `try` they are on.
    try:
        parsed = parse_device_topic(topic)
    except Exception:
        logger.exception(
            "topic parse raised - treating as unparsed",
            extra={"operation": "ingest:topic-parse-error", "topic": topic},
        )
        return "unparsed"
    if parsed is None:
        return "unparsed"
    return parsed.kind


def _record_outcome(kind: str, outcome, seconds: float) -> None:
    """The metrics half of `_log_outcome`, for a message that COMMITTED.

    ---- why `outcome` is a partition and the rejection counter is separate ----
    A single telemetry batch routinely comes back with `stored > 0` AND a
    non-empty `rejected` list - that is the whole point of measurement scope: one
    drifted timestamp loses the timestamp, not the fortnight. So a message-level
    label of "stored or rejected" is not a question with one answer.

    `ingest.messages.total` therefore answers only "what happened to the
    MESSAGE", with mutually exclusive values - `stored`, `rejected`, `empty` and
    `not_subscribed` from here, `db_error` and `dropped` from the loop - and every
    individual rejected reading is counted by `ingest.rejections.total` instead.
    Summing the two is meaningless by construction, which is better than a total
    that is quietly wrong.
    """
    if outcome.not_subscribed:
        # FIRST, and its own value. Left to fall through it would land on
        # `empty` - the label of revocation's retained clear - and a switched-off
        # community's fleet would read as a burst of healthy empty commits.
        disposition = "not_subscribed"
    elif outcome.stored:
        disposition = "stored"
    elif outcome.rejected:
        disposition = "rejected"
    else:
        # A committed message that stored nothing and rejected nothing: the
        # zero-length retained publish that revocation uses to clear a status,
        # and an ordinary status message.
        disposition = "empty"

    app_metrics.ingest_messages.add(1, {"kind": kind, "outcome": disposition})
    app_metrics.ingest_message_duration.record(seconds, {"kind": kind})

    if outcome.stored:
        app_metrics.ingest_measurements_stored.add(outcome.stored)
    for reason, _ in outcome.rejected:
        app_metrics.ingest_rejections.add(1, {"reason": reason.value})
    for code, _ in outcome.observations:
        app_metrics.ingest_observations.add(1, {"code": code.value})

    if outcome.oldest_lateness_s is not None:
        app_metrics.ingest_measurement_lateness.record(outcome.oldest_lateness_s, {"kind": kind})


def _log_outcome(topic: str, outcome) -> None:
    """One line per rejected message, per plan 7.4.

    A reject that is only dropped is indistinguishable from a device that never
    published - and the device itself can never learn either way, because the
    flow is one-way and there is no command topic.
    """
    for reason, detail in outcome.rejected:
        logger.warning(
            "reject reason=%s",
            reason.value,
            extra={"operation": "ingest:reject", "topic": topic, "detail": detail},
        )
    for code, detail in outcome.observations:
        logger.info(
            "observed %s",
            code.value,
            extra={"operation": "ingest:observe", "topic": topic, "detail": detail},
        )


async def _run(shutdown: asyncio.Event, subscriptions: SubscriptionCache) -> None:
    """Reconnect with backoff, for ever, until shutdown."""
    delay = _RECONNECT_BASE_DELAY_SECONDS
    while not shutdown.is_set():
        try:
            await _consume(shutdown, subscriptions)
            delay = _RECONNECT_BASE_DELAY_SECONDS
        except aiomqtt.MqttError as exc:
            logger.warning(
                "broker connection lost: %s",
                exc,
                extra={"operation": "ingest:disconnected", "retry_in_seconds": delay},
            )
        except SubscriptionsUnavailable:
            # Cold start only: see the module docstring. Not a crash, and not
            # logged as one - the broker was never dialled.
            logger.warning(
                "ingest:subscriptions-unavailable - the live-data subscription set "
                "cannot be read from the CRM; not connecting to the broker until it can",
                exc_info=True,
                extra={"operation": "ingest:subscriptions-unavailable", "retry_in_seconds": delay},
            )
        except Exception:
            logger.exception("ingest loop crashed", extra={"operation": "ingest:crash"})
        finally:
            # Cleared on EVERY exit path, so the heartbeat stops immediately.
            _connected.clear()

        if shutdown.is_set():
            return
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(shutdown.wait(), timeout=delay)
        delay = min(delay * 2, _RECONNECT_MAX_DELAY_SECONDS)


def _install_signal_handlers(shutdown: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    if sys.platform == "win32":
        # add_signal_handler is not implemented on the Windows event loops.
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: shutdown.set())
        return
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown.set)


async def main() -> None:
    setup_tracer_provider("ingest-worker")
    shutdown = asyncio.Event()
    _install_signal_handlers(shutdown)
    subscriptions = SubscriptionCache(
        crm_subscription_loader(AsyncSessionCRMFactory),
        ttl_seconds=settings.SUBSCRIPTION_CACHE_TTL_SECONDS,
    )

    heartbeat = asyncio.create_task(_heartbeat(shutdown))
    try:
        await _run(shutdown, subscriptions)
    finally:
        heartbeat.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat
        await local_engine.dispose()
        await crm_engine.dispose()
        logger.info("ingest worker stopped", extra={"operation": "ingest:stopped"})
        # LAST, after the final log line, so that line is in the batch this
        # flushes. The SDK's own atexit handler would flush too - but with a
        # 30-second budget against Docker's 10-second stop grace, so on a slow
        # collector the container is SIGKILLed mid-flush. See shutdown_telemetry.
        shutdown_telemetry()


if __name__ == "__main__":
    if sys.platform == "win32":
        # paho's asyncio integration uses add_reader/add_writer, which the
        # ProactorEventLoop does not implement. Without this every connection
        # dies with a bare "Operation timed out" that reads as a broker fault.
        # Confined to __main__ so importing this module never mutates a caller's
        # loop policy.
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
