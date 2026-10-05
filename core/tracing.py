import atexit
import logging
import os
import uuid

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace import Status, StatusCode

from core.config import Environment, settings
from core.context_vars import current_request_id, current_user_id, current_user_role
from core.logging import RequestIdFilter

logger = logging.getLogger(__name__)

# SECONDS. The OTLP HTTP exporters take `timeout` in seconds - the SDK's own
# DEFAULT_TIMEOUT is 10 and it is used as `deadline_sec = time() + self._timeout`
# - while `MeterProvider.shutdown` and every MetricReader take MILLISECONDS.
# Passing one constant named `_MS` to both, as the sibling template does, gave
# the exporters an 83-MINUTE HTTP timeout: a collector that accepts a connection
# and then hangs would hold the export thread for the rest of the afternoon, and
# every batch behind it with it.
EXPORTER_TIMEOUT_SECONDS = 5
EXPORTER_TIMEOUT_MS = EXPORTER_TIMEOUT_SECONDS * 1000

# Kept so `shutdown_telemetry` can reach it - `metrics.get_meter_provider()`
# returns the API-level object, which has no `shutdown`.
#
# Declared ABOVE the function that rebinds it. Below, it also works, because the
# annotated assignment runs at import and the `global` rebinding happens later -
# but only because nothing calls `setup_tracer_provider` during import. Putting
# it here removes the ordering from the set of things that have to stay true.
_meter_provider: MeterProvider | None = None
_log_provider: LoggerProvider | None = None
# Held so the flush can DETACH it - see `shutdown_telemetry`.
_log_handler: logging.Handler | None = None


def setup_tracer_provider(component: str = "api") -> None:
    """
    Configures OpenTelemetry logs and metrics.
    Only runs in staging/production where a collector is available.
    In local, the default no-op providers are used.

    ``component`` names WHICH OF THE THREE CONTAINERS this is.

    All three - the API, the ingest worker and the scheduler - run the same code
    and would otherwise report under one identity, so "ingest stopped" and "the
    API is down" would be the same absence on the same series. It is a custom
    resource attribute rather than a suffix on ``service.name`` because
    dashboards and alerts select on ``service.name`` and it has to stay stable,
    and rather than ``service.instance.id`` because semantic conventions define
    that as unique per PROCESS - three replicas of the worker share this value
    deliberately.

    CALL IT BEFORE THE FIRST THING WORTH COUNTING. Instruments created against
    the proxy provider rebind when this runs, but nothing recorded beforehand is
    replayed - it is discarded silently, not even as a zero.
    """
    if settings.ENV == Environment.LOCAL:
        return

    # AND when there is nowhere to send it, whatever the ENV.
    #
    # `validate_env_config` enforces the LOGGING_* triple only under PRODUCTION,
    # and `.env.staging.exemple` explicitly tells you to leave them blank because
    # that "boots cleanly". It does - and then every container quietly starts a
    # 15-second export loop and a root-logger OTLP handler aimed at
    # `http://localhost:4318/`, which is the OTLP exporter's DEFAULT_ENDPOINT
    # when the argument is empty. Three containers, one per annexe, all
    # retrying against a port nothing is listening on.
    if not settings.LOGGING_METRICS_URL or not settings.LOGGING_LOGS_URL:
        logger.warning(
            "telemetry not configured - LOGGING_LOGS_URL/LOGGING_METRICS_URL are blank, "
            "so no exporter is started (env=%s, component=%s)",
            settings.ENV,
            component,
        )
        return

    # THIS SERVICE'S NAME, not the one it was copied from. It reached staging
    # as `administrative-document-backend`, which is invisible rather than
    # merely wrong: no dashboard or alert can select live-data, and a sibling's
    # error rates absorb ours. Only ENV != local reaches this line, so nothing
    # in dev or in the suite would ever have said so.
    resource = Resource.create(
        {
            "service.name": "live-data-backend",
            "env": settings.ENV,
            "component": component,
            # `component` groups the three CONTAINERS; this separates REPLICAS
            # within one of them, and the scheduler needs it: its own module
            # docstring says the advisory locks make a second replica a no-op and
            # that it is therefore NOT pinned to one, unlike the ingest worker.
            #
            # Without it two schedulers emit byte-identical stream identities for
            # `scheduler.job.duration.seconds` - recorded on EVERY path including
            # the one where the lock was held elsewhere - and the backend merges
            # two independent cumulative histograms into nonsense.
            #
            # The container id, which Docker puts in HOSTNAME. The uuid fallback
            # is for a bare process; it changes on restart, which is what
            # `service.instance.id` is defined to do.
            "service.instance.id": os.getenv("HOSTNAME") or str(uuid.uuid4()),
        }
    )
    headers = {"Authorization": f"Bearer {settings.LOGGING_TOKEN}"}

    # --- Logs ---
    log_exporter = OTLPLogExporter(
        endpoint=settings.LOGGING_LOGS_URL,
        headers=headers,
        timeout=EXPORTER_TIMEOUT_SECONDS,
    )
    log_provider = LoggerProvider(resource=resource)
    log_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
    handler = LoggingHandler(level=logging.INFO, logger_provider=log_provider)
    # Stamp request_id / user_id / community_id / user_role onto every record
    # before it's serialised and exported. Without this, prod logs lose
    # the per-request context that staging/local gain from configure_logging().
    handler.addFilter(RequestIdFilter())
    logging.getLogger().addHandler(handler)

    global _log_handler
    _log_handler = handler

    # --- Metrics ---
    metric_exporter = OTLPMetricExporter(
        endpoint=settings.LOGGING_METRICS_URL,
        headers=headers,
        timeout=EXPORTER_TIMEOUT_SECONDS,
    )
    metric_reader = PeriodicExportingMetricReader(metric_exporter, export_interval_millis=15000)
    meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
    metrics.set_meter_provider(meter_provider)

    global _meter_provider, _log_provider
    _meter_provider = meter_provider
    _log_provider = log_provider

    # AFTER the SDK's own, so LIFO puts this one first.
    #
    # The two workers also call `shutdown_telemetry()` explicitly in their
    # `finally`, at a known point after their last log line. THE API CONTAINER
    # DID NOT, and had no bounded flush at all - FastAPI's lifespan teardown was
    # never given one. Registering here covers every entrypoint, including the
    # ones nobody remembers; the explicit calls stay because running at a chosen
    # moment is better than running at interpreter exit, and calling twice is a
    # documented no-op.
    atexit.register(shutdown_telemetry)

    logger.info(
        "OpenTelemetry telemetry configured (logs, metrics)", extra={"component": component}
    )


def shutdown_telemetry(timeout_millis: int = EXPORTER_TIMEOUT_MS) -> None:
    """Flush the last interval of counters, WITHIN A BUDGET DOCKER WILL ALLOW.

    ---------------------------------------------------------------------------
    THE SDK ALREADY FLUSHES AT EXIT. THIS IS ABOUT THE TIMEOUT, NOT THE FLUSH.

    `MeterProvider.__init__` takes `shutdown_on_exit=True` by default and
    registers `atexit(self.shutdown)`, and that handler really does export - one
    final collect happens even with the interval nowhere near due (measured, not
    read: a counter recorded under a ten-minute interval still exported on a
    clean exit). An earlier version of this docstring claimed the opposite.

    What the atexit path does NOT do is bound itself usefully. `atexit` calls
    `shutdown()` with no arguments, so the budget is the SDK's default of
    **30 seconds**, while Docker sends SIGKILL **10 seconds** after SIGTERM.
    Against an unreachable or slow collector - which is exactly when a shutdown
    blocks - the container is killed mid-flush: the data is lost anyway AND every
    deploy takes the full grace period, per container.

    Calling it here with a 5 s budget flushes inside that window and makes the
    atexit call a no-op ("shutdown can only be called once"). Nothing helps
    against SIGKILL itself.
    ---------------------------------------------------------------------------

    Safe when no provider was ever installed (ENV=local, or a crash before
    setup), and safe to call twice.
    """
    # LOGS FIRST, metrics second: the caller's last log line is written before
    # this runs and both entrypoints say so. `BatchLogRecordProcessor` batches on
    # its own timer exactly like the metric reader, so an unflushed log provider
    # drops the "ingest worker stopped" / "scheduler stopped" line an operator
    # uses to tell a clean stop from a kill.
    #
    # `force_flush` for logs and `shutdown` for metrics, because those are the
    # two that take a BUDGET. `LoggerProvider.shutdown()` has no timeout
    # parameter at all and would run unbounded - which is the behaviour this
    # whole function exists to prevent. Its atexit handler still runs afterwards,
    # on a provider that has nothing left to send.
    global _log_handler
    if _log_provider is not None:
        try:
            _log_provider.force_flush(timeout_millis=timeout_millis)
            # AND DETACH IT. `logging.shutdown` is registered with atexit by the
            # logging module at import, so LIFO runs it LAST - after the
            # interpreter has begun tearing down, where `LoggingHandler.flush()`
            # spawning a thread raises `RuntimeError: can't create new thread at
            # interpreter shutdown`. Every container printed that traceback on
            # exit. Once the batch is flushed there is nothing left to lose by
            # removing the handler, and nothing left for `logging.shutdown` to do.
            if _log_handler is not None:
                logging.getLogger().removeHandler(_log_handler)
                _log_handler = None
        except Exception:
            # A collector that is down or slow must not stop a container from
            # exiting. The process is on its way out; there is nowhere to report
            # to, and the next line would go to the handler being flushed.
            logger.warning("flushing logs on shutdown failed", exc_info=True)

    if _meter_provider is not None:
        try:
            _meter_provider.shutdown(timeout_millis=timeout_millis)
        except Exception:
            logger.warning("flushing metrics on shutdown failed", exc_info=True)


tracer = trace.get_tracer(__name__)


async def enrich_span():
    """
    FastAPI global dependency — enriches the active OpenTelemetry span with
    user context from ContextVars.

    Must be registered after set_auth_context so ContextVars are already populated.

    Sets:
        user.id    → current_user_id
        user.email → current_user_email
        user.role  → current_user_role

    Automatically sets span status to OK on clean exit if status was not
    already set to ERROR by the request handler.
    """
    span = trace.get_current_span()

    if span and span.is_recording():
        span.set_attribute("user.id", current_user_id.get() or "")
        span.set_attribute("user.role", current_user_role.get() or "")
        span.set_attribute("request.id", current_request_id.get() or "")

    try:
        yield
    finally:
        if span and span.is_recording() and span.status.status_code == StatusCode.UNSET:
            span.set_status(Status(StatusCode.OK))


def add_attribute(key: str, value) -> None:
    """Safely adds a key-value attribute to the current span."""
    span = trace.get_current_span()
    if span and span.is_recording():
        span.set_attribute(key, value)


def add_event(name: str, attributes: dict | None = None) -> None:
    """Safely adds a named event with optional attributes to the current span."""
    span = trace.get_current_span()
    if span and span.is_recording():
        span.add_event(name, attributes or {})


def add_exception(exception: Exception) -> None:
    """
    Records an exception on the current span and sets status to ERROR.
    Call this in exception handlers when you want the trace to reflect the failure.
    """
    span = trace.get_current_span()
    if span and span.is_recording():
        span.record_exception(exception)
        span.set_status(Status(StatusCode.ERROR, str(exception)))


def set_span_status(status: Status) -> None:
    """Safely sets the status of the current span."""
    span = trace.get_current_span()
    if span and span.is_recording():
        span.set_status(status)
