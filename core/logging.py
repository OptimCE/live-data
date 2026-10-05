# core/logging.py
import logging
import sys

from pythonjsonlogger.json import JsonFormatter

from core.config import Environment, settings


class RequestIdFilter(logging.Filter):
    """Injects request-scoped ContextVars onto every log record.

    Every log line — including the central error handlers in
    core/errors/handlers.py — gets request_id, user_id, community_id and
    user_role stamped automatically. This is the single point where log
    enrichment happens; no raise site needs to repeat the context.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        # Lazy import to avoid circular import — configure_logging() runs at
        # module load time in main.py before other core modules are ready.
        from core.context_vars import (
            current_community_id,
            current_request_id,
            current_user_id,
            current_user_role,
        )

        record.request_id = current_request_id.get() or "-"
        record.user_id = current_user_id.get() or "-"
        record.community_id = current_community_id.get() or "-"
        record.user_role = current_user_role.get() or "-"
        return True


def configure_logging() -> None:
    """
    Configure root logger based on environment.

    local    → human-readable console output, DEBUG level
    staging  → JSON stdout, INFO level
    production → JSON stdout, INFO level

    Called once at startup in main.py. Never call define_logger() per-module —
    use logging.getLogger(__name__) everywhere else.
    """
    root = logging.getLogger()
    root.handlers.clear()

    if settings.ENV == Environment.LOCAL:
        handler = logging.StreamHandler(sys.stdout)
        handler.addFilter(RequestIdFilter())
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | req=%(request_id)s "
            "| user=%(user_id)s | community=%(community_id)s | %(message)s",
            datefmt="%H:%M:%S",
        )
        handler.setFormatter(formatter)
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)
    elif settings.ENV == Environment.STAGING:
        handler = logging.StreamHandler(sys.stdout)
        handler.addFilter(RequestIdFilter())
        formatter = JsonFormatter(
            fmt=(
                "%(asctime)s %(levelname)s %(name)s %(request_id)s "
                "%(user_id)s %(community_id)s %(user_role)s %(message)s"
            ),
            rename_fields={"levelname": "level", "asctime": "timestamp"},
        )
        handler.setFormatter(formatter)
        root.addHandler(handler)
        root.setLevel(logging.INFO)
    else:
        # production: logs exported via OpenTelemetry (OTLP), no console output
        root.setLevel(logging.INFO)

    # Silence noisy third-party loggers
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
