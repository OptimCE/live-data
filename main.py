import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from starlette.middleware.cors import CORSMiddleware

from api.health.routes import health_router
from api.live.routes import live_routes
from api.live_public.routes import PUBLIC_TAG, live_public_routes
from core.config import Environment, settings
from core.errors.errors import ErrorException
from core.errors.handlers import error_exception_handler, unhandled_exception_handler
from core.logging import configure_logging
from core.middleware.correlation_id import CorrelationIdMiddleware
from core.middleware.locale_middleware import LocaleMiddleware
from core.middleware.request_limits import RequestLimitsMiddleware
from core.middleware.set_auth_context import GatewayScopeMiddleware
from core.tracing import enrich_span, setup_tracer_provider
from ports.broker_mqtt import MqttDeviceBroker
from ports.providers import set_device_broker

configure_logging()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown.

    NO NATS (plan 4). Nothing publishes and nothing consumes, and wiring it would
    make /health/readiness fail closed on a broker this service does not use. The
    subject space `optimce.live.>` is reserved in shared/const.py; do not add
    core/queue/ without a producer.

    NO REALTIME either, so there is no `log_realtime_state(...)` line here. Its
    absence is meaningful in the sibling services - it means the image predates
    the feature - so its absence here is worth a sentence: this service never had
    it. At 15-minute granularity a push can only say "refetch", which the client
    already does on its poll.

    THE BROKER connection is long-lived and is started here. A TLS handshake
    inside a request KrakenD cuts at 3000 ms is not viable, so a connection per
    request is out - and `start()` NEVER RAISES: a broker that is down at boot
    degrades to 503 on enrolment rather than preventing the API from serving the
    admin surface, which needs no broker at all. `/health/readiness` deliberately
    does not gate on it either, because compose's healthcheck reads that and a
    broker restart must not roll the API out of service.
    """
    setup_tracer_provider()
    broker = MqttDeviceBroker()
    await broker.start()
    set_device_broker(broker)
    try:
        yield
    finally:
        set_device_broker(None)
        await broker.stop()


protected_deps = [Depends(enrich_span)]
app = FastAPI(
    lifespan=lifespan,
    docs_url="/docs" if settings.ENV == Environment.LOCAL else None,
    redoc_url="/redoc" if settings.ENV == Environment.LOCAL else None,
    openapi_url="/openapi.json" if settings.ENV == Environment.LOCAL else None,
)

# --- Middleware (executed bottom to top — last registered = outermost = executes first) ---
app.add_middleware(LocaleMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in settings.ALLOW_ORIGIN.split(",") if o.strip()],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "Accept-Language", "X-Request-ID"],
    expose_headers=[
        "Content-Disposition",
        "X-RateLimit-Limit",
        "X-RateLimit-Remaining",
        "X-RateLimit-Reset",
        "X-Request-ID",
    ],
)
app.add_middleware(RequestLimitsMiddleware)
app.add_middleware(CorrelationIdMiddleware)
# Outermost. It reads x-user-id / x-community-id / x-user-orgs / x-source-ip
# UNCONDITIONALLY, including on the public leg — which is why nginx blanks those
# five headers on `= /api/live-public/enroll`, and why nothing under
# api/live_public/ may read the ContextVars it sets.
app.add_middleware(GatewayScopeMiddleware)

# --- Exception handlers ---
# Starlette's stub types the handler signature as (Request, Exception); FastAPI lets
# you narrow to a specific exception subclass at runtime, which the stub doesn't model.
app.add_exception_handler(ErrorException, error_exception_handler)  # type: ignore[arg-type]
app.add_exception_handler(Exception, unhandled_exception_handler)

# --- Routers ---
#
# TWO routers, and the `tags=` on the second is load-bearing rather than
# cosmetic: it is the ONLY thing that tells scripts/export_openapi.py which
# operations belong in live-public.json. KrakenD gates its JWT validator per
# SERVICE ENTRY, so the public routes need their own spec, their own
# krakend-builder.yaml key and `auth: false` — there is no per-endpoint switch.
#
# Neither router carries a `prefix=`. The `/live` and `/live-public` namespaces
# come from the builder keys; `url_pattern` strips them, so this app serves
# `/version` and `/enroll`.
app.include_router(
    live_routes,
    tags=["Live data"],
    dependencies=protected_deps,
)
app.include_router(
    live_public_routes,
    tags=[PUBLIC_TAG],
    # NO `dependencies=protected_deps` here. `enrich_span` is harmless, but
    # keeping this list empty is what makes "the public router has no
    # dependencies" a property a test can assert rather than a claim.
)
app.include_router(health_router, prefix="/health", tags=["Health"])

FastAPIInstrumentor.instrument_app(app)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)  # noqa: S104  # container-internal API behind KrakenD gateway
