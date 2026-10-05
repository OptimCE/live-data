"""Global test fixtures for the live-data service.

Infrastructure:
  pytest-docker   - spins up tests/docker-compose.test.yml at session start and
                    tears it down at session end. No manual `docker compose up`.

Schema:
  No Alembic. The schema is applied from scripts/sql/schema.sql - the single
  source of truth - through ASYNCPG's simple-query protocol, not psql.
  (The sibling services' conftest docstrings say psql; the code has always used
  asyncpg. The difference matters: asyncpg accepts multiple statements but NOT
  psql meta-commands or bind parameters, which is why schema.sql contains none.)
  `Base.metadata.create_all()` is intentionally NOT used - it would miss the
  check constraints, the partial indexes, the partitioning and the plpgsql.

Session isolation:
  Each test gets a connection-level transaction rolled back on teardown.
  `join_transaction_mode="create_savepoint"` turns a `session.commit()` inside a
  route handler into RELEASE SAVEPOINT, so the outer transaction stays open for
  the final rollback. Factories must flush(), never commit().

Auth:
  Exercised FOR REAL through GatewayScopeMiddleware by passing gateway_headers()
  per request. Only the two session dependencies are overridden - never an auth
  dependency, because the auth chain is a thing these tests exist to check.

NO BROKER.
  There is no Mosquitto container here, and that is D-7 rather than an omission.
  GitHub Actions creates `services:` containers BEFORE `actions/checkout`, so a
  repo-tracked mosquitto.conf cannot be their bind-mount source - and this broker
  needs one. (Not one of the six tests/docker-compose.test.yml files in the
  monorepo mounts anything; live-data would be introducing the first.) So the
  suite drives FakeDeviceBroker and unit-tests the surface we expose to the
  broker, which is the house policy already stated in the siblings'
  test_nats_resilience.py. The end-to-end half runs against the dev stack from
  the monorepo's scripts/verify-live-ingest.sh.

NOTHING UNDER tests/ MAY READ A FILE OUTSIDE THIS REPOSITORY.
  Submodule CI checks out the submodule alone. A test that reaches for a
  monorepo-root file passes on a dev machine and fails in CI - that cost a
  sibling service 128 tests once. live-data has no such dependency today; keep
  it that way.
"""

import contextlib
import os
import socket
from collections.abc import AsyncGenerator
from pathlib import Path

# Force the test environment file (.env.test) BEFORE any project module is
# imported. core.config.Settings reads ENV at import time to choose .env.<env>,
# and the module ends with `settings = Settings()`, so this assignment must come
# above `from main import app`.
os.environ.setdefault("ENV", "test")

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from core.database.database import get_crm_session, get_local_session
from main import app
from worker.subscriptions import SubscriptionCache

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Must match docker-compose.test.yml: port 5433.
TEST_DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5433/test_db_be"
_ASYNCPG_DSN = "postgresql://postgres:postgres@localhost:5433/test_db_be"

_REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
SCHEMA_SQL = os.path.normpath(os.path.join(_REPO_ROOT, "scripts", "sql", "schema.sql"))

# Forward-only, applied in name order AFTER schema.sql. schema.sql already
# contains everything they do; applying both is what proves each migration is
# re-runnable, and what makes this database match the live UPGRADE path rather
# than only the fresh-provision one.
MIGRATIONS_DIR = os.path.normpath(os.path.join(_REPO_ROOT, "scripts", "sql", "migrations"))

# Test-only DDL for the CRM tables this service reads. The real CRM schema is
# owned by crm-backend; only the minimum is mirrored here, with column types
# identical to production.
CRM_TEST_SCHEMA_SQL = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "sql", "crm_test_schema.sql")
)

# No seeds directory: this service ships no reference data. The sibling
# conftests glob scripts/sql/seeds/*.sql; that loop is deliberately absent
# rather than left globbing an empty directory, so its absence reads as a fact
# about the service rather than as an oversight.


# ---------------------------------------------------------------------------
# pytest-docker — managed Postgres container
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    """Point pytest-docker at the test-only Compose file."""
    return os.path.join(str(pytestconfig.rootdir), "tests", "docker-compose.test.yml")


@pytest.fixture(scope="session")
def docker_compose_project_name():
    return "live-data-test"


def _is_pg_ready(host: str, port: int) -> bool:
    """True if Postgres is accepting TCP connections."""
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def test_db_ready(request):
    """Block until Postgres is accepting connections on port 5433.

    Two paths, and they are not symmetric. In GitHub Actions a service container
    is already running before the first step, so this only verifies
    connectivity. Locally, pytest-docker starts the container from
    docker-compose.test.yml first.

    Note the CI branch hard-codes localhost:5433 and ignores docker_ip/port_for
    entirely - so a SECOND container added to the local compose file would have
    no equivalent here. That asymmetry is the concrete reason a broker container
    does not live in this file (see the module docstring).
    """
    if os.getenv("CI"):
        # GitHub Actions: service container already running on 5433.
        import time

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if _is_pg_ready("localhost", 5433):
                return 5433
            time.sleep(0.5)
        raise RuntimeError("Postgres not ready on port 5433 after 30 s")

    # Local: delegate to pytest-docker.
    docker_services = request.getfixturevalue("docker_services")
    docker_ip = request.getfixturevalue("docker_ip")
    port = docker_services.port_for("db-test", 5432)
    docker_services.wait_until_responsive(
        timeout=30.0,
        pause=0.5,
        check=lambda: _is_pg_ready(docker_ip, port),
    )
    return port


# ---------------------------------------------------------------------------
# Schema — applied once per session from scripts/sql/schema.sql
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session", autouse=True)
def apply_schema(test_db_ready):
    import asyncio

    import asyncpg

    schema_sql = Path(SCHEMA_SQL).read_text(encoding="utf-8")
    crm_test_schema_sql = Path(CRM_TEST_SCHEMA_SQL).read_text(encoding="utf-8")
    # Every migration, in name order, ON TOP of schema.sql - which already
    # contains all of it.
    #
    # That is deliberate, not waste. `provision.sh` applies a schema only to a
    # database with no relation of relkind 'r'/'p', so schema.sql is never
    # re-applied to a live one and the two files are maintained in parallel with
    # nothing keeping them equal. Applying both here means the test database
    # matches the LIVE UPGRADE path, and it proves on every run that each
    # migration is re-runnable - a bare CREATE TRIGGER in one of them fails here
    # rather than the second time someone runs it against production.
    migration_sql = [
        path.read_text(encoding="utf-8") for path in sorted(Path(MIGRATIONS_DIR).glob("*.sql"))
    ]

    async def _apply():
        conn = await asyncpg.connect(_ASYNCPG_DSN)
        try:
            # The whole file, once, through the simple-query protocol - exactly
            # as provision.sh applies it in the dev stack.
            await conn.execute(schema_sql)
            for sql in migration_sql:
                await conn.execute(sql)
            await conn.execute(crm_test_schema_sql)
        finally:
            await conn.close()

    async def _teardown():
        conn = await asyncpg.connect(_ASYNCPG_DSN)
        try:
            # CASCADE reaches the partitions too: they are ordinary tables in
            # schema public, so dropping the schema takes parent and children
            # together. Recreating it leaves a database the next session can
            # provision from scratch.
            await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        finally:
            await conn.close()

    asyncio.run(_apply())
    yield
    asyncio.run(_teardown())


# ---------------------------------------------------------------------------
# Engine — session-scoped SYNC fixture, NullPool
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def test_engine(apply_schema):
    """Session-scoped SYNC fixture (not pytest_asyncio), with NullPool.

    Both choices are Windows workarounds and both are deliberate. A session-
    scoped ASYNC fixture would have its finalizer run after the session event
    loop is already closed, and asyncpg then raises
    `AttributeError: 'NoneType' object has no attribute 'send'` from the
    IocpProactor. NullPool means every `engine.connect()` opens and closes a
    fresh connection inside the test's own loop, so no pool machinery outlives
    it.
    """
    import asyncio

    engine = create_async_engine(TEST_DATABASE_URL, poolclass=NullPool)
    yield engine
    asyncio.run(engine.dispose())


# ---------------------------------------------------------------------------
# DB session — per-test, rolled back automatically
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def db_session(test_engine) -> AsyncGenerator[AsyncSession, None]:
    """Wrap each test in a connection-level transaction, rolled back on teardown."""
    async with test_engine.connect() as conn:
        await conn.begin()
        factory = async_sessionmaker(
            bind=conn,
            expire_on_commit=False,
            join_transaction_mode="create_savepoint",
        )
        session = factory()
        yield session
        await session.close()
        await conn.rollback()


# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def client(db_session: AsyncSession) -> AsyncGenerator[AsyncClient, None]:
    """Full HTTP stack via ASGITransport (no network; ASGI lifespan NOT triggered).

    Only the two session dependencies are overridden. Auth is exercised for real
    by passing gateway_headers() per request - never by overriding an auth
    dependency, because on this service the auth chain (and in particular what
    the PUBLIC leg does with forged headers) is one of the things under test.

    One Postgres holds both the owned schema and the mirrored CRM tables, so the
    same per-test session backs both bases.
    """

    async def _override_get_session():
        yield db_session

    app.dependency_overrides[get_crm_session] = _override_get_session
    app.dependency_overrides[get_local_session] = _override_get_session

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Tenancy + auth helpers
# ---------------------------------------------------------------------------


def gateway_headers(
    auth_community_id: str,
    *,
    role: str = "MANAGER",
    user_id: str = "auth-user-1",
    community_name: str = "/Test Community",
) -> dict[str, str]:
    """The headers KrakenD injects after validating the Keycloak token.

    `x-user-orgs` is the Keycloak blob of every org the caller belongs to; the
    middleware picks the role of the org matching `x-community-id`. The org path
    deliberately contains a SPACE, because real community names do and that once
    broke the parser.
    """
    return {
        "x-user-id": user_id,
        "x-community-id": auth_community_id,
        "x-user-orgs": f"[orgId:{auth_community_id} orgPath:{community_name} roles:[{role}]]",
    }


@pytest_asyncio.fixture
async def community(db_session: AsyncSession):
    """A community subscribed to this annexe, as every authenticated request assumes."""
    from shared.const import FeatureName
    from tests.factories.subscription_factory import create_community, create_subscription

    created = await create_community(db_session)
    await create_subscription(
        db_session,
        id_community=created.id,
        feature=FeatureName.LIVE_DATA,
        is_active=True,
    )
    return created


@pytest_asyncio.fixture
async def unsubscribed_community(db_session: AsyncSession):
    """A community with NO live-data subscription.

    The negative control for require_feature, and - more importantly - for the
    PUBLIC enrolment leg, where the subscription must be checked by hand because
    require_feature cannot run at all.
    """
    from tests.factories.subscription_factory import create_community

    return await create_community(db_session)


@pytest_asyncio.fixture
async def deactivated_community(db_session: AsyncSession):
    """A community that WAS subscribed and has been switched off (D-12).

    Not the same thing as `unsubscribed_community`, and the difference is what
    crm-backend's unsubscribe actually leaves behind: the row is KEPT, with
    `is_active = false`. A check written as "a row exists" passes the
    never-subscribed fixture and waves this one through - so every switch-off
    test runs against both.
    """
    from shared.const import FeatureName
    from tests.factories.subscription_factory import create_community, create_subscription

    created = await create_community(db_session)
    await create_subscription(
        db_session,
        id_community=created.id,
        feature=FeatureName.LIVE_DATA,
        is_active=False,
    )
    return created


@pytest.fixture
def manager_headers(community):
    return gateway_headers(community.auth_community_id, role="MANAGER")


@pytest.fixture
def member_headers(community):
    return gateway_headers(community.auth_community_id, role="MEMBER")


@pytest.fixture
def admin_headers(community):
    return gateway_headers(community.auth_community_id, role="ADMIN")


# ---------------------------------------------------------------------------
# Worker and scheduler helpers
# ---------------------------------------------------------------------------


def sessionmaker_for(session):
    """A stand-in sessionmaker that hands back the TEST's session.

    The suite's isolation turns `session.commit()` into RELEASE SAVEPOINT inside
    one transaction that is rolled back at teardown, so a job opening its own
    connection would see none of the rows a test just wrote. Handing it this
    session is what makes a scheduler job's own query testable at all.
    """

    @contextlib.asynccontextmanager
    async def factory():
        yield session

    return factory


def static_subscriptions(*ids: int) -> SubscriptionCache:
    """A `SubscriptionCache` over a fixed set of community ids, no CRM behind it.

    For tests about something else that still have to hand the worker a set,
    because `handle_message` and `_consume` take one with no default. Tests about
    the set itself load it through the real CRM read instead.
    """
    active = frozenset(ids)

    async def load() -> frozenset[int]:
        return active

    return SubscriptionCache(load, ttl_seconds=60)


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def metric_reader():
    """ONE MeterProvider for the whole session, because a second is refused.

    -----------------------------------------------------------------------
    `metrics.set_meter_provider()` IS ONCE-ONLY AND ONLY WARNS.

    The API guards it with a `do_once`; a later call logs "Overriding of current
    MeterProvider is not allowed" and is IGNORED, after which that provider's
    reader returns `None` from `get_metrics_data()`. A per-test provider would
    therefore make assertions a function of pytest's collection order - whichever
    test ran first would pass and the rest would fail on a `None`, with the only
    explanation in a log line nobody reads.

    Session scope also means COUNTERS ARE CUMULATIVE ACROSS TESTS. Read deltas
    with `metric_delta`, never absolutes.
    -----------------------------------------------------------------------

    Nothing else installs a provider under the suite: `setup_tracer_provider` is
    only reached through the ASGI lifespan, which `client` deliberately does not
    trigger. That matters more than it looks - ENV=test is not Environment.LOCAL,
    so the early return does NOT apply, and a test that did run it would build
    real OTLP exporters against a blank endpoint and burn this one-shot slot.
    """
    from opentelemetry import metrics as otel_metrics
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    otel_metrics.set_meter_provider(provider)

    # Record once, immediately, so `get_metrics_data()` never returns None again.
    #
    # It returns None until SOMETHING has been recorded - which is
    # indistinguishable, at the call site, from the reader belonging to a
    # provider that was installed second and ignored. Priming it here collapses
    # that ambiguity: afterwards a None genuinely means the provider was
    # overridden, and `read_metrics` can say so instead of guessing.
    otel_metrics.get_meter("live-data").create_counter("test.harness.ready").add(1)
    assert reader.get_metrics_data() is not None, (
        "this reader collected nothing, which means a MeterProvider was installed "
        "BEFORE this fixture and ours was the one refused - `set_meter_provider` "
        "is once-only and only WARNS. Find the earlier caller; nothing in the "
        "suite should install one, because the ASGI lifespan is never triggered."
    )
    # The direct check the None-test cannot make: it is possible to collect data
    # and still not be the provider the code under test records through.
    assert otel_metrics.get_meter_provider() is provider
    return reader


def read_metrics(reader) -> dict[tuple[str, tuple], float]:
    """Flatten everything collected so far into {(name, attrs): value}.

    Histograms report their COUNT rather than their sum: the assertions that
    matter are "did this observation happen", and a sum invites a test that
    pins a duration, which is a flake on a loaded machine.
    """
    out: dict[tuple[str, tuple], float] = {}
    data = reader.get_metrics_data()
    if data is None:  # pragma: no cover - the fixture primes the reader
        # NOT the second-provider case: a refused second provider leaves the
        # FIRST reader working and returns None from the ignored one. Reaching
        # here means this reader's own provider was shut down.
        raise AssertionError(
            "the metric reader returned None after being primed - its provider "
            "has been shut down, so nothing can be observed for the rest of the "
            "session"
        )
    for resource_metric in data.resource_metrics:
        for scope_metric in resource_metric.scope_metrics:
            for metric in scope_metric.metrics:
                for point in metric.data.data_points:
                    key = (metric.name, tuple(sorted(point.attributes.items())))
                    # A sum point carries `value`; a histogram point carries
                    # `count`. The count is deliberate: assertions that pin a
                    # duration are flakes on a loaded machine, so what is
                    # observable here is "did this observation happen".
                    raw = getattr(point, "value", None)
                    out[key] = float(raw if raw is not None else getattr(point, "count", 0))
    return out


@pytest.fixture
def metric_delta(metric_reader):
    """Snapshot now; return a callable giving what changed since.

    Deltas, not absolutes, because the provider is session-scoped and every
    earlier test's counts are still in the sum.
    """
    before = read_metrics(metric_reader)

    def changed() -> dict[tuple[str, tuple], float]:
        after = read_metrics(metric_reader)
        return {
            key: value - before.get(key, 0)
            for key, value in after.items()
            if value - before.get(key, 0) != 0
        }

    return changed
