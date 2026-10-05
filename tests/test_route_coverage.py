"""Nothing reads tenant data without going through the chokepoint.

plan 9.1: "Every scoped read goes through one `_scoped()` chokepoint, and a
route-coverage test fails when a route is added without it. This is not
architectural taste. Retrofitting authorisation is how every
`administrative-document` GET once ended up readable by any member of a
subscribed community, full snapshot included."

THREE LAYERS, because each one misses what the others catch:

  1. STATIC - an AST walk of `api/live/repository.py`. Catches a new repository
     method that forgets `_scoped`, even if no route calls it yet.
  2. BEHAVIOURAL - every route, driven twice against two populated communities.
     Catches a leak that the AST cannot see, such as a raw `text()` query.
  3. NEGATIVE CONTROLS - the checkers are run against code that MUST fail them.
     Without this layer the first two can silently stop checking anything, which
     is the failure mode of every guard ever written.

BE HONEST ABOUT WHICH GATE FIRES. A request with an unresolvable community gets
its 403 from `require_feature`, not from `_scoped()`. `_scoped`'s own fail-closed
behaviour is proven separately, at the repository level, with the ContextVar
unset - see `TestScopedFailsClosed`.
"""

import ast
import datetime
import inspect
import pathlib
import uuid

import pytest
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from api.live import repository as repository_module
from core.context_vars import current_internal_community_id
from core.errors.errors import ErrorException
from main import app
from ports.crm_core import SqlAlchemyCrmCoreRead
from shared.const import FeatureName
from tests.conftest import gateway_headers
from tests.factories.device_factory import create_device, create_hour_of_measurements
from tests.factories.meter_factory import create_owned_meter
from tests.factories.operation_factory import (
    create_member,
    create_operation,
    link_user_to_member,
)
from tests.factories.subscription_factory import create_community, create_subscription
from worker import rollups
from worker.ownership import refresh_community

# ---------------------------------------------------------------------------
# Layer 1 - static
# ---------------------------------------------------------------------------

SCOPING_CALLS = frozenset({"_scoped", "_scoped_join"})
_SOURCE = pathlib.Path(inspect.getfile(repository_module)).read_text(encoding="utf-8")
_TREE = ast.parse(_SOURCE)


def _methods(tree: ast.AST, class_name: str) -> list[ast.FunctionDef | ast.AsyncFunctionDef]:
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return [
                item
                for item in node.body
                if isinstance(item, ast.FunctionDef | ast.AsyncFunctionDef)
            ]
    raise AssertionError(f"class {class_name} not found")


def _called_names(fn: ast.AST) -> set[str]:
    """Every bare function name called inside `fn`.

    Bare names only - `self._session.execute(...)` is an Attribute, not a Name,
    and is not what this looks for. `select(`, `_scoped(` and `text(` all are.
    """
    return {
        node.func.id
        for node in ast.walk(fn)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }


def unscoped_reads(tree: ast.AST, class_name: str) -> list[str]:
    """Method names that build a SELECT without scoping it."""
    offenders = []
    for method in _methods(tree, class_name):
        called = _called_names(method)
        if "select" in called and not (called & SCOPING_CALLS):
            offenders.append(method.name)
    return offenders


class TestStaticCoverage:
    def test_the_walker_found_the_methods(self):
        """Guards the guard. A rename that empties this list would make every
        assertion below pass by iterating over nothing."""
        names = {m.name for m in _methods(_TREE, "LiveRepository")}
        assert len(names) >= 10
        assert "list_devices" in names
        assert "community_hours" in names

    def test_every_select_in_LiveRepository_is_scoped(self):  # noqa: N802 - names a class
        offenders = unscoped_reads(_TREE, "LiveRepository")
        assert (
            offenders == []
        ), f"these LiveRepository methods build a SELECT without _scoped(): {offenders}"

    def test_EnrolmentRepository_is_deliberately_unscoped(self):  # noqa: N802 - names a class
        """THE INVERSE ASSERTION, and it is not symmetry for its own sake.

        The public enrolment leg has no user, no community header and therefore
        no tenant ContextVar - nginx blanks those headers on purpose. `_scoped`
        there would raise 403 on every enrolment in the field. So this class must
        NOT use it, and this test is what stops someone "fixing" the asymmetry
        after reading the test above.
        """
        for method in _methods(_TREE, "EnrolmentRepository"):
            called = _called_names(method)
            assert not (called & SCOPING_CALLS), f"{method.name} must stay unscoped"
            assert "tenant_id" not in called, f"{method.name} must stay unscoped"


# ---------------------------------------------------------------------------
# Layer 2 - behavioural
# ---------------------------------------------------------------------------

# Every authenticated route, and how to call it. `set(PROBE) == set(routes)` is
# asserted below, so ADDING A ROUTE WITHOUT A PROBE FAILS HERE - which is the
# whole point. A probe that is merely hard to write is a signal in itself.
PROBE: dict[tuple[str, str], dict] = {
    ("/version", "GET"): {},
    ("/devices", "GET"): {},
    ("/summary", "GET"): {},
    ("/series", "GET"): {"params": {"resolution": "hour"}},
    ("/settings", "GET"): {},
    ("/forecast", "GET"): {},
    ("/forecast/methods", "GET"): {},
    ("/ops/health", "GET"): {},
    ("/devices/{device_id}/diagnostics", "GET"): {"path_params": ["device_id"]},
    # Sharing operations (D-14): the manager's per-operation view and the
    # member's own operations.
    ("/operations", "GET"): {},
    ("/operations/{operation_id}/summary", "GET"): {"path_params": ["operation_id"]},
    ("/operations/{operation_id}/series", "GET"): {
        "path_params": ["operation_id"],
        "params": {"resolution": "hour"},
    },
    ("/mine/operations", "GET"): {},
    ("/mine/operations/{operation_id}/series", "GET"): {
        "path_params": ["operation_id"],
        "params": {"resolution": "hour"},
    },
    # WRITES are probed too, and deliberately: a POST that ignores the tenant
    # writes a row into another community, which no read-only probe can see.
    ("/devices", "POST"): {
        "json": {"name": "probe", "ean": "541448000000009999", "pure_injection": True}
    },
    ("/settings", "PUT"): {
        "json": {"members_see_production": True, "members_see_aggregate": True, "k": 5}
    },
    ("/devices/{device_id}/token", "POST"): {"path_params": ["device_id"]},
    ("/devices/{device_id}/revoke", "POST"): {"path_params": ["device_id"]},
}


def _url(path: str, spec: dict, **values: str) -> str:
    """Substitute EVERY placeholder a probe declares, and prove none is left.

    It used to replace `{device_id}` only. A literal `{operation_id}` would still
    get the expected 403 from `require_feature`, which runs BEFORE FastAPI
    validates the path - so the probe would pass while calling nothing real.
    """
    defaults = {"device_id": str(uuid.uuid4()), "operation_id": "1"}
    url = path
    for name in spec.get("path_params", []):
        url = url.replace("{" + name + "}", values.get(name, defaults[name]))
    assert "{" not in url, f"{path}: placeholder left unsubstituted in {url}"
    return url


_NOT_TENANT_ROUTES = frozenset({"/enroll"})


_FRAMEWORK_ROUTES = frozenset({"/openapi.json", "/docs", "/redoc", "/docs/oauth2-redirect"})


def tenant_routes_of(application) -> set[tuple[str, str]]:
    """Every authenticated, tenant-scoped (path, method) on `application`.

    Takes the app as a parameter rather than closing over the real one, so the
    negative control below can run THIS function - not a copy of it - against a
    throwaway app. A guard tested through a reimplementation of itself is a guard
    whose test passes after the original breaks.
    """
    found = set()
    for route in application.routes:
        methods = getattr(route, "methods", None)
        path = getattr(route, "path", "")
        if not methods or path.startswith("/health"):
            continue
        if path in _NOT_TENANT_ROUTES or path in _FRAMEWORK_ROUTES:
            continue
        for method in methods:
            if method in ("HEAD", "OPTIONS"):
                continue
            found.add((path, method))
    return found


class TestProbeCoverage:
    def test_every_route_has_a_probe(self):
        """Adding a route without a probe fails HERE, before it ships.

        The alternative - a coverage test that only checks the routes it happens
        to know about - is the shape that let an `administrative-document` GET go
        unnoticed. Equality, not containment.
        """
        assert tenant_routes_of(app) == set(PROBE)


@pytest.fixture
async def two_communities(db_session: AsyncSession):
    """Two subscribed, populated communities, A and B, each with one sharing
    operation, rolled up.

    The fixture's user `auth-user-1` is linked to the member of B who holds B's
    meter - an `app_user` is GLOBAL, one person across every community. So
    `/mine/operations` asked as A must still answer `[]`: anything else is the
    cross-community leak the per-join scoping in ports/crm_operations.py exists
    to prevent.
    """
    made = []
    now = datetime.datetime.now(datetime.UTC)
    bucket = now.replace(minute=0, second=0, microsecond=0) - datetime.timedelta(hours=2)
    for _ in range(2):
        community = await create_community(db_session)
        await create_subscription(
            db_session,
            id_community=community.id,
            feature=FeatureName.LIVE_DATA,
            is_active=True,
        )
        operation = await create_operation(
            db_session, id_community=community.id, name=f"Operation of {community.name}"
        )
        member = await create_member(db_session, id_community=community.id)
        ean = await create_owned_meter(
            db_session,
            id_community=community.id,
            id_member=member,
            id_sharing_operation=operation,
        )
        device_id = await create_device(db_session, id_community=community.id, ean=ean)
        await create_hour_of_measurements(
            db_session, id_device=device_id, id_community=community.id, bucket=bucket
        )
        await refresh_community(
            db_session, SqlAlchemyCrmCoreRead(db_session), id_community=community.id, now=now
        )
        await rollups.tick_community(db_session, id_community=community.id, now=now)
        made.append((community, ean, device_id, operation, member))
    b_member = made[1][4]
    await link_user_to_member(db_session, auth_user_id="auth-user-1", id_member=b_member)
    return made


@pytest.fixture(params=["unsubscribed_community", "deactivated_community"])
def switched_off_community(request):
    """Never subscribed, or subscribed and then switched off (D-12).

    A SYNC fixture resolving the async one, because `getfixturevalue` cannot be
    called from inside a running test's event loop."""
    return request.getfixturevalue(request.param)


def _strings_in(payload) -> set[str]:
    """Every string and number in a response body, flattened."""
    found: set[str] = set()
    if isinstance(payload, dict):
        for key, value in payload.items():
            found.add(str(key))
            found |= _strings_in(value)
    elif isinstance(payload, list):
        for item in payload:
            found |= _strings_in(item)
    elif payload is not None:
        found.add(str(payload))
    return found


class TestBehaviouralCoverage:
    async def test_no_identifier_of_b_appears_in_any_response_to_a(self, client, two_communities):
        """Drive every GET as community A and look for anything belonging to B.

        The EAN is the strongest marker available: it is B's, it is unique, and
        it appears in any payload that leaked a device of theirs.
        """
        a, b = two_communities
        a_community, b_community, b_ean = a[0], b[0], b[1]
        headers = gateway_headers(a_community.auth_community_id, role="MANAGER")
        forbidden = {
            b_ean,
            b_community.name,
            b_community.auth_community_id,
            f"Operation of {b_community.name}",
        }

        for (path, method), spec in sorted(PROBE.items()):
            if method != "GET" or spec.get("path_params"):
                continue
            response = await client.get(path, headers=headers, params=spec.get("params"))
            assert response.status_code == 200, f"{method} {path} -> {response.status_code}"
            leaked = _strings_in(response.json()) & forbidden
            assert not leaked, f"{method} {path} leaked {leaked} from the other community"

    async def test_a_device_of_b_is_not_reachable_by_id_from_a(
        self, client, db_session, two_communities
    ):
        """The by-id path, which a list-only probe cannot reach. 404 rather than
        403: telling A that B's device exists is itself a disclosure."""
        a, b = two_communities
        a_community, b_device_id = a[0], b[2]
        public_id = await db_session.scalar(
            repository_module.select(repository_module.DeviceModel.public_id).where(
                repository_module.DeviceModel.id == b_device_id
            )
        )
        headers = gateway_headers(a_community.auth_community_id, role="MANAGER")
        response = await client.post(f"/devices/{public_id}/token", headers=headers)
        assert response.status_code == 404

    async def test_an_operation_of_b_is_not_reachable_from_a(self, client, two_communities):
        """By id, as A's MANAGER and as A's member: 404 both times - the same
        answer an operation that does not exist gets. Telling A that B's
        operation exists is itself a disclosure."""
        a, b = two_communities
        a_community, a_operation, b_operation = a[0], a[3], b[3]
        manager = gateway_headers(a_community.auth_community_id, role="MANAGER")
        member = gateway_headers(a_community.auth_community_id, role="MEMBER")

        for url in (f"/operations/{b_operation}/series", f"/operations/{b_operation}/summary"):
            response = await client.get(url, headers=manager, params={"resolution": "hour"})
            assert response.status_code == 404, f"{url} -> {response.status_code}"
            assert response.json()["error_code"] == 2446
        mine = await client.get(
            f"/mine/operations/{b_operation}/series",
            headers=member,
            params={"resolution": "hour"},
        )
        assert mine.status_code == 404
        # POSITIVE CONTROL: A's own operation answers, or the 404s prove nothing.
        own = await client.get(
            f"/operations/{a_operation}/series", headers=manager, params={"resolution": "hour"}
        )
        assert own.status_code == 200

    async def test_every_route_is_403_not_subscribed_while_switched_off(
        self, client, db_session, switched_off_community
    ):
        """STRICT PARITY (D-12): switched off, EVERY route answers 403 with
        `error_code` 1003 - reads, writes, the ops surface and revocation alike,
        with no carve-out.

        Driven off `PROBE`, and `test_every_route_has_a_probe` makes `PROBE`
        equal to the app's routes, so a route added later is covered here
        without anyone remembering to. Both kinds of "off": no row at all, and
        the `is_active = false` row crm-backend's unsubscribe actually leaves -
        a gate written as "a row exists" passes the first and not the second.
        """
        community = switched_off_community
        headers = gateway_headers(community.auth_community_id, role="MANAGER")

        for (path, method), spec in sorted(PROBE.items()):
            url = _url(path, spec)
            response = await client.request(
                method, url, headers=headers, params=spec.get("params"), json=spec.get("json")
            )
            assert response.status_code == 403, f"{method} {path} -> {response.status_code}"
            assert response.json()["error_code"] == 1003, f"{method} {path}: {response.json()}"

        # And no write got through before the gate: the POST and the PUT are in
        # the table above precisely because a read-only probe cannot see this.
        for table in ("device", "community_live_settings"):
            written = await db_session.scalar(
                text(f"SELECT count(*) FROM {table} WHERE id_community = :c"),  # noqa: S608
                {"c": community.id},
            )
            assert written == 0, f"a switched-off community wrote to {table}"


# ---------------------------------------------------------------------------
# Layer 3 - negative controls
# ---------------------------------------------------------------------------


class TestTheGuardsCanFail:
    """Precedent: `test_public_surface.py::test_the_guard_can_fail` and
    `test_worker_import_graph.py::test_the_probe_can_fail`."""

    def test_the_ast_walker_catches_an_unscoped_method(self):
        """Run the real checker against code that must fail it.

        Without this, a refactor that broke `_called_names` - a rename, a change
        to how the repository builds statements - would leave
        `test_every_select_in_LiveRepository_is_scoped` reporting success over an
        empty set of findings, for ever.
        """
        offending = ast.parse(
            "class LiveRepository:\n"
            "    async def leaky(self):\n"
            "        stmt = select(DeviceModel)\n"
            "        return await self._session.execute(stmt)\n"
        )
        assert unscoped_reads(offending, "LiveRepository") == ["leaky"]

    def test_the_ast_walker_accepts_a_scoped_method(self):
        """The other direction: a checker that flagged everything would also pass
        the test above while making the real assertion unsatisfiable."""
        clean = ast.parse(
            "class LiveRepository:\n"
            "    async def tidy(self):\n"
            "        stmt = _scoped(select(DeviceModel), DeviceModel)\n"
            "        return await self._session.execute(stmt)\n"
        )
        assert unscoped_reads(clean, "LiveRepository") == []

    def test_the_probe_table_notices_a_new_route(self):
        """A throwaway app carrying a route the table does not know about. The
        real `PROBE` check is equality against `app`; this proves the comparison
        would actually catch an addition."""
        probe_app = FastAPI()

        @probe_app.get("/newly-added")
        async def _newly_added():  # pragma: no cover - never called
            return {}

        assert tenant_routes_of(probe_app) == {("/newly-added", "GET")}
        assert tenant_routes_of(probe_app) != set(PROBE)


class TestScopedFailsClosed:
    """`_scoped`'s OWN behaviour, proven where no other gate can produce it.

    Through HTTP, a request with no resolvable community is refused by
    `require_feature` before the repository is reached - so an HTTP test proves
    nothing about `_scoped`. These call it with the ContextVar unset.
    """

    async def test_a_missing_tenant_is_a_403_not_an_empty_result(self, db_session: AsyncSession):
        """The difference that matters for an AGGREGATE. `where(false())` would
        make `SUM(production_wh)` return NULL, which this service would render as
        a summary saying the community produced nothing - indistinguishable from
        night, and arriving with a 200."""
        token = current_internal_community_id.set(None)
        try:
            repo = repository_module.LiveRepository(db_session)
            with pytest.raises(ErrorException) as caught:
                await repo.latest_closed_community_hour()
            assert caught.value.status_code == 403
        finally:
            current_internal_community_id.reset(token)

    async def test_the_rollup_watermarks_are_also_a_403(self, db_session: AsyncSession):
        """Three aggregates. Over no rows each is NULL, which the freshness
        verdict reads as "never rolled up" - a calm answer to a request that
        should have been refused."""
        token = current_internal_community_id.set(None)
        try:
            repo = repository_module.LiveRepository(db_session)
            with pytest.raises(ErrorException) as caught:
                await repo.rollup_watermarks()
            assert caught.value.status_code == 403
        finally:
            current_internal_community_id.reset(token)

    async def test_the_device_list_is_also_a_403(self, db_session: AsyncSession):
        """Migrated from `with_community_scope` in build step 6. It used to
        return an empty list, which reads as "you have no devices" rather than as
        a failure - plan section 18 row 31's "one mechanism, not two"."""
        token = current_internal_community_id.set(None)
        try:
            repo = repository_module.LiveRepository(db_session)
            with pytest.raises(ErrorException) as caught:
                await repo.list_devices()
            assert caught.value.status_code == 403
        finally:
            current_internal_community_id.reset(token)
