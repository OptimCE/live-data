"""The unauthenticated surface is EXACTLY what this service intends.

`live-public` is the platform's first public service entry: all 191 pre-existing
gateway endpoints carry an `auth/validator` and these do not. There is therefore
no existing signal - no failing build, no gateway error, no log line - for a
route that becomes public by accident.

Three independent gates exist, and this file is one of them:

  1. scripts/export_openapi.py asserts the public OPERATION SET equals a
     hard-coded literal, and refuses to write the specs otherwise.
  2. THIS FILE asserts the public routes' DEPENDENCY TREES are clean, and - in
     the inverse direction - that nothing else is dependency-free.
  3. The monorepo's scripts/verify-krakend-public-surface.py asserts that
     exactly one endpoint in the generated krakend.json lacks `extra_config`.

Each catches something the others cannot. (1) is about intent, (2) about
behaviour, (3) about what the gateway actually publishes.

----------------------------------------------------------------------------
WHY THESE TESTS MATCH ON __qualname__ AND NOT ON IDENTITY.

`require_feature(...)` and `require_min_role(...)` are FACTORIES: each call
returns a fresh closure. So

    d.call is require_feature          # always False
    d.call is require_min_role(...)    # always False, even for the same role

An identity check therefore passes green while inspecting nothing at all - the
precise shape of "a test that cannot fail". The closures are named
`require_feature.<locals>._check`, which is stable and is what is matched below.

`test_the_guard_can_fail` proves the matcher by building a route that SHOULD be
caught and asserting it is.
----------------------------------------------------------------------------
"""

from fastapi import Depends, FastAPI
from fastapi.routing import APIRoute

from api.live_public.routes import PUBLIC_OPERATIONS, PUBLIC_TAG
from core.security.community_scope import resolve_internal_community
from core.security.dependencies import require_feature, require_min_role
from core.security.user_context import Role
from main import app
from shared.const import FeatureName

# The qualified names of every dependency that makes a route community-scoped or
# role-gated. Matched by name because they are freshly-built closures.
_TENANT_DEPENDENCY_QUALNAMES = frozenset(
    {
        "require_feature.<locals>._check",
        "require_min_role.<locals>._check",
        "require_community",
        "require_authenticated",
        "resolve_internal_community",
    }
)


def _dependency_qualnames(route: APIRoute) -> set[str]:
    """Every callable in a route's dependency tree, by qualified name.

    Walks recursively: a dependency carried on the ROUTER shows up nested inside
    the route's dependant, not at the top level.
    """
    found: set[str] = set()

    def _walk(dependant) -> None:
        call = getattr(dependant, "call", None)
        if call is not None:
            found.add(getattr(call, "__qualname__", getattr(call, "__name__", "")))
        for sub in getattr(dependant, "dependencies", []):
            _walk(sub)

    _walk(route.dependant)
    return found


def _api_routes(application: FastAPI) -> list[APIRoute]:
    return [r for r in application.routes if isinstance(r, APIRoute)]


def _public_routes(application: FastAPI) -> list[APIRoute]:
    return [r for r in _api_routes(application) if PUBLIC_TAG in (r.tags or [])]


def _operations(routes: list[APIRoute]) -> set[tuple[str, str]]:
    return {(r.path, m.lower()) for r in routes for m in (r.methods or set()) if m != "HEAD"}


class TestThePublicSurfaceIsExactlyWhatWeIntend:
    def test_the_public_operation_set_equals_the_declared_literal(self):
        """Equality, not a subset check.

        A subset check passes when a route is MISSING, and equality is what
        catches the likelier mistake in the other direction: a route added to
        `live_public_routes` and published with no auth at all.
        """
        assert _operations(_public_routes(app)) == set(PUBLIC_OPERATIONS)

    def test_rotate_is_not_registered(self):
        """protocol 5.4 freezes rotate's SHAPES and defers the ENDPOINT.

        Asserted explicitly rather than left implicit in the set above, because
        the temptation is specific: a 501 stub "so connectors can probe it". A
        501 at a real path is still a gin route, an nginx location, a rate-limit
        zone, and a public username/password oracle.
        """
        paths = {r.path for r in _api_routes(app)}
        assert "/enroll/rotate" not in paths


class TestNoPublicRouteIsCommunityScoped:
    def test_public_routes_carry_no_tenant_dependency(self):
        """Client-supplied `x-user-*` REACHES this handler.

        `auth: false` removes the PRODUCER of those headers, not the gateway's
        input-header allow-list - and `input_headers` is global in the generator
        with no per-service override, so the gateway genuinely cannot strip them.
        `GatewayScopeMiddleware` then reads them unconditionally.

        nginx blanks the five headers on `= /api/live-public/enroll`. This test
        is the second lock: even if that location were dropped, no public route
        may consult a tenant the caller supplied.
        """
        for route in _public_routes(app):
            offenders = _dependency_qualnames(route) & _TENANT_DEPENDENCY_QUALNAMES
            assert not offenders, (
                f"{route.path} is PUBLIC and depends on {sorted(offenders)}. "
                "On this leg the community and the user are attacker-controlled."
            )

    def test_every_non_public_route_is_tenant_scoped(self):
        """THE INVERSE, and the half that actually catches the next mistake.

        The forward test passes trivially if a route simply never reaches the
        public router. What catches a new authenticated route that forgot its
        gating - or one quietly added to a router with no dependencies - is
        asserting that the dependency-free set is EXACTLY the public set.
        """
        for route in _api_routes(app):
            if route.path.startswith("/health"):
                continue  # never reaches the gateway; filtered from the specs
            is_public = PUBLIC_TAG in (route.tags or [])
            scoped = bool(_dependency_qualnames(route) & _TENANT_DEPENDENCY_QUALNAMES)
            assert scoped is not is_public, (
                f"{route.path}: public={is_public} but tenant-scoped={scoped}. "
                "Every authenticated route must be scoped, and only the declared "
                "public routes may be unscoped."
            )

    def test_the_guard_can_fail(self):
        """NEGATIVE CONTROL - proves the matcher inspects something.

        Build a route carrying the very dependencies a public route must not
        have, and assert the matcher catches them. Without this, an identity
        comparison (`d.call is require_feature`) would report success on every
        route for ever, because these are freshly-built closures.
        """
        probe = FastAPI()

        @probe.get(
            "/would-be-public",
            dependencies=[
                Depends(resolve_internal_community),
                Depends(require_feature(FeatureName.LIVE_DATA)),
                Depends(require_min_role(Role.MANAGER)),
            ],
        )
        async def _would_be_public():  # pragma: no cover - never called
            return {}

        route = _api_routes(probe)[0]
        caught = _dependency_qualnames(route) & _TENANT_DEPENDENCY_QUALNAMES
        assert "require_feature.<locals>._check" in caught
        assert "require_min_role.<locals>._check" in caught
        assert "resolve_internal_community" in caught
