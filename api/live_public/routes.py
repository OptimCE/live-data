"""The UNAUTHENTICATED `live-public` surface.

This is the platform's first public service entry. All 191 pre-existing gateway
endpoints carry an `auth/validator`; these do not. Read the whole docstring
before adding anything here.

----------------------------------------------------------------------------
DO NOT ADD `from __future__ import annotations`. See api/live/routes.py - and it
bites harder here, because a 422 on the only public route has no obvious owner.
----------------------------------------------------------------------------

WHY THIS IS A SEPARATE ROUTER AND A SEPARATE OPENAPI FILE

KrakenD gates the JWT validator PER SERVICE ENTRY, not per endpoint. So "one
public route among authenticated ones" is not expressible: the public routes need
their own `services:` key with `auth: false`, their own swagger file, and their
own namespace. Hence two routers here, two specs out of
`scripts/export_openapi.py`, and two entries in `krakend-builder.yaml`.

The tag below is what partitions the two specs. It is set in exactly one place -
`main.py`'s `include_router(..., tags=[PUBLIC_TAG])` - and `export_openapi.py`
asserts the resulting public operation set EQUALS a hard-coded literal.

That equality check matters more than a disjointness check, and the reason is
counter-intuitive. A path appearing in BOTH specs does not collide: the service
prefixes differ, so it yields `/live/X` and `/live-public/X`, two distinct gin
routes, and `krakend check -tnc` exits 0. What actually happens is that an
authenticated-intent route gets PUBLISHED UNAUTHENTICATED - and the likelier
mistake, a route placed only on the public router, passes any "claimed by exactly
one router" test cleanly. Only equality against a literal catches both.

----------------------------------------------------------------------------
THREE TRAPS ON THIS LEG, ALL DEMONSTRATED IN PHASE 0

1.  CLIENT-SUPPLIED `x-user-*` REACHES THIS HANDLER.

    `auth: false` removes the PRODUCER of those headers, not the input-header
    allow-list. `input_headers` is GLOBAL in the generator (one list written onto
    all 208 endpoints, `cli.py:54` -> `parser.py:161`) with no per-service
    override, so the gateway CANNOT strip them - removing `x-user-id` would strip
    it from all 191 authenticated endpoints too.

    `GatewayScopeMiddleware` reads them unconditionally, on every request,
    including this one. The only thing that blanks them is an exact-match nginx
    location (`= /api/live-public/enroll`, in BOTH templates), and
    `tests/test_public_surface.py` asserts no route here has a community-scoped
    dependency.

    So: NOTHING in this module may read `current_user_id`,
    `current_community_id`, `current_internal_community_id` or
    `current_user_role`. They are attacker-controlled here.

2.  `require_feature` CANNOT BE USED.

    It calls `require_community()` and reads `community_subscription` scoped to
    `X-Community-ID` - and this request has neither a community header nor a
    user. Worse than merely not working: `with_community_scope` returns
    `stmt.where(false())` when the tenant ContextVar is unset, so the naive
    implementations fail SILENTLY IN BOTH DIRECTIONS - either every enrolment is
    rejected, or (if the check is written as "reject only when a row is found")
    every enrolment from a lapsed community succeeds.

    The subscription is therefore resolved from the DEVICE ROW THE TOKEN POINTS
    AT, with the community id passed as an explicit bind parameter, in a query
    named `_unscoped`.

3.  RATE LIMITING LIVES IN NGINX, NOT HERE.

    This service cannot see a client IP: KrakenD's input-header allow-list
    excludes `X-Forwarded-For` and `X-Real-IP`, and nothing sets `x-source-ip`,
    so `request.client.host` is KrakenD's container address for every caller. A
    per-row attempt counter would be useless anyway - a guessed token never finds
    a row. The only meaningful application signal would be a GLOBAL
    token-not-found counter, and THERE IS STILL NOT ONE.

    This paragraph has now been wrong in both directions. It first claimed the
    counter was "emitted below" when no instrument existed anywhere, which is
    worse than the absence itself: a reader checking whether enrolment brute
    force is observable stops here and concludes it is. It was then corrected to
    say `core/metrics.py` holds a single instrument used by the health routes -
    true when written, and false since the ingest worker and the scheduler were
    instrumented. `core/metrics.py` now carries fifteen, and NONE of them is on
    this path: the enrolment leg runs in the API container, which was out of
    scope for that change.

    So nginx's `limit_req_zone live_enroll` remains the only real defence and its
    log the only signal. Adding the counter is a small, self-contained piece of
    work - not a refactor of anything here.
----------------------------------------------------------------------------
"""

from typing import Annotated

from fastapi import APIRouter, Depends

from api.live.deps import get_enrolment_service
from api.live.mappers import to_enroll_response
from api.live_public.schemas import EnrollRequest, EnrollResponse
from api.live_public.service import EnrolmentService
from core.api_response import ApiResponse
from core.errors.with_default_error import with_default_error
from shared.custom_errors import errors

# The single place the public tag is spelled. Both the spec splitter and the
# surface tests import THIS constant rather than repeating the string.
PUBLIC_TAG = "Live public"

# THE COMPLETE, INTENDED UNAUTHENTICATED SURFACE. (path, method-lowercase).
#
# Declared here beside the router rather than in scripts/, so that the intent
# lives with the code it describes and both the build-time gate
# (scripts/export_openapi.py, which refuses to write the specs on a mismatch)
# and the test-time gate (tests/test_public_surface.py) read one literal.
#
# Build steps 1-5 ship exactly one public operation. `POST /enroll/rotate` is
# NOT here and must not be: protocol 5.4 freezes its SHAPES now and defers the
# ENDPOINT to phase 1.5, because a route - even a 501 - is still a gin entry,
# an nginx location, a rate-limit zone, and a public username/password oracle.
#
# Widening this is a deliberate, reviewable act, and belongs in the same change
# as the nginx header-blanking location and the rate-limit zone for the new path.
PUBLIC_OPERATIONS: frozenset[tuple[str, str]] = frozenset({("/enroll", "post")})

# No router-level dependencies, deliberately - and the emptiness is asserted by
# tests/test_public_surface.py rather than left to reading. Every dependency the
# authenticated router carries would either fail closed or fail open here.
live_public_routes = APIRouter()

ServiceDep = Annotated[EnrolmentService, Depends(get_enrolment_service)]


@live_public_routes.post("/enroll", response_model=ApiResponse[EnrollResponse])
@with_default_error(errors.live.ENROLMENT_FAILED)
async def enroll(payload: EnrollRequest, service: ServiceDep) -> ApiResponse[EnrollResponse]:
    """Exchange a one-time token for broker credentials. protocol 5.1.

    ONE CALL, because the device may be an ESP32 being configured through a
    captive portal by someone holding a phone in a basement. Everything a
    connector needs comes back in a single response, and it stores that in NVS
    and never asks again.

    THE PASSWORD IS SHOWN ONCE AND IS NEVER RECOVERABLE. OptimCE does not store
    it; the broker keeps only a hash. A lost secret means re-enrolment with a
    fresh token, which is a normal operation rather than an incident.

    A consumed or expired token returns 4xx and NO broker credentials are
    created - the ordering rule in `api/live_public/service.py` is what
    guarantees that, not this handler.

    Note what this signature does NOT take: no community, no user, no request.
    Everything tenant-shaped is discovered from the device row the token points
    at. On this leg the `x-user-*` headers are attacker-supplied.
    """
    result = await service.enrol(
        supplied_token=payload.token,
        connector_name=payload.connector.name,
        connector_version=payload.connector.version,
    )
    return ApiResponse(data=to_enroll_response(result))
