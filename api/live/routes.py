"""The authenticated `live` surface.

----------------------------------------------------------------------------
DO NOT ADD `from __future__ import annotations` TO THIS MODULE.

`with_default_error` resolves string annotations against its OWN module globals.
With PEP 563 on, a Pydantic body type becomes a string FastAPI cannot resolve,
so it is demoted to a QUERY parameter and every POST 422s with
`loc: [query, body]` while the body is never parsed. A linter autofix or a
copy-paste from a module that has it is how it arrives.
----------------------------------------------------------------------------

NO `prefix=` ON THE ROUTER, AND ABSOLUTE PATHS.

KrakenD's generator sets `url_pattern` to the path WITHOUT the service prefix
(`parser.py:218`): the gateway endpoint is `/live/devices`, the backend call is
`/devices`. A router declared `prefix="/live"` would 404 every route while
`krakend.json` looked perfect - and the gateway would report it as a backend
error, not as a routing mistake.

The `/live` namespace comes from the builder KEY being literally `live`
(`cli.py:94-96`: `"" if key == "root" else f"/{key}"`), with no `prefix:` entry
on either service. That is D-2, and it is what makes a `(path, method)`
collision between the two live services structurally impossible - the subtrees
are disjoint.
"""

import datetime
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query

from api.live.deps import get_live_data_service, get_live_read_service
from api.live.mappers import to_device_out, to_token_out
from api.live.read_service import LiveReadService, window_error_to_http
from api.live.repository import tenant_id
from api.live.schemas import (
    DeviceCreate,
    DeviceOut,
    DeviceStatusOut,
    EnrollmentTokenOut,
    ForecastMethodOut,
    ForecastOut,
    LiveSettingsOut,
    LiveSettingsUpdate,
    MemberOperationOut,
    MemberOperationSeriesOut,
    OperationOut,
    OperationSummaryOut,
    OpsHealthOut,
    SeriesOut,
    SummaryOut,
    VersionOut,
)
from api.live.service import LiveDataService
from core.api_response import ApiResponse
from core.config import settings
from core.errors.with_default_error import with_default_error
from core.security.community_scope import resolve_internal_community
from core.security.dependencies import require_feature, require_min_role
from core.security.user_context import Role
from domain.windows import WindowError, resolve_window
from shared.const import LOCAL_SCHEMA_VERSION, PROTOCOL_VERSION, FeatureName
from shared.custom_errors import errors

# Router-level dependencies, so a route cannot opt out by forgetting them:
#   resolve_internal_community  maps the Keycloak org id in X-Community-ID to the
#                               internal community.id that with_community_scope
#                               filters on. Without it every scoped SELECT
#                               silently matches nothing.
#   require_feature             403 NOT_SUBSCRIBED when the community has no
#                               active `live-data` subscription. It runs BEFORE
#                               any repository call and fails closed, which is
#                               what makes the ambient-tenant pattern safe here.
live_routes = APIRouter(
    dependencies=[
        Depends(resolve_internal_community),
        Depends(require_feature(FeatureName.LIVE_DATA)),
    ]
)

manager_only = Depends(require_min_role(Role.MANAGER))

# D-14 (2026-10-04) lifted D-5 and drew the member's line: a member sees ONLY the
# sharing operation(s) they hold a meter in - never the community total, never
# another operation. So `/summary`, `/series` and `/forecast`, the COMMUNITY
# reads, are manager-only now, and the member floor carries exactly the `/mine`
# routes.
#
# Those still run the visibility gate (`require_aggregate_visible`), so plan 14
# criterion 8 - a member of a community with production visibility disabled
# gets a 403 - is still produced by the SETTING and not by the role gate, which
# is the whole reason a member-floor route must exist for it to be testable.
member_floor = Depends(require_min_role(Role.MEMBER))

# A real CRM operation id: identity columns start at 1, and 0 is the REMAINDER
# row of the operation rollups, which is never addressable as an operation.
OperationId = Annotated[int, Path(gt=0)]

ServiceDep = Annotated[LiveDataService, Depends(get_live_data_service)]
ReadDep = Annotated[LiveReadService, Depends(get_live_read_service)]


@live_routes.get("/version", response_model=ApiResponse[VersionOut], dependencies=[manager_only])
@with_default_error(errors.live.GET_SETTINGS)
async def get_version() -> ApiResponse[VersionOut]:
    """Which protocol and schema this deployment speaks.

    Touches no database on purpose: it is the probe that proves the whole
    authenticated chain is wired - gateway JWT validation, the `x-user-*`
    propagation, GatewayScopeMiddleware, community resolution, the subscription
    check and the role gate - without depending on anything that could fail for a
    different reason.

    `schema_version` is the constant this build expects, NOT what the database
    reports; comparing the two is `/health/readiness`'s job.
    """
    return ApiResponse(
        data=VersionOut(
            service="live-data",
            protocol_version=PROTOCOL_VERSION,
            schema_version=LOCAL_SCHEMA_VERSION,
        )
    )


@live_routes.get(
    "/devices", response_model=ApiResponse[list[DeviceOut]], dependencies=[manager_only]
)
@with_default_error(errors.live.GET_DEVICES)
async def list_devices(service: ServiceDep) -> ApiResponse[list[DeviceOut]]:
    """Every device in the caller's community.

    The list read that the `with_community_scope` chokepoint exists for. A
    tenancy bug shows up HERE - as another community's meters - long before it
    would show up on a by-id lookup.
    """
    rows = await service.list_devices()
    return ApiResponse(data=[to_device_out(row) for row in rows])


@live_routes.post("/devices", response_model=ApiResponse[DeviceOut], dependencies=[manager_only])
@with_default_error(errors.live.CREATE_DEVICE)
async def create_device(body: DeviceCreate, service: ServiceDep) -> ApiResponse[DeviceOut]:
    """Create a device and mint its first enrolment token.

    Returns the DEVICE. The token comes back from `POST /devices/{id}/token`,
    which is a separate call on purpose: for a consumption device the token goes
    to the MEMBER through their own account, never to the administrator, and
    keeping it off the creation response makes that split the default rather
    than something to remember.

    200, not 201: no route in this codebase sets a non-200 success code.
    """
    device, _ = await service.create_device(
        id_community=tenant_id(),
        name=body.name,
        ean=body.ean,
        device_type=body.type,
        pure_injection=body.pure_injection,
        ttl_hours=settings.ENROLMENT_TOKEN_TTL_HOURS,
    )
    return ApiResponse(data=to_device_out(device))


@live_routes.post(
    "/devices/{device_id}/token",
    response_model=ApiResponse[EnrollmentTokenOut],
    dependencies=[manager_only],
)
@with_default_error(errors.live.ISSUE_TOKEN)
async def issue_token(device_id: uuid.UUID, service: ServiceDep) -> ApiResponse[EnrollmentTokenOut]:
    """A fresh enrolment token, shown ONCE.

    Invalidates any unconsumed token for this device. Regenerating is a normal
    operation rather than an incident: the device password is shown once and is
    never recoverable, so a lost secret is re-enrolled, not recovered.
    """
    issued = await service.reissue_token(device_id, ttl_hours=settings.ENROLMENT_TOKEN_TTL_HOURS)
    return ApiResponse(data=to_token_out(issued))


@live_routes.post(
    "/devices/{device_id}/revoke",
    response_model=ApiResponse[DeviceOut],
    dependencies=[manager_only],
)
@with_default_error(errors.live.REVOKE_DEVICE)
async def revoke_device(device_id: uuid.UUID, service: ServiceDep) -> ApiResponse[DeviceOut]:
    """Revoke a device: disable, clear its retained status, delete.

    Immediate and server-side. There is nothing to do on the device itself -
    from its point of view its password simply stops being accepted - and
    revoking does NOT delete what it already sent.
    """
    device = await service.revoke_device(device_id)
    return ApiResponse(data=to_device_out(device))


# ---------------------------------------------------------------------------
# The read surface (build step 6)
#
# `response_model_exclude_none=True` on the two aggregate routes, and it is part
# of the contract rather than a serialisation preference: a term withheld by the
# k threshold must be ABSENT FROM THE JSON, not null. `null` is what a chart
# library renders as zero, so a nulled `import_wh` does not read as "withheld",
# it reads as "the community imported nothing" - a claim nobody made, on the one
# field that was deliberately not answered. The `absent` list names each one.
# ---------------------------------------------------------------------------


@live_routes.get(
    "/summary",
    response_model=ApiResponse[SummaryOut],
    response_model_exclude_none=True,
    dependencies=[manager_only],
)
@with_default_error(errors.live.GET_SUMMARY)
async def get_summary(service: ReadDep) -> ApiResponse[SummaryOut]:
    """The community as of the last CLOSED hour, plus live device counts."""
    return ApiResponse(data=await service.summary(now=datetime.datetime.now(datetime.UTC)))


@live_routes.get(
    "/series",
    response_model=ApiResponse[SeriesOut],
    response_model_exclude_none=True,
    dependencies=[manager_only],
)
@with_default_error(errors.live.GET_SERIES)
async def get_series(
    service: ReadDep,
    resolution: str | None = None,
    date_from: Annotated[datetime.datetime | None, Query(alias="from")] = None,
    date_to: Annotated[datetime.datetime | None, Query(alias="to")] = None,
) -> ApiResponse[SeriesOut]:
    """A time series at quarter, hour or day resolution.

    `from`/`to` are OPTIONAL and must be SNAPPED to the requested grid when
    given - an unsnapped bound is a 422, never a silent correction. plan 9.3:
    free-form bounds are the differencing attack, and snapping them silently
    answers every request in it.

    Aliased because `from` is a Python keyword; the wire name is what the SPA and
    the two other protocol implementers see, and it is `from`.
    """
    return ApiResponse(data=await service.series(_window(resolution, date_from, date_to)))


@live_routes.get(
    "/settings", response_model=ApiResponse[LiveSettingsOut], dependencies=[manager_only]
)
@with_default_error(errors.live.GET_SETTINGS)
async def get_settings(service: ReadDep) -> ApiResponse[LiveSettingsOut]:
    """The visibility settings in force.

    Returns the platform defaults with `is_default: true` when no row exists, and
    DOES NOT CREATE ONE. A GET that writes breaks on a read replica, audits a
    manager who merely opened a panel, and freezes today's default into a row so
    that a later change to the platform default silently does not reach them.
    """
    return ApiResponse(data=await service.get_settings())


@live_routes.put(
    "/settings", response_model=ApiResponse[LiveSettingsOut], dependencies=[manager_only]
)
@with_default_error(errors.live.UPDATE_SETTINGS)
async def put_settings(body: LiveSettingsUpdate, service: ReadDep) -> ApiResponse[LiveSettingsOut]:
    """Replace the visibility settings. Audited with before AND after."""
    return ApiResponse(data=await service.update_settings(body))


def _window(
    resolution: str | None,
    date_from: datetime.datetime | None,
    date_to: datetime.datetime | None,
):
    """The one place a series window is resolved: snapped bounds or a 422."""
    try:
        return resolve_window(
            now=datetime.datetime.now(datetime.UTC),
            resolution=resolution,
            start=date_from,
            end=date_to,
        )
    except WindowError as exc:
        raise window_error_to_http(exc) from exc


# ---------------------------------------------------------------------------
# Sharing operations (D-14)
#
# The MANAGER's per-operation view under `/operations`, and the MEMBER's own
# operations under `/mine` - separate routes and separate DTOs, so "can a member
# reach the import curve?" is a question about which routes exist rather than
# about a branch inside one. An operation that does not exist, belongs to another
# community, or is not one the member holds answers the same 404.
# ---------------------------------------------------------------------------


@live_routes.get(
    "/operations", response_model=ApiResponse[list[OperationOut]], dependencies=[manager_only]
)
@with_default_error(errors.live.GET_OPERATIONS)
async def list_operations(service: ReadDep) -> ApiResponse[list[OperationOut]]:
    """The community's operations with a monitored meter, and their coverage."""
    return ApiResponse(data=await service.operations(now=datetime.datetime.now(datetime.UTC)))


@live_routes.get(
    "/operations/{operation_id}/summary",
    response_model=ApiResponse[OperationSummaryOut],
    response_model_exclude_none=True,
    dependencies=[manager_only],
)
@with_default_error(errors.live.GET_SUMMARY)
async def get_operation_summary(
    operation_id: OperationId, service: ReadDep
) -> ApiResponse[OperationSummaryOut]:
    """One operation as of its last CLOSED hour, under the operation's own k."""
    return ApiResponse(
        data=await service.operation_summary(operation_id, now=datetime.datetime.now(datetime.UTC))
    )


@live_routes.get(
    "/operations/{operation_id}/series",
    response_model=ApiResponse[SeriesOut],
    response_model_exclude_none=True,
    dependencies=[manager_only],
)
@with_default_error(errors.live.GET_SERIES)
async def get_operation_series(
    operation_id: OperationId,
    service: ReadDep,
    resolution: str | None = None,
    date_from: Annotated[datetime.datetime | None, Query(alias="from")] = None,
    date_to: Annotated[datetime.datetime | None, Query(alias="to")] = None,
) -> ApiResponse[SeriesOut]:
    """One operation's series, with the ESTIMATED shared energy, per bucket k."""
    window = _window(resolution, date_from, date_to)
    return ApiResponse(data=await service.operation_series(operation_id, window))


@live_routes.get(
    "/mine/operations",
    response_model=ApiResponse[list[MemberOperationOut]],
    dependencies=[member_floor],
)
@with_default_error(errors.live.GET_OPERATIONS)
async def list_my_operations(service: ReadDep) -> ApiResponse[list[MemberOperationOut]]:
    """The operations the caller holds an ACTIVE meter in. 200 + [] when none."""
    return ApiResponse(data=await service.my_operations(now=datetime.datetime.now(datetime.UTC)))


@live_routes.get(
    "/mine/operations/{operation_id}/series",
    response_model=ApiResponse[MemberOperationSeriesOut],
    response_model_exclude_none=True,
    dependencies=[member_floor],
)
@with_default_error(errors.live.GET_SERIES)
async def get_my_operation_series(
    operation_id: OperationId,
    service: ReadDep,
    resolution: str | None = None,
    date_from: Annotated[datetime.datetime | None, Query(alias="from")] = None,
    date_to: Annotated[datetime.datetime | None, Query(alias="to")] = None,
) -> ApiResponse[MemberOperationSeriesOut]:
    """Production (and, for net meters, export under k) of one of MY operations."""
    window = _window(resolution, date_from, date_to)
    return ApiResponse(
        data=await service.my_operation_series(
            operation_id, window, now=datetime.datetime.now(datetime.UTC)
        )
    )


# ---------------------------------------------------------------------------
# The forecast seam (build step 9)
#
# Both routes ship in phase 1 with no method behind them, and both answer with
# their REAL shape. plan 11.1: "It is present from the start so that the contract
# is fixed before the frontend is written, and so the 'no method' state is
# exercised rather than discovered later."
# ---------------------------------------------------------------------------


@live_routes.get("/forecast", response_model=ApiResponse[ForecastOut], dependencies=[manager_only])
@with_default_error(errors.live.GET_FORECAST)
async def get_forecast(service: ReadDep) -> ApiResponse[ForecastOut]:
    """Production forecast. Empty with a NAMED reason until a method exists."""
    return ApiResponse(data=await service.forecast())


@live_routes.get(
    "/forecast/methods",
    response_model=ApiResponse[list[ForecastMethodOut]],
    dependencies=[manager_only],
)
@with_default_error(errors.live.GET_FORECAST)
async def get_forecast_methods(service: ReadDep) -> ApiResponse[list[ForecastMethodOut]]:
    """The registered methods and their JSON-Schema parameter shapes.

    Manager-only: it describes how the platform is configured, not the
    community's energy. Correctly `[]` in phase 1.
    """
    return ApiResponse(data=await service.forecast_methods())


# ---------------------------------------------------------------------------
# Ops (build step 11)
#
# `response_model_exclude_none=True` here as well, for a different reason than on
# the aggregates: an unanswered field is ABSENT on every read route of this
# service, `DeviceStatusOut` documents its coarsened timestamps as "OMITTED", and
# the SPA's types are written for absence. Without it these two routes sent
# `"rollup_age_minutes": null` for a community with nothing rolled up yet, the
# SPA's `!== undefined` let the null through, and the Ops tab read "recomputed
# null minutes ago".
# ---------------------------------------------------------------------------


@live_routes.get(
    "/ops/health",
    response_model=ApiResponse[OpsHealthOut],
    response_model_exclude_none=True,
    dependencies=[manager_only],
)
@with_default_error(errors.live.GET_OPS_HEALTH)
async def get_ops_health(service: ReadDep) -> ApiResponse[OpsHealthOut]:
    """The fleet, its rollup freshness, and what could not be stored.

    MANAGER, not member: it is a maintenance view of other people's hardware.
    """
    return ApiResponse(data=await service.ops_health(now=datetime.datetime.now(datetime.UTC)))


@live_routes.get(
    "/devices/{device_id}/diagnostics",
    response_model=ApiResponse[DeviceStatusOut],
    response_model_exclude_none=True,
    dependencies=[manager_only],
)
@with_default_error(errors.live.GET_DIAGNOSTICS)
async def get_device_diagnostics(
    device_id: uuid.UUID, service: ReadDep
) -> ApiResponse[DeviceStatusOut]:
    """One device's state, with the hint that goes with it.

    The `hint` is an i18n KEY, not a sentence: `LIVE.HINT.P1_NOT_ENABLED` for a
    device that is connected and reporting zeros, which section 11.2 calls the
    single most likely cause of a silent meter and which nobody guesses from the
    outside - the hardware is fine, the wiring is fine, and the port is closed.
    """
    return ApiResponse(
        data=await service.diagnostics(device_id, now=datetime.datetime.now(datetime.UTC))
    )
