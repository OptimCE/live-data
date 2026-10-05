"""DTOs for the authenticated `live` surface.

snake_case fields, enums sent as their integer values, timestamps as ISO-8601.
Statuses are never writable directly.

Scope note: build step 6 added the READ surface below the device DTOs -
`SummaryOut`, `SeriesOut`, `LiveSettingsOut`. They were deliberately withheld
through steps 1-5 rather than guessed at, because `SeriesOut`'s
`suppressed_buckets`/`truncated`/`cap` encode 9.3's per-bucket k-suppression and
freezing a guess there would have given step 10 something wrong to build against.

`ForecastOut` lives in `api/live/forecast_schemas.py` (step 9) and
`DiagnosticsOut` below arrives with step 11.

THE ABSENT-TERM RULE, which applies to every payload here.
There is no `consumption: null` and no `import_wh: null`. A term that cannot be
published is ABSENT FROM THE JSON ENTIRELY and named in `absent`, with a reason.
`null` is what a chart library renders as zero - so a nulled term does not read
as "withheld", it reads as "the community consumed nothing", which is a claim
nobody made.

What genuinely freezes is the PROTOCOL boundary - and that lives in
`api/live_public/schemas.py` and `domain/protocol.py`.
"""

import datetime
import uuid
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from domain.rollup_freshness import RollupFreshness
from shared.const import DeviceStatus, DeviceType


class IndicativePayload(BaseModel):
    """Base for every payload derived from live measurements.

    Plan 2: live data is INDICATIVE. The DSO's data remains the only basis for
    allocation keys and invoicing, and every live view must say so - stamped by
    the SERVER, not the frontend, because the SPA is one of three clients.

    ---- why this is a base class and not a field on ApiResponse ----
    `core/api_response.py` is BYTE-IDENTICAL across five services. Adding a field
    to it here would silently fork a file the platform treats as shared, and the
    next person to diff the copies would find live-data's odd one out with no
    explanation. The flag belongs to the payload, not to the envelope.

    `Literal[True]` rather than `bool = True`: it cannot be turned off by
    construction, and it serialises into the OpenAPI schema as a constant, so a
    client can see the guarantee rather than the default.
    """

    indicative: Literal[True] = True


class VersionOut(BaseModel):
    """GET /version. Authenticated, manager-only, touches no database.

    ---- why this route exists ----
    Plan 13's step 1 says "`api/live` has only `/health`". That cannot work:
    `scripts/export_openapi.py` filters out every path starting `/health`, so a
    health-only app emits a spec with ZERO paths, `krakend.json` comes out
    byte-identical to the one already deployed, and the step whose entire purpose
    is to de-risk the silent wiring asserts nothing about the gateway leg, the
    nginx header blanking, or `auth: false`.

    One authenticated no-DB route exercises the whole chain end to end -
    GatewayScopeMiddleware, resolve_internal_community, require_feature,
    manager_only, ApiResponse - and gives the `live` OpenAPI file a path before
    the device routes exist. Recorded as an addition to plan 11.1 in 18.
    """

    model_config = ConfigDict(from_attributes=True)

    service: str
    protocol_version: int
    schema_version: int


class DeviceOut(IndicativePayload):
    """A device as the administrator sees it."""

    model_config = ConfigDict(from_attributes=True)

    # The PUBLIC id: what appears in topics, what IS the MQTT username, and what
    # every other API path takes. The internal integer primary key is never
    # exposed.
    device_id: uuid.UUID
    name: str
    type: DeviceType
    status: DeviceStatus
    ean: str
    pure_injection: bool
    capacity_kva: float | None = Field(
        default=None,
        description=(
            "Snapshot of the meter's declared injection capacity, in kVA. This is "
            "the AC inverter/connection ceiling, NOT the DC panel peak (kWc) - a "
            "PV array is routinely oversized against its inverter, so treat it as "
            "a bound to clip against and never as a capacity to trust."
        ),
    )
    connector_name: str | None = None
    connector_version: str | None = None
    enrolled_at: datetime.datetime | None = None
    created_at: datetime.datetime


class DeviceCreate(BaseModel):
    """POST /devices.

    No `status` and no `device_id`: both are server-assigned. Creating a device
    does not enrol it - it mints the row and the first token, and the device
    enrols itself later through the public leg.
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=128)
    # Validated against the CRM at creation, because nothing else ever will:
    # `device.ean` is a plain column in another database, never a foreign key.
    ean: str = Field(min_length=1, max_length=64)
    type: DeviceType = Field(
        default=DeviceType.PRODUCTION,
        description="Phase 1 enrols production sites only (plan deviation 6).",
    )
    pure_injection: bool = Field(
        default=False,
        description=(
            "True only when NOTHING consumes behind this meter, in which case the "
            "export IS the production. Otherwise production is invisible to a P1 "
            "port and `production_wh` must be null - a connector that guesses "
            "here silently understates community production for ever, in a way "
            "that is indistinguishable from a cloudy day."
        ),
    )


class EnrollmentTokenOut(BaseModel):
    """The token, in plaintext, exactly once.

    Returned by POST /devices and POST /devices/{id}/token and stored only as a
    SHA-256 hash. There is no endpoint that reads it back: if it is lost, issue
    another - which invalidates this one.

    For a `consumption` device the token goes to the MEMBER, through their own
    platform account, never to the administrator - collecting it is also where
    the member makes their visibility choices.

    "Phase 1 enrols production only" is a PLAN statement, not a gate: `POST
    /devices` accepts `type: 2` today and passes it straight through. That is
    deliberate - `read_service._to_status` coarsens `last_seen_at` for every
    consumption device by DEFAULT, so the privacy protection is in place before
    the path is reachable, rather than being the change that gets forgotten on
    the day somebody un-refuses the type.
    """

    model_config = ConfigDict(extra="forbid")

    token: str = Field(description="Shown once. Never recoverable.")
    expires_at: datetime.datetime
    qr_svg: str = Field(
        description=(
            "The token as an inline SVG data URI, rendered server-side. Inline "
            "rather than a second endpoint, because a URL carrying a credential "
            "lands in nginx's and KrakenD's access logs."
        )
    )


# ---------------------------------------------------------------------------
# The read surface (build step 6)
# ---------------------------------------------------------------------------


class AbsentTerm(BaseModel):
    """A term that is not in this payload, and why.

    The alternative - omitting it silently - makes "withheld for privacy",
    "not measured by this hardware" and "not built yet" indistinguishable, and
    all three arrive as a missing key. A client cannot write different copy for
    them, so it writes none.
    """

    term: str
    reason: str


class SummaryOut(IndicativePayload):
    """GET /summary. The community right now. MANAGER only since D-14: a member
    sees their own sharing operation(s) through /mine, never the community."""

    # The most recent CLOSED hour - one the tick recomputed after it ended - and
    # absent until one exists. NOT the newest hour: that is the hour in progress
    # for most of every hour, and the card is labelled "last full hour".
    bucket: datetime.datetime | None = None

    # ---- energy ----
    # production_wh is ALWAYS present when it is known, at any k - decided
    # 2026-09-16, see domain/kanon.py. import_wh/export_wh are ABSENT below k.
    production_wh: float | None = None
    import_wh: float | None = None
    export_wh: float | None = None
    # The ESTIMATED energy shared inside the community's operations (D-14): the
    # sum over operations of LEAST(export, import) per quarter-hour, among
    # monitored meters only. A grid-derived term, withheld with import/export.
    shared_wh: float | None = None

    # Derived as `wh * 3600 / interval_s` from the last complete interval, never
    # from the payload's optional `power_w` (plan 11.1).
    power_w: float | None = None

    # ---- the fleet ----
    n_devices: int = 0
    n_devices_online: int = 0
    n_devices_never_seen: int = 0
    n_devices_silent: int = 0
    n_members: int | None = None

    # A Literal, not an enum with one member used so far.
    #
    # Deviation 6 forbids deriving a GOOD/BAD signal from production alone: with
    # no consumption term, "the sun is shining" is not "now is a good time to run
    # the washing machine", and a green light that means the former will be read
    # as the latter. Typing it as `Literal["neutral"]` makes the other values
    # UNREPRESENTABLE rather than merely unused - a future contributor adding
    # `"good"` has to change the type, which is a conversation.
    signal: Literal["neutral"] = "neutral"

    # ---- is the scheduler keeping up ----
    # The same verdict `/ops/health` carries (domain/rollup_freshness.py), so the
    # dashboard's stale banner and the Ops tab cannot disagree, whatever range the
    # chart shows. The series' newest point could not say it: a quarter-hour
    # series never touches the rollups, and a day series only has closed days.
    rollup_freshness: RollupFreshness = RollupFreshness.NEVER
    rollup_lag_minutes: float | None = None

    absent: list[AbsentTerm] = Field(default_factory=list)


class SeriesPointOut(BaseModel):
    """One bucket. Suppressed buckets are OMITTED from the series, not nulled."""

    bucket: datetime.datetime
    production_wh: float | None = None
    import_wh: float | None = None
    export_wh: float | None = None
    # Estimated, among monitored meters; withheld with the grid terms (D-14).
    shared_wh: float | None = None
    n_devices: int = 0


class SeriesOut(IndicativePayload):
    """GET /series and GET /operations/{id}/series. MANAGER only (D-14)."""

    resolution: str
    start: datetime.datetime
    end: datetime.datetime
    points: list[SeriesPointOut] = Field(default_factory=list)

    # Per-bucket suppression, per plan 9.3. An all-or-nothing "below k" bit
    # computed as a MIN over a caller-chosen window is bisectable on the same
    # grid, and one bad bucket blanks a 30-day chart.
    #
    # Note what is counted: buckets whose GRID TERMS were withheld. The bucket
    # itself is still in `points` carrying its production, because production is
    # not subject to k.
    suppressed_buckets: int = 0

    # Like MeterMapDTO. The response crosses KrakenD, which parses and
    # re-serialises every body inside a 3000 ms budget with no per-route override.
    truncated: bool = False
    cap: int = 0

    absent: list[AbsentTerm] = Field(default_factory=list)


class LiveSettingsOut(BaseModel):
    """GET /settings and the body of PUT /settings, at MANAGER."""

    members_see_production: bool
    members_see_aggregate: bool
    k: int

    # True when no row exists and these are the platform defaults. The panel shows
    # a first-run banner on it; without the flag a manager cannot tell "we chose
    # this" from "nobody has ever looked".
    is_default: bool = False


class LiveSettingsUpdate(BaseModel):
    """PUT /settings. FULL REPLACEMENT, not a patch.

    `extra="forbid"` so a misspelled field is a 422 rather than a silently
    ignored privacy change - the failure mode that matters here is a manager who
    believes they turned something off.
    """

    model_config = ConfigDict(extra="forbid")

    members_see_production: bool
    members_see_aggregate: bool
    # Floor 3, mirroring `ck_community_live_settings_k_floor`. Bounded above too:
    # a k of 100000 is not a privacy setting, it is an outage.
    k: int = Field(ge=3, le=1000)


# ---------------------------------------------------------------------------
# The forecast seam (build step 9)
# ---------------------------------------------------------------------------


class ForecastPointOut(BaseModel):
    bucket: datetime.datetime
    wh: float


class ForecastOut(IndicativePayload):
    """GET /forecast.

    EMPTY WITH A REASON, never a 404 and never a bare list. plan 14 criterion 7
    is explicit about this and gives the exact body:

        {"data":{"buckets":[],"reason":"no_method_for_production_chain"},...}

    The reason is what makes the empty case distinguishable from a broken job
    once methods DO exist. A 404 says "this endpoint is not here", which is false
    and which a frontend handles by hiding the panel - so the day the first
    method ships, nothing appears and nobody knows why.

    `buckets` rather than `points`, matching the criterion's body verbatim: three
    implementers code against that document from outside this repository.
    """

    buckets: list[ForecastPointOut] = Field(default_factory=list)
    # None once a method actually produced something.
    reason: str | None = None
    method: str | None = None
    method_version: str | None = None


class ForecastMethodOut(BaseModel):
    """One registered method, as an admin screen needs it.

    `input_schema` is JSON Schema, which is what lets the screen render a form
    for a method it has never heard of - the reason the registry is data and not
    a chain of imports.
    """

    name: str
    description: str
    version: str
    supports: list[int]
    required_weather_variables: list[str]
    input_schema: dict


# ---------------------------------------------------------------------------
# Ops (build step 11)
# ---------------------------------------------------------------------------


class DeviceStatusOut(IndicativePayload):
    """A device with its last-known state, for the administrator's list.

    `last_seen_at` IS OMITTED for a consumption device - see
    `api/live/read_service.py`. plan 16: "'This member's device has been offline
    for three days' is the absence signal the consent flag exists to withhold."

    OMITTED means absent from the JSON, and the two routes serving this model are
    `response_model_exclude_none` for that reason. They were not, for a while:
    every unanswered field went out as `null`, and the SPA - typed for absence,
    like every other read route here - rendered "recomputed null minutes ago".
    """

    device_id: uuid.UUID
    name: str
    type: DeviceType
    status: DeviceStatus
    ean: str
    connector_name: str | None = None
    connector_version: str | None = None

    health: str
    hint: str
    online: bool | None = None
    # `no_telegram`, `parse_error`, `port_closed`, `source_unreachable`, `ok`.
    # Stored since build step 3 and readable nowhere until now.
    diag: str | None = None
    diag_since: datetime.datetime | None = None
    last_seen_at: datetime.datetime | None = None
    last_measurement_at: datetime.datetime | None = None
    last_reject_reason: str | None = None
    last_reject_at: datetime.datetime | None = None
    energy_recent_wh: float | None = None


class OpsHealthOut(IndicativePayload):
    """GET /ops/health. The fleet at a glance, for a manager at 03:00."""

    n_devices: int = 0
    # Keyed by `domain.device_health.DeviceHealth`. A dict rather than one field
    # per state, so a new state does not need a DTO change and a frontend that
    # does not know it renders it as an unknown bucket rather than dropping it.
    by_health: dict[str, int] = Field(default_factory=dict)
    # The newest community-hour bucket: how new the DATA is. Not the scheduler's
    # health - a quiet fleet ages this while every tick runs, which is how the Ops
    # tab came to blame the rollups every quiet evening. Kept for the runbook.
    newest_rollup_bucket: datetime.datetime | None = None
    rollup_age_minutes: float | None = None
    # Whether the SCHEDULER is keeping up (domain/rollup_freshness.py), and the
    # age that verdict rests on. Every device can be healthy while the tick is
    # dead, and nothing else on this page would say so. `rollup_computed_at` and
    # `rollup_pending_since` are the raw watermarks, for whoever is triaging.
    rollup_freshness: RollupFreshness = RollupFreshness.NEVER
    rollup_lag_minutes: float | None = None
    rollup_computed_at: datetime.datetime | None = None
    rollup_pending_since: datetime.datetime | None = None
    # Whole MESSAGES that could not be stored.
    dead_letters_24h: int = 0
    # Devices whose last rejection, within 24 h, dropped individual READINGS. Those
    # leave no dead letter - the rest of the batch was stored - so without this
    # a capacity mismatch clipping every sunny peak reads as "nothing lost".
    # A LOWER BOUND: `device_last` keeps only the last reject, and a later
    # message-level one overwrites it.
    n_devices_readings_rejected_24h: int = 0
    devices: list[DeviceStatusOut] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Sharing operations (D-14)
# ---------------------------------------------------------------------------


class OperationOut(BaseModel):
    """GET /operations: one sharing operation with live data, for the manager.

    `n_devices` and `n_meters` are the COVERAGE of the shared estimate: devices
    monitoring meters in this operation today, out of the operation's ACTIVE
    meters in the CRM. An estimate over 2 of 9 meters says little about the other
    7, and the dashboard prints both numbers next to it.
    """

    id: int
    name: str
    n_devices: int = 0
    n_meters: int = 0


class OperationSummaryOut(IndicativePayload):
    """GET /operations/{id}/summary: one operation as of its last CLOSED hour."""

    id_sharing_operation: int
    bucket: datetime.datetime | None = None
    production_wh: float | None = None
    import_wh: float | None = None
    export_wh: float | None = None
    shared_wh: float | None = None
    n_devices: int = 0
    n_members: int | None = None
    n_meters: int = 0
    absent: list[AbsentTerm] = Field(default_factory=list)


class MemberOperationOut(BaseModel):
    """GET /mine/operations: an operation the caller holds an ACTIVE meter in."""

    id: int
    name: str


class MemberOperationPointOut(BaseModel):
    """One bucket of a member's operation series.

    A SEPARATE TYPE, and that is the guard. A member sees their operation's
    production and - where its meters cannot measure production (protocol 3.4) -
    its export, under k. Never its import, never the shared estimate, never the
    community. Reusing `SeriesPointOut` and relying on those fields being left
    None would leak the day someone forgets to clear one; here they cannot be
    expressed at all, and tests/api/test_operations_api.py asserts the schema.
    """

    bucket: datetime.datetime
    production_wh: float | None = None
    export_wh: float | None = None
    n_devices: int = 0


class MemberOperationSeriesOut(IndicativePayload):
    """GET /mine/operations/{id}/series."""

    id_sharing_operation: int
    resolution: str
    start: datetime.datetime
    end: datetime.datetime
    points: list[MemberOperationPointOut] = Field(default_factory=list)
    suppressed_buckets: int = 0
    truncated: bool = False
    cap: int = 0
    absent: list[AbsentTerm] = Field(default_factory=list)
