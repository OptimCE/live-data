"""Domain constants.

IntEnums for coded values that are stored as integers; StrEnums for values stored
as text. Nothing here may import from ``api/`` or pull the HTTP stack: this module
is imported by ``worker/``, whose image installs no fastapi (see Dockerfile.worker).
"""

from enum import IntEnum, StrEnum
from typing import Final


class FeatureName(StrEnum):
    """The subscription key this annexe is gated on.

    Matches ``community_subscription.feature`` in ``crm_db``, which is a free-form
    VARCHAR(64) with no enum constraining it, and the ``feature`` key in
    ``crm-backend/config/annexes-services.json``. Exactly one member per service.
    """

    LIVE_DATA = "live-data"


# The schema version `scripts/sql/schema.sql` declares, and the value
# `api/health/routes.py` compares against.
#
# This is load-bearing, and not merely informational. `10-databases.sql` creates
# `live_data_local` BEFORE any schema is applied, so a readiness probe that runs
# `SELECT 1` succeeds against a database with zero tables and the compose
# healthcheck goes green over an empty service. Reading `schema_version` instead
# collapses five separate silent failures into one 503: a missing schema mount, a
# mount that Docker materialised as an empty DIRECTORY, a `schema.sql` that git
# never tracked, a half-applied schema, and a stale one.
#
# Bump it in the same change as `schema.sql`, and add the matching row there.
LOCAL_SCHEMA_VERSION: Final[int] = 4


class DeviceType(IntEnum):
    """What a device measures.

    Phase 1 enrols PRODUCTION only (plan deviation 6): a P1 on a production site
    measures that site's exchange with the grid, not the community's load, so
    there is no consumption term and the signal is NEUTRAL rather than green.
    CONSUMPTION is declared now because the column is cheap and the value is
    stored on disk - never renumber these.
    """

    PRODUCTION = 1
    CONSUMPTION = 2


class ProductionChain(IntEnum):
    """crm-backend's `ProductionChain`, mirrored.

    Source of truth: crm-backend/src/modules/meters/shared/meter.types.ts. Copied
    rather than derived because it lives in another repository and another
    language; `ports/crm_read.MeterSnapshot.production_chain` carries the raw int
    off `meter_data`, and this is what gives it a name.

    It exists here for build step 9: a forecast method declares which chains it
    `supports`, so registering a wind model does not mean touching the
    photovoltaic one or the weather port. A chain with no method registered is
    the `no_method_for_production_chain` answer, not an error.
    """

    PHOTOVOLTAIC = 1
    WIND = 2
    HYDRO = 3
    BIOMASS = 4
    BIOGAS = 5
    COGEN_FOSSIL = 6
    OTHER = 7


class DeviceStatus(IntEnum):
    """Lifecycle of a device row.

    PENDING  - created, token issued, never enrolled. No broker client exists.
    ACTIVE   - enrolled; a dynsec client exists and may publish.
    REVOKED  - the broker client has been deleted. Revocation is immediate and
               server-side; from the connector's point of view its credentials
               simply stop working (protocol 5.5). Measurements already received
               are NOT deleted - that is a separate action.
    """

    PENDING = 1
    ACTIVE = 2
    REVOKED = 3


class DiagCode(StrEnum):
    """protocol 3.2 `diag.code`.

    FIVE values, not four. Plan 16's list omits `source_unreachable`; the protocol
    is the reference, because it is the document with three independent
    implementers. Optional in the payload so a minimal connector is still valid.

    This field is why a support call can become a diagnosis: a P1 the DSO has not
    enabled, a bad cable, a wifi drop and a dead device are all identical from the
    server - silence.
    """

    OK = "ok"
    NO_TELEGRAM = "no_telegram"  # usually: the DSO has not enabled the P1 port
    PARSE_ERROR = "parse_error"
    PORT_CLOSED = "port_closed"
    SOURCE_UNREACHABLE = "source_unreachable"


# ---- MQTT topics (protocol 2) -------------------------------------------------
# A device may publish only to its own two topics and may not subscribe at all.
# The `+` in the worker's subscription is a single level: `ce/+/+/telemetry`
# matches `ce/{community_id}/{device_id}/telemetry` and nothing deeper.
TOPIC_PREFIX: Final[str] = "ce"
TOPIC_TELEMETRY_SUFFIX: Final[str] = "telemetry"
TOPIC_STATUS_SUFFIX: Final[str] = "status"
TOPIC_TELEMETRY_WILDCARD: Final[str] = "ce/+/+/telemetry"
TOPIC_STATUS_WILDCARD: Final[str] = "ce/+/+/status"

# ---- dynamic-security control API (plan 8.1) ---------------------------------
DYNSEC_REQUEST_TOPIC: Final[str] = "$CONTROL/dynamic-security/v1"
DYNSEC_RESPONSE_TOPIC: Final[str] = "$CONTROL/dynamic-security/v1/response"

# The three roles created once at bootstrap. "Two roles" in plan 7.2 was wrong:
# Phase 0 tried the zero-length retained publish that 8.6's revoke sequence needs
# as ALL THREE identities the design defines - including the dynsec bootstrap
# admin, whose default role grants publishClientSend on $CONTROL/... and nothing
# else - and every one was `Denied PUBLISH`.
ROLE_DEVICE: Final[str] = "device"
ROLE_INGEST: Final[str] = "ingest"
ROLE_REAPER: Final[str] = "reaper"

# ---- Protocol version ---------------------------------------------------------
# `v` increases only for a breaking change, and the server accepts both the old
# and the new version for at least one year (protocol 7).
PROTOCOL_VERSION: Final[int] = 1

# ---- Enrolment tokens (plan 8.3) ---------------------------------------------
# Crockford base32: no I, L, O or U, so it survives being read aloud down a phone
# or typed into a captive portal by someone holding a phone in a basement.
CROCKFORD_ALPHABET: Final[str] = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
TOKEN_ENTROPY_BITS: Final[int] = 128
TOKEN_GROUP_SIZE: Final[int] = 4  # rendered as K7M9-P2QR-4TVX...

# ---- Advisory locks -----------------------------------------------------------
# Per-service hex namespace. 0x0AD3_0001 (administrative-document) and
# 0x0B111_0001 (billing) are taken, and each of those repos carries a test that
# hard-codes the OTHER service's key to assert non-collision. Keep the low 32 bits
# unique too: administrative-document's visibility test queries pg_locks.objid
# with `key & 0xFFFFFFFF`.
#
# Steps 1-5 use none of these - they are declared here so step 8 does not have to
# re-derive the namespace, and so a collision test can be written once.
ADVISORY_LOCK_PARTITIONS: Final[int] = 0x11FE_0001
ADVISORY_LOCK_ROLLUPS: Final[int] = 0x11FE_0002
ADVISORY_LOCK_RETENTION: Final[int] = 0x11FE_0003
ADVISORY_LOCK_FORECAST: Final[int] = 0x11FE_0004
# Step 7's ownership refresh. Its OWN key rather than sharing the rollup one:
# the refresh reads the CRM across a database boundary and can be slow, and
# sharing would let it delay every 15-minute tick behind it.
ADVISORY_LOCK_OWNERSHIP: Final[int] = 0x11FE_0005

# ---- Rollups (plan 6.3) -------------------------------------------------------
# NEITHER OF THESE IS A SETTING, AND THAT IS THE POINT.
#
# Closed periods are never recomputed. So changing the day's timezone after any
# day row exists silently invalidates every historical row, with nothing anywhere
# to detect it - no error, no version, no drift check. A value that can be
# changed from the environment is a value that will be, on a machine nobody is
# looking at. Freezing it here makes the change a code change, which is a diff,
# which is a review.
#
# Europe/Brussels rather than UTC because a UTC day splits the Belgian day at
# 01:00 or 02:00 local, so every daily total a manager reads would be wrong by an
# hour or two of energy. The cost is that two days a year are 23 and 25 hours
# long - handled in domain/buckets.py, never by adding 24 hours.
ROLLUP_DAY_TIMEZONE: Final[str] = "Europe/Brussels"

# The recompute window, bucket-aligned at both ends. 48 hours is long enough to
# absorb an ordinary store-and-forward backlog without the dirty queue, and short
# enough that a tick stays cheap. Anything older arrives through `rollup_dirty`.
ROLLUP_WINDOW_HOURS: Final[int] = 48

# The REMAINDER row of `rollup_operation_hour/_day`: every device that belongs to
# no sharing operation at a bucket (none in the CRM, an ambiguous window, no
# window, a meter still WAITING_GRD). With it, the rows of a bucket sum to the
# community total exactly - which is what lets domain/kanon.py tell whether the
# global view would expose something its visible operations do not. CRM ids are
# identity columns from 1, so 0 can never be a real operation. See D-14.
NO_SHARING_OPERATION: Final[int] = 0

# ---- NATS ---------------------------------------------------------------------
# There is none, deliberately (plan 4). Nothing publishes and nothing consumes, and
# wiring it would make /health/readiness fail closed on a broker this service does
# not use. The subject space `optimce.live.>` is RESERVED here so that a later
# phase does not have to negotiate for it, and so that nobody reads the absence of
# a stream as an oversight. Do not add core/queue/ without a producer.
RESERVED_NATS_SUBJECT_PREFIX: Final[str] = "optimce.live."

# ---- Request limits -----------------------------------------------------------
# Lives here rather than in core/middleware/request_limits.py, which imports
# starlette: worker/ must be able to import this module, and Dockerfile.worker
# installs no starlette.
MAX_BODY_BYTES: Final[int] = 2 * 1024 * 1024
