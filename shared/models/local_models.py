"""SQLAlchemy models mirroring scripts/sql/schema.sql.

`scripts/sql/schema.sql` IS THE SOURCE OF TRUTH. There is no Alembic and no
autogeneration: this module is written by hand to match it, and the test harness
is what keeps the two honest — `tests/conftest.py` applies schema.sql (and ONLY
schema.sql) to a real Postgres, never `Base.metadata.create_all()`, so any drift
surfaces as an UndefinedColumnError in the first test that touches the model.

`__table_args__` restates each index and constraint under its exact SQL name.
That is not decoration: it is how a reader of this file learns that
`uq_device_community_ean_live` is partial, without opening the DDL.

Every tenant table carries `id_community` as a plain INTEGER, spelled exactly
that way — `core/database/with_community.py` hard-codes `model.id_community`
behind a `type: ignore`, so mypy will not catch a different name and every scoped
SELECT would 500 at runtime.
"""

import datetime
import uuid

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    Double,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    SmallInteger,
    String,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from core.database.database import LocalBase


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


class _TimestampMixin:
    created_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, default=_utcnow
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, default=_utcnow, onupdate=_utcnow
    )


class SchemaVersionModel(LocalBase):
    """Read by api/health/routes.py and compared with LOCAL_SCHEMA_VERSION.

    That comparison is what stops readiness going green over an empty database.
    """

    __tablename__ = "schema_version"

    version: Mapped[int] = mapped_column(Integer, primary_key=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    applied_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, default=_utcnow
    )


class DeviceModel(_TimestampMixin, LocalBase):
    """A thing that publishes the protocol on its topics with its credentials."""

    __tablename__ = "device"
    __table_args__ = (
        # Partial: a REVOKED device does not block re-enrolling the same meter,
        # which is the normal recovery when a password is lost (the password is
        # shown once and is never recoverable).
        Index(
            "uq_device_community_ean_live",
            "id_community",
            "ean",
            unique=True,
            postgresql_where=text("status <> 3"),
        ),
        Index("ix_device_community", "id_community"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    # The protocol's `device_id`: in the topic, AS the MQTT username, and pinned
    # as the broker client id. Externally visible; never reused.
    public_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, unique=True)
    id_community: Mapped[int] = mapped_column(Integer, nullable=False)
    type: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    # A plain column, never an FK — it lives in crm_db. And never accompanied by
    # a cached id_member: ownership is resolved per timestamp, because a meter
    # can change holder mid-month and a cached owner is the meter-transfer
    # privacy leak.
    ean: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=1)
    pure_injection: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    # kVA — the AC injection limit, NOT kWc. Named with its unit so a later
    # reader cannot quietly assume DC panel peak. Clip against it; never trust it.
    capacity_kva: Mapped[float | None] = mapped_column(Numeric(12, 3), nullable=True)
    max_wh_per_interval: Mapped[float | None] = mapped_column(Double, nullable=True)
    # Refreshed from EVERY status message, not only at enrolment.
    # shared.const.ProductionChain, snapshotted from the CRM at creation like
    # capacity_kva. NULL for a meter the CRM has not classified, and the forecast
    # registry treats NULL as matching no method rather than guessing
    # photovoltaic.
    production_chain: Mapped[int | None] = mapped_column(SmallInteger, nullable=True)

    connector_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    connector_version: Mapped[str | None] = mapped_column(String(32), nullable=True)
    enrolled_at: Mapped[datetime.datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    revoked_at: Mapped[datetime.datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )


class EnrollmentTokenModel(LocalBase):
    """Single use, 72 hours, hashed at rest.

    Note there is no `created_at`/`updated_at` mixin: the row is written once and
    then only ever has `claimed_until`/`consumed_at` stamped on it, and an
    `onupdate` timestamp would add a column nothing reads.
    """

    __tablename__ = "enrollment_token"
    __table_args__ = (
        # At most one unconsumed token per device: issuing a new one invalidates
        # the old.
        Index(
            "uq_enrollment_token_device_unconsumed",
            "id_device",
            unique=True,
            postgresql_where=text("consumed_at IS NULL"),
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    id_device: Mapped[int] = mapped_column(
        Integer, ForeignKey("device.id", ondelete="CASCADE"), nullable=False
    )
    id_community: Mapped[int] = mapped_column(Integer, nullable=False)
    # SHA-256 hex of the Crockford base32 token. Looked up BY hash, so the
    # comparison is an index probe rather than a byte loop.
    token_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    expires_at: Mapped[datetime.datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    # The claim lease. Two columns, not one: `claimed_until` is taken BEFORE the
    # broker call and `consumed_at` only after both sides are done, so a 504 in
    # between expires a claim instead of burning a credential the member never
    # received.
    claimed_until: Mapped[datetime.datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    consumed_at: Mapped[datetime.datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, default=_utcnow
    )


class MeasurementModel(LocalBase):
    """One quarter-hour reading. PARTITIONED MONTHLY ON ts.

    The composite primary key contains the partition key, which is what makes
    `ON CONFLICT (id_device, ts) DO UPDATE` legal on the partitioned parent — and
    that upsert is the whole of the protocol's idempotence guarantee. A connector
    that is unsure whether a message arrived is supposed to re-send it.

    SQLAlchemy has no declarative notion of a partitioned parent; it sees an
    ordinary table, which is correct — every read and write goes through the
    parent. The partitioning lives in schema.sql alone.
    """

    __tablename__ = "measurement"
    __table_args__ = (Index("ix_measurement_community_ts", "id_community", "ts"),)

    id_device: Mapped[int] = mapped_column(Integer, primary_key=True)
    # END of the interval, always.
    ts: Mapped[datetime.datetime] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)
    id_community: Mapped[int] = mapped_column(Integer, nullable=False)
    interval_s: Mapped[int] = mapped_column(Integer, nullable=False)
    # Double, never Float(precision) that maps to REAL: SUM(real) returns real,
    # and step 8's rollups would accumulate float32 error into every displayed
    # total, silently and unfixably.
    import_wh: Mapped[float] = mapped_column(Double, nullable=False)
    export_wh: Mapped[float] = mapped_column(Double, nullable=False)
    production_wh: Mapped[float | None] = mapped_column(Double, nullable=True)
    received_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, default=_utcnow
    )


class DeviceLastModel(LocalBase):
    """One row per device. TWO CLOCKS — see schema.sql for why.

    `ts` is the device's measurement clock and guards the measurement columns.
    `status_at` is the server's receipt clock and guards liveness/diagnostics,
    because an LWT payload is composed at CONNECT time and would otherwise lose
    every comparison against the online statuses that followed it — leaving a
    crashed device showing online for ever, in a row that looks fresh.
    """

    __tablename__ = "device_last"
    __table_args__ = (Index("ix_device_last_community", "id_community"),)

    id_device: Mapped[int] = mapped_column(
        Integer, ForeignKey("device.id", ondelete="CASCADE"), primary_key=True
    )
    id_community: Mapped[int] = mapped_column(Integer, nullable=False)

    # ---- measurement clock ----
    ts: Mapped[datetime.datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    power_w: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # ---- server receipt clock ----
    status_at: Mapped[datetime.datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    online: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    diag: Mapped[str | None] = mapped_column(String(32), nullable=True)
    diag_since: Mapped[datetime.datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )

    # Written ONLY for a device row that loaded — an FK violation here would
    # abort the whole ingest transaction.
    last_reject_reason: Mapped[str | None] = mapped_column(String(48), nullable=True)
    last_reject_at: Mapped[datetime.datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    # Advanced by telemetry AND status, so "never seen" and "offline" are
    # distinguishable inside the first quarter-hour after a connect.
    last_seen_at: Mapped[datetime.datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, default=_utcnow, onupdate=_utcnow
    )


class CommunityLiveSettingsModel(_TimestampMixin, LocalBase):
    """Per-community visibility settings. Inert until build step 6.

    Created now because the defaults are a privacy decision, and writing them
    down here means step 6 inherits them rather than inventing a value at the
    moment it first needs one.
    """

    __tablename__ = "community_live_settings"
    __table_args__ = (CheckConstraint("k >= 3", name="ck_community_live_settings_k_floor"),)

    id_community: Mapped[int] = mapped_column(Integer, primary_key=True)
    members_see_production: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    members_see_aggregate: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    # Thresholded on n_members, never n_devices: a member with three meters is
    # one member, and counting devices makes the guarantee decorative.
    k: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=5)


class IngestDeadLetterModel(LocalBase):
    """The sole record that a message existed and could not be stored.

    Load-bearing in a way it would not be on a broker with manual acks: aiomqtt
    2.x PUBACKs every QoS-1 message before any database write, so there is no
    acknowledgement to withhold and no redelivery. One chance per message.

    Append-only by convention rather than by trigger — nothing in the service
    updates it, and a constraint trigger here would be ceremony around a table
    whose only reader is a human at 03:00. The one DELETE is the scheduler's
    nightly retention, past RETENTION_DEAD_LETTER_DAYS
    (`worker/retention.prune_dead_letters`).
    """

    __tablename__ = "ingest_dead_letter"
    __table_args__ = (Index("ix_ingest_dead_letter_received_at", text("received_at DESC")),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    topic: Mapped[str] = mapped_column(Text, nullable=False)
    # NULL when the device could not be identified at all.
    id_device: Mapped[int | None] = mapped_column(Integer, nullable=True)
    reason: Mapped[str] = mapped_column(String(48), nullable=False)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Capped by the caller: a 64 KB payload stored verbatim for every failure
    # during an outage is its own incident.
    payload: Mapped[str | None] = mapped_column(Text, nullable=True)
    received_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, default=_utcnow
    )


# ---------------------------------------------------------------------------
# Migration 0001 — steps 7, 8 and 9.
#
# These tables live in BOTH scripts/sql/schema.sql and
# scripts/sql/migrations/0001_*.sql, because provision.sh never re-applies a
# schema to a database that already has tables. tests/conftest.py applies both,
# so a model that drifts from either surfaces here first.
# ---------------------------------------------------------------------------


class DeviceOwnerWindowModel(LocalBase):
    """A local projection of CRM meter ownership, refreshed by the scheduler.

    Ownership is TIME-SLICED and is resolved per timestamp, never cached onto
    `device`: one EAN has several `meter_data` rows and can change holder
    mid-month. A denormalised owner is the meter-transfer privacy leak — the
    previous occupant's credential keeps publishing, attributed to them, about
    someone else's household.

    `ambiguous` is the row billing does not need. Billing REFUSES a run (422) on
    overlapping ownership windows; a background projection cannot refuse, so it
    flags the window. An ambiguous window's energy is still counted — excluding
    it would understate the community total — but its membership is not, because
    overcounting members weakens k while undercounting only over-suppresses.
    """

    __tablename__ = "device_owner_window"
    __table_args__ = (
        Index("ix_device_owner_window_ean_from", "ean", text("valid_from DESC")),
        Index("ix_device_owner_window_community", "id_community"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    # A plain column, never a foreign key: `meter` lives in crm_db.
    ean: Mapped[str] = mapped_column(String(64), nullable=False)
    id_community: Mapped[int] = mapped_column(Integer, nullable=False)
    # NULL when the CRM has no owner for this window.
    id_member: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Brussels-local DATE bounds, mirroring meter_data. CLOSED at both ends, like
    # meter_data's (D-11); NULL valid_to means open-ended.
    valid_from: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    valid_to: Mapped[datetime.date | None] = mapped_column(Date, nullable=True)
    ambiguous: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    # The CRM window's sharing operation (migration 0003, D-14). NULL when the
    # meter is in none at these dates.
    id_sharing_operation: Mapped[int | None] = mapped_column(Integer, nullable=True)
    refreshed_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, default=_utcnow
    )


class RollupDeviceHourModel(LocalBase):
    """Per-device hourly energies. PARTITIONED MONTHLY ON bucket.

    `bucket` is the START of the UTC hour, derived from `measurement.ts`, which
    is the END of its interval — so a reading stamped 11:00:00Z belongs to bucket
    10:00:00Z. See domain/buckets.py; that one convention is most of step 8's
    risk.
    """

    __tablename__ = "rollup_device_hour"
    __table_args__ = (Index("ix_rollup_device_hour_community_bucket", "id_community", "bucket"),)

    id_device: Mapped[int] = mapped_column(Integer, primary_key=True)
    bucket: Mapped[datetime.datetime] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)
    id_community: Mapped[int] = mapped_column(Integer, nullable=False)
    import_wh: Mapped[float] = mapped_column(Double, nullable=False)
    export_wh: Mapped[float] = mapped_column(Double, nullable=False)
    # NULLABLE and never coalesced to 0. NULL means "this device does not measure
    # production"; 0 asserts "produced nothing", which is a different and false
    # statement. Typing this non-optional is how a COALESCE gets added later to
    # "fix mypy".
    production_wh: Mapped[float | None] = mapped_column(Double, nullable=True)
    # < 4 means a partial hour, which is what lets the hour in progress be
    # published without lying about it.
    n_samples: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    n_production_samples: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    computed_at: Mapped[datetime.datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class RollupDeviceDayModel(LocalBase):
    """Per-device daily energies. PARTITIONED MONTHLY ON bucket.

    `bucket` is the instant of LOCAL midnight (Europe/Brussels — see
    shared.const.ROLLUP_DAY_TIMEZONE). `n_hours` is 23 or 25 on the two DST days
    of the year, and that is correct rather than a defect.
    """

    __tablename__ = "rollup_device_day"
    __table_args__ = (Index("ix_rollup_device_day_community_bucket", "id_community", "bucket"),)

    id_device: Mapped[int] = mapped_column(Integer, primary_key=True)
    bucket: Mapped[datetime.datetime] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)
    id_community: Mapped[int] = mapped_column(Integer, nullable=False)
    import_wh: Mapped[float] = mapped_column(Double, nullable=False)
    export_wh: Mapped[float] = mapped_column(Double, nullable=False)
    production_wh: Mapped[float | None] = mapped_column(Double, nullable=True)
    n_hours: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    n_production_hours: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    computed_at: Mapped[datetime.datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class RollupCommunityHourModel(LocalBase):
    """Per-community hourly energies plus the k inputs. NOT partitioned.

    One row per community per bucket is four orders of magnitude smaller than
    `measurement`; partitioning it would mean two more monthly partitions a month
    for no operational benefit.
    """

    __tablename__ = "rollup_community_hour"

    id_community: Mapped[int] = mapped_column(Integer, primary_key=True)
    bucket: Mapped[datetime.datetime] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)
    import_wh: Mapped[float] = mapped_column(Double, nullable=False)
    export_wh: Mapped[float] = mapped_column(Double, nullable=False)
    production_wh: Mapped[float | None] = mapped_column(Double, nullable=True)
    n_devices: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    n_devices_production: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    # n_members, NOT n_devices (plan 9.3): "a member with three meters is one
    # member; counting devices makes the guarantee decorative." Resolved at THIS
    # bucket's instant — resolving at now() retroactively rewrites a closed hour.
    n_members: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    # Counted for energy, excluded from membership. k thresholds on a LOWER BOUND
    # of distinct members, so this fails closed.
    n_devices_unattributed: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    computed_at: Mapped[datetime.datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class RollupCommunityDayModel(LocalBase):
    """Per-community daily energies. NOT partitioned, and NOT retained-against.

    Section 16 treats a community aggregate as non-personal — no device, no
    member — which is why this is the one rollup kept indefinitely while the
    per-device tables age out with the raw data.

    `n_members` here is the MAX over the day's hours, never the SUM. SUM gives a
    five-member community an n_members of 120, and k then passes on a day that
    should have been suppressed.
    """

    __tablename__ = "rollup_community_day"

    id_community: Mapped[int] = mapped_column(Integer, primary_key=True)
    bucket: Mapped[datetime.datetime] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)
    import_wh: Mapped[float] = mapped_column(Double, nullable=False)
    export_wh: Mapped[float] = mapped_column(Double, nullable=False)
    production_wh: Mapped[float | None] = mapped_column(Double, nullable=True)
    n_devices: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    n_devices_production: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    n_members: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    n_devices_unattributed: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    n_hours: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    computed_at: Mapped[datetime.datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    # MIN over the day's hours (migration 0003). A day's grid terms are published
    # only if EVERY hour's were, or "day minus its published hours" is the
    # withheld ones. NULL on a row computed before 0003: read as "suppress".
    n_members_min: Mapped[int | None] = mapped_column(SmallInteger, nullable=True)


class RollupOperationHourModel(LocalBase):
    """Per sharing operation hourly energies. PARTITIONED MONTHLY ON bucket.

    One row per (community, operation, bucket), plus the REMAINDER row
    `id_sharing_operation = 0` for every device in no operation at that bucket -
    so the rows of a bucket sum to the community total exactly (D-14).

    `shared_wh` is an ESTIMATE: per quarter-hour, LEAST(export, import) over the
    operation's monitored meters, summed. NULL exactly on the remainder row.
    """

    __tablename__ = "rollup_operation_hour"

    id_community: Mapped[int] = mapped_column(Integer, primary_key=True)
    id_sharing_operation: Mapped[int] = mapped_column(Integer, primary_key=True)
    bucket: Mapped[datetime.datetime] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)
    import_wh: Mapped[float] = mapped_column(Double, nullable=False)
    export_wh: Mapped[float] = mapped_column(Double, nullable=False)
    production_wh: Mapped[float | None] = mapped_column(Double, nullable=True)
    shared_wh: Mapped[float | None] = mapped_column(Double, nullable=True)
    n_devices: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    n_devices_production: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    n_members: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    computed_at: Mapped[datetime.datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class RollupOperationDayModel(LocalBase):
    """Per sharing operation daily energies, from the operation hours.

    Partitioned and kept 13 months: an operation of two households is two
    households, so unlike the community day this is personal data. `n_members`
    is the day's MAX, `n_members_min` its MIN - the day is published only on the
    MIN (see `RollupCommunityDayModel.n_members_min`).
    """

    __tablename__ = "rollup_operation_day"

    id_community: Mapped[int] = mapped_column(Integer, primary_key=True)
    id_sharing_operation: Mapped[int] = mapped_column(Integer, primary_key=True)
    bucket: Mapped[datetime.datetime] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)
    import_wh: Mapped[float] = mapped_column(Double, nullable=False)
    export_wh: Mapped[float] = mapped_column(Double, nullable=False)
    production_wh: Mapped[float | None] = mapped_column(Double, nullable=True)
    shared_wh: Mapped[float | None] = mapped_column(Double, nullable=True)
    n_devices: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    n_devices_production: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    n_members: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    n_members_min: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    n_hours: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    computed_at: Mapped[datetime.datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class RollupDirtyModel(LocalBase):
    """Buckets awaiting recompute.

    Community-hour granularity, not device-hour: the unit of recomputation is the
    community hour, because `n_members` cannot be computed for one device.

    Written in the SAME STATEMENT as the measurement, unconditionally — see
    worker/ingest.py. Marking only "outside the window" loses a bucket written at
    10:59:59 when the tick fires at 11:00:02, and loses everything ingested
    during a scheduler outage.
    """

    __tablename__ = "rollup_dirty"

    id_community: Mapped[int] = mapped_column(Integer, primary_key=True)
    bucket: Mapped[datetime.datetime] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)
    marked_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, default=_utcnow
    )


class ForecastProductionModel(LocalBase):
    """Predicted production. INERT in phase 1 — nothing writes it.

    `method` and `method_version` are NOT optional (plan 10.3): without them two
    methods cannot be compared, a retro-adjustment cannot be attributed, and rows
    produced by a model since corrected cannot be found.
    """

    __tablename__ = "forecast_production"
    __table_args__ = (Index("ix_forecast_production_community_bucket", "id_community", "bucket"),)

    ean: Mapped[str] = mapped_column(String(64), primary_key=True)
    bucket: Mapped[datetime.datetime] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)
    method: Mapped[str] = mapped_column(String(64), primary_key=True)
    id_community: Mapped[int] = mapped_column(Integer, nullable=False)
    wh: Mapped[float] = mapped_column(Double, nullable=False)
    method_version: Mapped[str] = mapped_column(String(32), nullable=False)
    computed_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, default=_utcnow
    )


class DeviceForecastMethodModel(_TimestampMixin, LocalBase):
    """A per-device method override. INERT in phase 1."""

    __tablename__ = "device_forecast_method"

    id_device: Mapped[int] = mapped_column(
        Integer, ForeignKey("device.id", ondelete="CASCADE"), primary_key=True
    )
    method: Mapped[str] = mapped_column(String(64), nullable=False)
    params: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)


class ConsentModel(LocalBase):
    """A member's visibility choices. INERT until phase 2.

    Append-only, with PER-FIELD `effective_from` — the only shape that satisfies
    section 9's per-field, per-direction retroactivity: a withdrawal applies to
    the past by default, an addition does not. Read with `DISTINCT ON
    (id_member, field) ... ORDER BY decided_at DESC, id DESC`.

    Visibility is evaluated AT READ TIME, never at write time. Measurements are
    always stored; a withdrawal masks them.
    """

    __tablename__ = "consent"
    __table_args__ = (
        Index("ix_consent_member_field", "id_member", "field", text("decided_at DESC")),
        Index("ix_consent_community", "id_community"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    id_community: Mapped[int] = mapped_column(Integer, nullable=False)
    id_member: Mapped[int] = mapped_column(Integer, nullable=False)
    field: Mapped[str] = mapped_column(String(48), nullable=False)
    granted: Mapped[bool] = mapped_column(Boolean, nullable=False)
    # NULL means "from the beginning of time" — a withdrawal applying to the past.
    effective_from: Mapped[datetime.datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    decided_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, default=_utcnow
    )


class ConsentEventModel(LocalBase):
    """The audit trail behind a consent decision. INERT until phase 2."""

    __tablename__ = "consent_event"
    __table_args__ = (Index("ix_consent_event_consent", "id_consent"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    id_consent: Mapped[int] = mapped_column(
        Integer, ForeignKey("consent.id", ondelete="CASCADE"), nullable=False
    )
    id_community: Mapped[int] = mapped_column(Integer, nullable=False)
    actor: Mapped[str] = mapped_column(String(64), nullable=False)
    action: Mapped[str] = mapped_column(String(32), nullable=False)
    recorded_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, default=_utcnow
    )
