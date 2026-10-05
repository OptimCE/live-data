import os
from enum import StrEnum

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from domain.partitions import MONTHS_BACK


# CORS contract:
# - Local/test: ALLOW_ORIGIN may default to "*".
# - Staging/production: ALLOW_ORIGIN is REQUIRED, must not contain "*", and may
#   be a comma-separated list (e.g. "https://app.example.com,https://admin.example.com").
# Enforced in Settings.validate_env_config below; example values live in
# .env.staging.exemple and .env.production.exemple.
class Environment(StrEnum):
    LOCAL = "local"
    TEST = "test"
    STAGING = "staging"
    PRODUCTION = "production"


def _get_env_file() -> str:
    env = os.getenv("ENV", "local").strip()
    return f".env.{env}"


class Settings(BaseSettings):
    # NOTE: _get_env_file() is evaluated at CLASS-DEFINITION time, and this module
    # ends with `settings = Settings()`. So ENV must be in os.environ BEFORE the
    # first import of any project module -- which is why tests/conftest.py does
    # `os.environ.setdefault("ENV", "test")` above its imports, and why every
    # *-doc-gen compose one-shot sets ENV=local.
    model_config = SettingsConfigDict(
        env_file=_get_env_file(),
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ---- Core ----
    ENV: Environment = Environment.LOCAL

    # ---- CRM Database ----
    CRM_DATABASE_URL: str  # postgresql+asyncpg://...
    CRM_DB_POOL_SIZE: int = 20
    CRM_DB_MAX_OVERFLOW: int = 10
    CRM_DB_POOL_RECYCLE: int = 3600  # seconds - recycle connections after 1 hour
    CRM_DB_POOL_TIMEOUT: int = 30  # seconds - wait for available connection
    CRM_DB_SSL: bool = False  # enable SSL/TLS for database connection

    # ---- LOCAL Database ----
    LOCAL_DATABASE_URL: str  # postgresql+asyncpg://...
    LOCAL_DB_POOL_SIZE: int = 20
    LOCAL_DB_MAX_OVERFLOW: int = 10
    LOCAL_DB_POOL_RECYCLE: int = 3600  # seconds - recycle connections after 1 hour
    LOCAL_DB_POOL_TIMEOUT: int = 30  # seconds - wait for available connection
    LOCAL_DB_SSL: bool = False  # enable SSL/TLS for database connection

    # ---- MQTT broker: the control connection ----
    # Where the API and the worker DIAL the broker. This is an address on the
    # compose `backend` network and it is NEVER handed to a device.
    MQTT_HOST: str = "mosquitto"
    MQTT_PORT: int = 1883  # the unpublished in-network listener; TLS is 8883
    MQTT_TLS: bool = False
    # The dynamic-security admin identity. The API drives enrolment and revocation
    # entirely over the $CONTROL/dynamic-security/v1 topic API - there is no
    # mosquitto_ctrl binary and no subprocess.
    MQTT_ADMIN_USERNAME: str = ""
    MQTT_ADMIN_PASSWORD: str = ""
    # The INGEST identity, and it is deliberately NOT the admin one.
    #
    # The dynsec admin's role grants publishClientSend on $CONTROL/... and
    # nothing whatever on `ce/#`. A worker connecting as the admin would
    # SUBSCRIBE successfully, receive a perfectly normal SUBACK, and then be
    # delivered nothing at all - for ever, with no error at either end. That is
    # the failure the `ingest` role exists to avoid, and a SUBACK cannot
    # distinguish it from success.
    MQTT_INGEST_USERNAME: str = ""
    MQTT_INGEST_PASSWORD: str = ""
    # A HARD per-command timeout is mandatory, not defensive coding. A malformed
    # payload DOES get a dynsec response, but with no `correlationData`, so it can
    # never resolve a caller's future - an unbounded await deadlocks inside a
    # request KrakenD cuts at 3000 ms. The enrolment retry path is two commands,
    # so 2 x 800 ms plus the DB round-trips leaves roughly a second of headroom.
    # The Phase 0 spike's 5.0 s default exceeds the entire gateway budget.
    MQTT_COMMAND_TIMEOUT_MS: int = 800
    # Fixed, NOT derived from hostname/pid/uuid. The broker keys stored session
    # state on the client id, so a generated one starts a fresh session on every
    # container recreate and the whole queue is silently gone (protocol 4.3,
    # plan 7.3). One replica, deliberately - two workers on a fixed client id
    # kick each other off forever and both keep reporting healthy.
    MQTT_INGEST_CLIENT_ID: str = "live-data-ingest"
    MQTT_ADMIN_CLIENT_ID: str = "live-data-admin"

    # ---- MQTT broker: what a device is told ----
    # The PUBLIC address that goes into the enrolment response and therefore into
    # the device's NVS, where it is stored once and never asked for again
    # (protocol 5.2, live-data-install-p1 3). Handing out MQTT_HOST - a compose
    # service name - would brick every enrolled device, and the only fix is a
    # physical site visit to a box with no keyboard. The two are asserted to
    # differ in staging/production below. D-3: mqtt.optimce.be.
    BROKER_PUBLIC_HOST: str = "mqtt.optimce.be"
    BROKER_PUBLIC_PORT: int = 8883
    BROKER_PUBLIC_TLS: bool = True

    # ---- Ingest validation (plan 7.4, protocol 4.2) ----
    # Acceptance window. RAW RETENTION MUST EXCEED IT (plan 6.4): with a 35-day
    # window and a retention that ever drops below it, a legitimate late message
    # targets a dropped partition and fails.
    INGEST_MAX_FUTURE_SECONDS: int = 300  # ts_in_future - protocol 4.2: +5 min
    INGEST_MAX_AGE_DAYS: int = 35  # ts_too_old - a drifted clock, not a backlog
    INGEST_INTERVAL_SECONDS: int = 900  # the product's granularity (plan 2)
    INGEST_MAX_BATCH: int = 200  # protocol 3.3
    # Default per-interval energy ceiling, overridable per device
    # (`device.max_wh_per_interval`). Applied to import_wh + export_wh: the
    # physical statement is that total energy through the connection in one
    # interval cannot exceed capacity x interval. (`both_directions_positive` was
    # DROPPED as a rejection - see domain/reasons.py.)
    INGEST_DEFAULT_MAX_WH_PER_INTERVAL: float = 50_000.0
    # implausible_production, capacity half. `device.capacity_kva` is a snapshot of
    # the CRM's `meter_data.total_generating_capacity`, which is kVA - the AC
    # INJECTION LIMIT, not the DC panel peak (plan deviation 4). A PV array is
    # routinely oversized against its inverter, so this CLIPS; it never trusts.
    INGEST_CAPACITY_TOLERANCE: float = 1.1
    # implausible_production, night half. A NAIVE local-hour window, not a solar
    # position calculation. It is deliberately narrow: Brussels has civil twilight
    # past 22:00 in June, so anything wider produces false rejections in summer.
    INGEST_NIGHT_START_HOUR_LOCAL: int = 23
    INGEST_NIGHT_END_HOUR_LOCAL: int = 4
    INGEST_TIMEZONE: str = "Europe/Brussels"
    # Consecutive database failures after which the worker DISCONNECTS from the
    # broker on purpose. See worker/ingest.py: aiomqtt 2.x PUBACKs every QoS-1
    # message before any DB write, so the only durable buffer is the broker's own
    # persistent session. Disconnecting pushes the backlog there, where
    # max_queued_messages bounds it and logs; staying connected grows an unbounded
    # in-memory deque until an OOM kill discards everything acked-but-unwritten.
    INGEST_DB_FAILURES_BEFORE_DISCONNECT: int = 5

    # ---- The live-data subscription, as the worker and scheduler see it (D-12) ----
    # How long the set of subscribed communities is trusted before the CRM is read
    # again. The lag is at most this, IN BOTH DIRECTIONS: a community switched off
    # keeps being ingested for up to this long, and one switched back on keeps
    # being discarded for up to this long. The API gate has no cache, so it flips
    # at once. 1..3600, asserted below: 0 would read the CRM on every message, and
    # an hour is already the outer edge of "switching it off stops it".
    SUBSCRIPTION_CACHE_TTL_SECONDS: int = 60

    # ---- Rollups and the scheduler (plan 6.3) ----
    # The tick runs on the wall-clock grid, so ticks land at the same minutes past
    # the hour on every replica and across restarts. 60 % this == 0 is asserted at
    # boot: a value like 7 would drift the grid every hour and make "the tick that
    # closes an hour" a moving target.
    #
    # READ BY THE API TOO: `/ops/health` and `/summary` call the rollups stale
    # after `domain.rollup_freshness.STALE_AFTER_TICKS` of these. Set it on the
    # scheduler alone and the API measures staleness in the wrong ticks - so it
    # belongs in the env shared by both containers, never in one service's block.
    ROLLUP_TICK_MINUTES: int = 15
    # Offset into the slot. A device publishing at :00 has to arrive, be validated
    # and be committed before the tick that closes its interval reads the table;
    # ticking exactly on the grid races every device in the fleet at once.
    ROLLUP_TICK_OFFSET_SECONDS: int = 90
    # Ownership changes a few times a year. Hourly is already generous, and the
    # refresh crosses a database boundary - which is why it has its own advisory
    # lock rather than sharing the rollup one. A device on a NEW EAN does not
    # wait for the hour: `scheduler.ownership_is_due` refreshes on the next tick,
    # or every grid term it contributes to stays withheld below k meanwhile.
    OWNERSHIP_REFRESH_MINUTES: int = 60
    # When the daily maintenance jobs run, UTC. 02:00 is after the day rollup for
    # the Belgian day has closed in both DST states (00:00 local is 22:00 or 23:00
    # UTC) and well before anyone looks at a dashboard.
    MAINTENANCE_HOUR_UTC: int = 2

    # ---- Retention (plan 6.4, plan 16) ----
    # 13 rolling months, so a full year plus the current month is always present.
    # MUST EXCEED THE INGEST ACCEPTANCE WINDOW - asserted at boot. A retention that
    # drops below it means a legitimate 35-day-old message targets a partition that
    # no longer exists.
    RETENTION_RAW_MONTHS: int = 13
    # The hour rollup must outlive the raw data it is derived from, and for the
    # same reason: a dirty bucket can be 35 days old, and recomputing it needs the
    # raw rows. Also asserted.
    RETENTION_ROLLUP_DEVICE_HOUR_MONTHS: int = 13
    # Per-device DAILY energy is still personal data - plan 16 calls "daily rollups
    # unlimited" unlimited retention of personal data, and caps it.
    RETENTION_ROLLUP_DEVICE_DAY_MONTHS: int = 13
    RETENTION_ROLLUP_COMMUNITY_HOUR_MONTHS: int = 13
    # Per sharing operation (D-14). NOT exempt like the community day: an
    # operation of two households is two households, so both are personal data.
    # The hour must outlive the acceptance window for the same reason as the
    # device hour - asserted below.
    RETENTION_ROLLUP_OPERATION_HOUR_MONTHS: int = 13
    RETENTION_ROLLUP_OPERATION_DAY_MONTHS: int = 13
    # `rollup_community_day` HAS NO RETENTION, deliberately, and there is no
    # setting for it. One row per community per day, with no device and no member
    # on it - nothing personal survives the aggregation, and it is the only series
    # that can answer "how did we do last year". A setting here would invite
    # someone to cap it.
    #
    # `ingest_dead_letter`, in DAYS rather than months: the table is not
    # partitioned, so there is no month boundary to align to. Rows older than
    # this are deleted by the nightly retention job (worker/retention.py).
    #
    # 90, because the table's real reader is a person, late. /ops/health counts
    # only the last 24 h; the runbook's triage and a support question are what
    # reach further back - and a gap noticed at the monthly billing run is
    # investigated weeks after the month it happened in. A quarter covers that
    # month, the one it is noticed in and the one it is looked at in. A connector
    # failing every 15 minutes for ever then leaves ~8,600 rows behind it, not an
    # unbounded number.
    #
    # Bounded both ways at boot, below. The floor is the 24 h /ops/health reads.
    # The ceiling is the raw retention: a dead letter keeps up to 2 KB of the
    # rejected payload - a household's quarter-hourly readings, personal data -
    # and a rejected reading must not outlive every accepted one.
    RETENTION_DEAD_LETTER_DAYS: int = 90

    # ---- Enrolment (plan 8.2, 8.3) ----
    ENROLMENT_TOKEN_TTL_HOURS: int = 72  # protocol 5.1
    # The claim lease. The token is CLAIMED, not consumed, before the broker call:
    # a 504 between the broker command and the response would otherwise burn a
    # credential the member never received, on a keyboard-less device in a
    # basement. Short, because a retry inside the lease is refused.
    ENROLMENT_CLAIM_LEASE_SECONDS: int = 30

    # ---- CORS ----
    ALLOW_ORIGIN: str = "*"

    LOGGING_TOKEN: str = ""
    LOGGING_TRACES_URL: str = ""
    LOGGING_LOGS_URL: str = ""
    LOGGING_METRICS_URL: str = ""

    @model_validator(mode="after")
    def validate_env_config(self) -> "Settings":
        # ---- UNCONDITIONAL. Not gated on ENV, and that is the point. ----
        #
        # These are arithmetic relationships between settings. They are equally
        # wrong in `local` as in production, and an assertion that does not fire
        # under ENV=test cannot be covered by the suite that would have caught the
        # mistake - so gating them would disarm exactly the tests written for them.
        if self.RETENTION_RAW_MONTHS * 28 <= self.INGEST_MAX_AGE_DAYS:
            raise ValueError(
                f"RETENTION_RAW_MONTHS ({self.RETENTION_RAW_MONTHS}) must exceed "
                f"INGEST_MAX_AGE_DAYS ({self.INGEST_MAX_AGE_DAYS}): a legitimate late "
                "message would target a partition that has already been dropped"
            )
        if self.RETENTION_ROLLUP_DEVICE_HOUR_MONTHS * 28 <= self.INGEST_MAX_AGE_DAYS:
            raise ValueError(
                "RETENTION_ROLLUP_DEVICE_HOUR_MONTHS must exceed INGEST_MAX_AGE_DAYS: "
                "day rollups derive from hour rollups, and a dirty bucket can be as "
                "old as the acceptance window"
            )
        if self.RETENTION_ROLLUP_OPERATION_HOUR_MONTHS * 28 <= self.INGEST_MAX_AGE_DAYS:
            raise ValueError(
                "RETENTION_ROLLUP_OPERATION_HOUR_MONTHS must exceed INGEST_MAX_AGE_DAYS: "
                "operation days derive from operation hours, and a dirty bucket can be "
                "as old as the acceptance window"
            )
        if self.ROLLUP_TICK_MINUTES <= 0 or 60 % self.ROLLUP_TICK_MINUTES != 0:
            raise ValueError(
                f"60 must be divisible by ROLLUP_TICK_MINUTES ({self.ROLLUP_TICK_MINUTES}); "
                "otherwise the wall-clock grid drifts every hour and the tick that "
                "closes an hour is a different one each time"
            )
        # 28 is February, deliberately: the assertion has to hold in the worst
        # month rather than the average one.
        if MONTHS_BACK * 28 < self.INGEST_MAX_AGE_DAYS:
            raise ValueError(
                f"domain.partitions.MONTHS_BACK ({MONTHS_BACK}) does not cover "
                f"INGEST_MAX_AGE_DAYS ({self.INGEST_MAX_AGE_DAYS}): a legitimate late "
                "message would land in the DEFAULT partition on a fresh database"
            )
        if not 1 <= self.SUBSCRIPTION_CACHE_TTL_SECONDS <= 3600:
            raise ValueError(
                f"SUBSCRIPTION_CACHE_TTL_SECONDS ({self.SUBSCRIPTION_CACHE_TTL_SECONDS}) must be "
                "between 1 and 3600: below that every MQTT message reads the CRM, above it "
                "switching live data off stops nothing for hours"
            )
        # x 28 is February once more, and here the SHORTEST month is the safe
        # side: raw rows are kept at least RETENTION_RAW_MONTHS calendar months,
        # so a ceiling counted in 28-day months can never pass them.
        dead_letter_ceiling = self.RETENTION_RAW_MONTHS * 28
        if not 1 <= self.RETENTION_DEAD_LETTER_DAYS <= dead_letter_ceiling:
            raise ValueError(
                f"RETENTION_DEAD_LETTER_DAYS ({self.RETENTION_DEAD_LETTER_DAYS}) must be between "
                f"1 and RETENTION_RAW_MONTHS x 28 ({dead_letter_ceiling}): below a day the prune "
                "deletes the last 24 hours /ops/health counts, and above the raw retention a "
                "rejected reading outlives every accepted one"
            )
        if self.ENV != Environment.LOCAL:
            origins = [o.strip() for o in self.ALLOW_ORIGIN.split(",") if o.strip()]
            if not origins:
                raise ValueError(
                    "ALLOW_ORIGIN is required when ENV is not local; "
                    "set it explicitly in .env.{env} (no implicit fallback to '*')"
                )
            if "*" in self.ALLOW_ORIGIN:
                raise ValueError("Wildcard CORS not allowed in staging/production")
            if not self.CRM_DATABASE_URL.strip():
                raise ValueError("CRM_DATABASE_URL is required when ENV is not local")
            if not self.LOCAL_DATABASE_URL.strip():
                raise ValueError("LOCAL_DATABASE_URL is required when ENV is not local")
        if self.ENV in (Environment.STAGING, Environment.PRODUCTION):
            if not self.MQTT_ADMIN_USERNAME.strip():
                raise ValueError("MQTT_ADMIN_USERNAME is required in staging/production")
            if not self.MQTT_ADMIN_PASSWORD.strip():
                raise ValueError("MQTT_ADMIN_PASSWORD is required in staging/production")
            # The one boot assertion that prevents a fleet-wide bricking. A device
            # stores broker.host once and never asks again, so shipping a compose
            # service name is unrecoverable without visiting every basement.
            if not self.BROKER_PUBLIC_HOST.strip():
                raise ValueError("BROKER_PUBLIC_HOST is required in staging/production")
            if self.BROKER_PUBLIC_HOST.strip() == self.MQTT_HOST.strip():
                raise ValueError(
                    "BROKER_PUBLIC_HOST must differ from MQTT_HOST: the former is "
                    "written into every device's NVS and can never be changed "
                    "remotely; the latter is an address on the internal network"
                )
            if not self.BROKER_PUBLIC_TLS:
                raise ValueError("BROKER_PUBLIC_TLS must be true in staging/production")
        if self.ENV == Environment.PRODUCTION:
            if not self.LOGGING_TOKEN:
                raise ValueError("LOGGING_TOKEN required for staging/production")
            if not self.LOGGING_LOGS_URL:
                raise ValueError("LOGGING_LOGS_URL required for staging/production")
            if not self.LOGGING_METRICS_URL:
                raise ValueError("LOGGING_METRICS_URL required for staging/production")
        return self


settings = Settings()
