-- ============================================================================
-- MIGRATION 0001 - rollups, ownership projection, forecast and consent
--
-- APPLIED BY HAND. There is no migration runner in this monorepo.
--
-- Why this file exists at all: postgres/provision/provision.sh applies a schema
-- only when the target database has NO relation of relkind 'r' or 'p', and
-- `measurement` is a partitioned parent ('p'). So scripts/sql/schema.sql is
-- never re-applied to a live database, and a table added only there would exist
-- on every fresh environment and on none of the running ones.
--
-- The DDL below is BYTE-IDENTICAL to the corresponding block in schema.sql, and
-- tests/test_schema_migration_parity.py provisions two databases - one from
-- schema.sql alone, one from schema.sql plus this file - and compares their
-- catalogs. tests/conftest.py applies both on every test run, which also proves
-- this file is re-runnable on top of a schema that already contains it.
--
--     psql -U live_data_svc -d live_data_local -f 0001_rollups_ownership_forecast.sql
--
-- Idempotent: every statement is IF NOT EXISTS / ON CONFLICT DO NOTHING / OR
-- REPLACE, except the deliberate DROP FUNCTION documented at the bottom.
-- ============================================================================

-- ============================================================================
-- STEP 7/8/9 TABLES  (schema_version 2)
--
-- The ten tables plan §6.1 defers past step 5. They arrive together, in one
-- migration, because they are useless apart: `n_members` needs the ownership
-- projection, the day rollups need the hour rollups, and the forecast tables
-- need somewhere for a method to write.
--
-- EVERY TABLE HERE CARRIES A LITERAL `id_community` COLUMN, including ones where
-- it is derivable. `core/database/with_community.py` hard-codes
-- `model.id_community` behind a `type: ignore`, so a table that names it
-- anything else cannot be scoped at all - and the failure is a runtime 500 in a
-- read path rather than a type error.
--
-- ENERGIES ARE DOUBLE PRECISION, NEVER REAL. `SUM(real)` returns `real`;
-- `measurement` already documents paying that once, and a float4 rollup column
-- would reintroduce the identical unfixable error one level up.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- device_owner_window  (step 7)
--
-- A local projection of CRM ownership, refreshed by the scheduler. It exists so
-- that `n_members` can be resolved AT A BUCKET'S INSTANT without reaching across
-- the database boundary inside the rollup tick.
--
-- `ean` is a plain column and never a foreign key - `meter` lives in crm_db.
--
-- NEVER DENORMALISE THE OWNER ONTO `device`. Ownership is time-sliced: one EAN
-- has several `meter_data` rows and can change holder mid-month. A cached owner
-- is the meter-transfer privacy leak - the previous occupant's credential keeps
-- publishing, attributed to them, about someone else's household.
--
-- `ambiguous` is the row billing does not need. Billing REFUSES a run (422) when
-- it finds overlapping ownership windows; a background projection cannot refuse,
-- so it flags the window instead. An ambiguous window's ENERGY is still counted -
-- excluding it would understate the community total - but its MEMBERSHIP is not,
-- because overcounting members weakens k and undercounting only over-suppresses.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS device_owner_window (
    id            INTEGER     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ean           VARCHAR(64) NOT NULL,
    id_community  INTEGER     NOT NULL,
    -- NULL when the CRM has no owner for this window. The energy still counts;
    -- the member does not.
    id_member     INTEGER,
    -- Brussels-local DATE bounds, mirroring `meter_data.start_date`/`end_date`.
    --
    -- CLOSED AT BOTH ENDS, because meter_data's are. Every existing reader in
    -- the platform tests them as `BETWEEN start_date AND COALESCE(end_date,
    -- 'infinity'::date)` - billing's attribution join and its overlap
    -- pre-flight, and this service's own ports/crm_read.py. Converting to a
    -- half-open form on the way in would mean `valid_to = end_date + 1`, and an
    -- off-by-one there is a whole day of one household's energy attributed to
    -- the wrong member - silently, and only on meters that changed hands, which
    -- is exactly the case this table exists to get right.
    --
    -- So two windows are ADJACENT, not overlapping, when one ends the day before
    -- the next begins: `end_date = next.start_date - 1` is the normal shape of a
    -- transfer and must not be flagged ambiguous. See domain/ownership.py.
    --
    -- `valid_to` NULL means open-ended.
    valid_from    DATE        NOT NULL,
    valid_to      DATE,
    ambiguous     BOOLEAN     NOT NULL DEFAULT FALSE,
    refreshed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Read by the correlated subquery that resolves a device's owner at a bucket.
-- That subquery is correlated rather than a LEFT JOIN precisely because a join
-- fans out on overlapping windows and MULTIPLIES the community energy.
CREATE INDEX IF NOT EXISTS ix_device_owner_window_ean_from
    ON device_owner_window (ean, valid_from DESC);

CREATE INDEX IF NOT EXISTS ix_device_owner_window_community
    ON device_owner_window (id_community);

-- ---------------------------------------------------------------------------
-- rollup_device_hour  (step 8) - PARTITIONED, and registered below.
--
-- plan §6.2: "the maintenance job takes a table list. A job that names only
-- `measurement` lets the rollups freeze about four months in while ingestion
-- goes on looking perfectly healthy."
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rollup_device_hour (
    id_device             INTEGER          NOT NULL,
    -- START of the hour, UTC. Derived from `measurement.ts`, which is the END of
    -- its interval, so a reading at 11:00:00Z lands in bucket 10:00:00Z. See
    -- domain/buckets.py - this one convention is most of step 8's risk.
    bucket                TIMESTAMPTZ      NOT NULL,
    id_community          INTEGER          NOT NULL,
    import_wh             DOUBLE PRECISION NOT NULL,
    export_wh             DOUBLE PRECISION NOT NULL,
    -- NULLABLE, and NEVER coalesced to 0. NULL means "this device does not
    -- measure production" - a P1 on a site that also consumes cannot see it
    -- (protocol §3.4). Writing 0 asserts "produced nothing", a different and
    -- false statement that then propagates into the community total.
    production_wh         DOUBLE PRECISION,
    -- Distinguishes a partial hour (< 4) from a closed one, which is what lets
    -- the hour in progress be published without lying about it.
    n_samples             SMALLINT         NOT NULL,
    n_production_samples  SMALLINT         NOT NULL,
    computed_at           TIMESTAMPTZ      NOT NULL,
    PRIMARY KEY (id_device, bucket)
) PARTITION BY RANGE (bucket);

CREATE TABLE IF NOT EXISTS rollup_device_hour_default
    PARTITION OF rollup_device_hour DEFAULT;

CREATE INDEX IF NOT EXISTS ix_rollup_device_hour_community_bucket
    ON rollup_device_hour (id_community, bucket);

-- ---------------------------------------------------------------------------
-- rollup_device_day  (step 8) - PARTITIONED.
--
-- `bucket` is the instant of LOCAL midnight (Europe/Brussels - see
-- shared/const.ROLLUP_DAY_TIMEZONE). Partitioned even though plan §6.1 names
-- only `_hour`, because §16 requires capping per-device daily retention and
-- partitioning gives the retention job exactly ONE code path, driven by the
-- registry. A second mechanism is a mechanism that gets forgotten.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rollup_device_day (
    id_device             INTEGER          NOT NULL,
    bucket                TIMESTAMPTZ      NOT NULL,
    id_community          INTEGER          NOT NULL,
    import_wh             DOUBLE PRECISION NOT NULL,
    export_wh             DOUBLE PRECISION NOT NULL,
    production_wh         DOUBLE PRECISION,
    -- 23 or 25 on the two DST days, and that is correct rather than a bug.
    n_hours               SMALLINT         NOT NULL,
    n_production_hours    SMALLINT         NOT NULL,
    computed_at           TIMESTAMPTZ      NOT NULL,
    PRIMARY KEY (id_device, bucket)
) PARTITION BY RANGE (bucket);

CREATE TABLE IF NOT EXISTS rollup_device_day_default
    PARTITION OF rollup_device_day DEFAULT;

CREATE INDEX IF NOT EXISTS ix_rollup_device_day_community_bucket
    ON rollup_device_day (id_community, bucket);

-- ---------------------------------------------------------------------------
-- rollup_community_hour / _day  (step 8) - NOT partitioned.
--
-- One row per community per bucket is four orders of magnitude smaller than
-- `measurement`; adding them to the registry would mean two more monthly
-- partitions a month for no operational benefit. §16 treats a community
-- aggregate as non-personal, which is why `_day` has no retention at all.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rollup_community_hour (
    id_community            INTEGER          NOT NULL,
    bucket                  TIMESTAMPTZ      NOT NULL,
    import_wh               DOUBLE PRECISION NOT NULL,
    export_wh               DOUBLE PRECISION NOT NULL,
    production_wh           DOUBLE PRECISION,
    n_devices               SMALLINT         NOT NULL,
    n_devices_production    SMALLINT         NOT NULL,
    -- n_members, NOT n_devices (plan §9.3). "A member with three meters is one
    -- member; counting devices makes the guarantee decorative." Resolved at THIS
    -- BUCKET'S instant, never at now() - resolving at now() would retroactively
    -- rewrite who was in a closed hour.
    n_members               SMALLINT         NOT NULL,
    -- Devices whose owner could not be resolved unambiguously at this bucket.
    -- Their energy is counted; their membership is not. k must threshold on a
    -- LOWER BOUND of distinct members: undercounting over-suppresses,
    -- overcounting is the privacy failure. Fail closed.
    n_devices_unattributed  SMALLINT         NOT NULL,
    computed_at             TIMESTAMPTZ      NOT NULL,
    PRIMARY KEY (id_community, bucket)
);

CREATE TABLE IF NOT EXISTS rollup_community_day (
    id_community            INTEGER          NOT NULL,
    bucket                  TIMESTAMPTZ      NOT NULL,
    import_wh               DOUBLE PRECISION NOT NULL,
    export_wh               DOUBLE PRECISION NOT NULL,
    production_wh           DOUBLE PRECISION,
    n_devices               SMALLINT         NOT NULL,
    n_devices_production    SMALLINT         NOT NULL,
    -- MAX over the day's hours, NEVER SUM. SUM gives a 5-member community an
    -- n_members of 120 and k then passes on a day that should be suppressed.
    -- MAX is a valid lower bound on the day's distinct union, so it fails closed.
    n_members               SMALLINT         NOT NULL,
    n_devices_unattributed  SMALLINT         NOT NULL,
    n_hours                 SMALLINT         NOT NULL,
    computed_at             TIMESTAMPTZ      NOT NULL,
    PRIMARY KEY (id_community, bucket)
);

-- ---------------------------------------------------------------------------
-- rollup_dirty  (step 8)
--
-- Buckets awaiting recompute. Granularity is COMMUNITY-hour rather than
-- device-hour because the unit of recomputation is the community hour:
-- `n_members` cannot be computed for one device. Device granularity would
-- produce four times the rows and the sweep would collapse them anyway.
--
-- Written in the SAME STATEMENT as the measurement, unconditionally, with no
-- guard band - see worker/ingest.py. Marking only "outside the window" loses a
-- bucket written at 10:59:59 when the tick fires at 11:00:02, and loses
-- everything ingested during a scheduler outage.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rollup_dirty (
    id_community  INTEGER     NOT NULL,
    bucket        TIMESTAMPTZ NOT NULL,
    marked_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id_community, bucket)
);

-- Not decoration. This is a delete/insert queue on the ingest hot path: a
-- 50-device community makes 50 speculative inserts per quarter-hour against one
-- key, 49 of which conflict. At the default scale factor the "tiny table"
-- becomes a bloated one and the PK prefix scan the drain depends on stops being
-- cheap.
ALTER TABLE rollup_dirty SET (autovacuum_vacuum_scale_factor = 0.0,
                              autovacuum_vacuum_threshold   = 50);

-- ---------------------------------------------------------------------------
-- forecast_production / device_forecast_method  (step 9) - INERT.
--
-- Created now so the contract is fixed before any method exists; nothing writes
-- them in phase 1. `method` and `method_version` are NOT optional (plan §10.3):
-- without them two methods cannot be compared, a retro-adjustment cannot be
-- attributed, and rows produced by a model since corrected cannot be found.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS forecast_production (
    ean             VARCHAR(64)      NOT NULL,
    bucket          TIMESTAMPTZ      NOT NULL,
    id_community    INTEGER          NOT NULL,
    wh              DOUBLE PRECISION NOT NULL,
    method          VARCHAR(64)      NOT NULL,
    method_version  VARCHAR(32)      NOT NULL,
    computed_at     TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ean, bucket, method)
);

CREATE INDEX IF NOT EXISTS ix_forecast_production_community_bucket
    ON forecast_production (id_community, bucket);

CREATE TABLE IF NOT EXISTS device_forecast_method (
    id_device   INTEGER     PRIMARY KEY REFERENCES device (id) ON DELETE CASCADE,
    method      VARCHAR(64) NOT NULL,
    params      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- OR REPLACE, not a bare CREATE: PostgreSQL has no CREATE TRIGGER IF NOT
-- EXISTS, so a bare one makes this file non-re-runnable - which the conftest
-- fixture, applying schema.sql and then every migration on top of it, catches
-- on every test run. Available since PG 14; the stack pins postgres:18.
CREATE OR REPLACE TRIGGER trg_device_forecast_method_updated_at
    BEFORE UPDATE ON device_forecast_method
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ---------------------------------------------------------------------------
-- consent / consent_event  (phase 2) - INERT.
--
-- Append-only with PER-FIELD `effective_from`, which is the only shape that
-- satisfies §9's per-field, per-direction retroactivity: a withdrawal applies to
-- the past by default, an addition does not. Read with
-- `DISTINCT ON (id_member, field) ... ORDER BY decided_at DESC, id DESC`.
--
-- Visibility is evaluated AT READ TIME, never at write time. Measurements are
-- always stored; a withdrawal masks them.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS consent (
    id              INTEGER     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_community    INTEGER     NOT NULL,
    id_member       INTEGER     NOT NULL,
    -- admin_can_view_individual | share_with_community | show_on_map
    field           VARCHAR(48) NOT NULL,
    granted         BOOLEAN     NOT NULL,
    -- NULL means "from the beginning of time" - a withdrawal applying to the
    -- past. An addition carries the instant it was decided.
    effective_from  TIMESTAMPTZ,
    decided_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_consent_member_field
    ON consent (id_member, field, decided_at DESC);

CREATE INDEX IF NOT EXISTS ix_consent_community
    ON consent (id_community);

CREATE TABLE IF NOT EXISTS consent_event (
    id            INTEGER     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_consent    INTEGER     NOT NULL REFERENCES consent (id) ON DELETE CASCADE,
    id_community  INTEGER     NOT NULL,
    actor         VARCHAR(64) NOT NULL,
    action        VARCHAR(32) NOT NULL,
    recorded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_consent_event_consent
    ON consent_event (id_consent);

-- ---------------------------------------------------------------------------
-- The partition registry gains the two partitioned rollups.
--
-- domain/partitions.PARTITIONED_TABLES carries the same list, and
-- tests/test_partitions.py asserts the two agree. That test is what stops the
-- failure plan §6.2 names by name.
-- ---------------------------------------------------------------------------
INSERT INTO live_partitioned_table (table_name) VALUES
    ('rollup_device_hour'),
    ('rollup_device_day')
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- live_ensure_monthly_partitions gains `months_back`.
--
-- The old signature only ever provisioned forward from the current month, so
-- nothing re-created a PAST month. With `INGEST_MAX_AGE_DAYS = 35` a legitimate
-- late message reaches two calendar months back; on a freshly provisioned or
-- restored database those partitions do not exist, the message lands in the
-- DEFAULT partition, and `/health/readiness` goes red on day one looking exactly
-- like an ingest bug.
--
-- DROP THEN CREATE, not CREATE OR REPLACE. Adding a parameter to a plpgsql
-- function creates a second OVERLOAD rather than replacing it, and a later
-- `live_ensure_monthly_partitions(6)` call then resolves ambiguously.
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS live_ensure_monthly_partitions(INTEGER);

CREATE OR REPLACE FUNCTION live_ensure_monthly_partitions(
    months_ahead INTEGER DEFAULT 6,
    months_back  INTEGER DEFAULT 2
)
RETURNS INTEGER AS $$
DECLARE
  tbl          RECORD;
  offset_m     INTEGER;
  lower_bound  TIMESTAMPTZ;
  upper_bound  TIMESTAMPTZ;
  child        TEXT;
  created      INTEGER := 0;
BEGIN
  FOR tbl IN SELECT table_name FROM live_partitioned_table LOOP
    FOR offset_m IN -months_back..months_ahead LOOP
      -- Anchored to UTC EXPLICITLY. date_trunc() on a timestamptz truncates in
      -- the SESSION's timezone, and provision.sh sets none - so without the
      -- round trip through UTC a boundary would land at 23:00 or 01:00 of the
      -- previous day whenever the session happened to be Europe/Brussels, and
      -- every partition in the database would be an hour wide of the Python
      -- helper that names it.
      lower_bound := (date_trunc('month', (now() AT TIME ZONE 'UTC'))
                      + make_interval(months => offset_m)) AT TIME ZONE 'UTC';
      upper_bound := (date_trunc('month', (now() AT TIME ZONE 'UTC'))
                      + make_interval(months => offset_m + 1)) AT TIME ZONE 'UTC';
      child := format('%s_%s', tbl.table_name, to_char(lower_bound AT TIME ZONE 'UTC', 'YYYY_MM'));

      -- IF NOT EXISTS makes the whole function idempotent, which is what lets it
      -- be called both from schema.sql and from a daily tick.
      --
      -- NOTE it can still FAIL, and that is not a bug in this function: creating
      -- a partition over a range the DEFAULT partition already holds rows in
      -- requires a scan proving the default is empty there. worker/partitions.py
      -- drains the default first. Without that drain this call starts failing
      -- about three months after launch, at 00:00 UTC on the 1st.
      EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
        child, tbl.table_name, lower_bound, upper_bound
      );
      created := created + 1;
    END LOOP;
  END LOOP;
  RETURN created;
END;
$$ LANGUAGE plpgsql;

-- Provision two months back, the current month, and six ahead, for every table
-- now in the registry.
SELECT live_ensure_monthly_partitions(6, 2);

INSERT INTO schema_version (version, description) VALUES
    (2, 'Rollups, ownership projection, forecast and consent tables (steps 7-9)')
ON CONFLICT DO NOTHING;
