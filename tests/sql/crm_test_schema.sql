-- Test-only DDL for the CRM tables this service touches.
--
-- The real CRM schema is owned by crm-backend. live-data SELECTs from it as the
-- `live_data_svc` role and INSERTs audit rows; scripts/sql/schema.sql declares
-- the LOCAL (owned) tables only. Tests run against a single Postgres instance,
-- so the minimum CRM DDL the suite needs is mirrored here.
--
-- KEEP COLUMN TYPES IDENTICAL TO PRODUCTION. Every type below was read off
-- crm-backend/src/modules/**/domain/*.models.ts rather than guessed: getting one
-- wrong produces a suite that passes against a fiction.
--
-- Deliberately ABSENT: `notification`, `outbound_message`, `community_user`.
-- live-data produces no notifications in phase 1, so `live_data_svc` has no
-- INSERT grant on them and nothing here should be able to pretend otherwise.

-- ---- community -------------------------------------------------------------
-- Mirrors core/database/models.py::Community.
CREATE TABLE IF NOT EXISTS community (
    id                       INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name                     VARCHAR(255) NOT NULL UNIQUE,
    auth_community_id        VARCHAR(255) NOT NULL UNIQUE,
    regulator                VARCHAR(32)  NOT NULL DEFAULT 'BE-WAL-CWAPE',
    vat_number               VARCHAR(32),
    legal_name               VARCHAR(255),
    iban                     VARCHAR(34),
    account_holder_name      VARCHAR(255),
    headquarters_address_id  INTEGER,
    created_at               TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at               TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ---- community_subscription -------------------------------------------------
-- The per-annexe feature gate. Checked by require_feature() on the
-- authenticated surface, and BY HAND (unscoped, with an explicit bind
-- parameter) on the public enrolment leg, where require_feature cannot run
-- because there is neither a user nor an X-Community-ID header.
--
-- `feature` is a free-form VARCHAR(64) in production with no enum constraining
-- the vocabulary, which is why FeatureName.LIVE_DATA and the string in
-- crm-backend/config/annexes-services.json have to agree by convention.
CREATE TABLE IF NOT EXISTS community_subscription (
    id           INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_community INTEGER     NOT NULL,
    feature      VARCHAR(64) NOT NULL,
    is_active    BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_community_subscription_community_feature
        UNIQUE (id_community, feature)
);

CREATE INDEX IF NOT EXISTS idx_community_subscription_id_community
    ON community_subscription (id_community);

-- ---- address ----------------------------------------------------------------
-- Coordinates live here, reached as meter.id_address -> address. There is no
-- "production site" entity in this CRM (plan deviation 3): a production site IS
-- a meter whose meter_data row carries injection_status and
-- total_generating_capacity.
--
-- `number` is VARCHAR(32), not INT: it changed on 2026-08-30 because `12A` is a
-- real Belgian house number.
CREATE TABLE IF NOT EXISTS address (
    id           INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    street       VARCHAR(255),
    number       VARCHAR(32),
    postcode     VARCHAR(16),
    supplement   VARCHAR(255),
    city         VARCHAR(255),
    country      CHAR(2) NOT NULL DEFAULT 'BE',
    id_community INTEGER,
    latitude     DOUBLE PRECISION,
    longitude    DOUBLE PRECISION,
    created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ---- meter ------------------------------------------------------------------
-- Mirrors crm-backend Meter. Note the EAN is the PRIMARY KEY - it is a natural
-- key, and that is why `device.ean` in the local schema is a plain VARCHAR and
-- never a foreign key: it points into another database.
CREATE TABLE IF NOT EXISTS meter (
    ean               VARCHAR(64)  PRIMARY KEY,
    meter_number      VARCHAR(255) NOT NULL,
    id_address        INTEGER,
    id_community      INTEGER      NOT NULL,
    tarif_group       INTEGER,
    phases_number     INTEGER,
    reading_frequency INTEGER,
    created_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_meter_community ON meter (id_community);

-- ---- meter_data -------------------------------------------------------------
-- TIME-SLICED: one EAN has several rows, and the holder can change mid-month.
-- `status` is MeterDataStatus: 1 ACTIVE, 2 INACTIVE, 3 WAITING_GRD,
-- 4 WAITING_MANAGER.
--
-- `total_generating_capacity` is a plain FLOAT with NO UNIT IN THE DDL, and the
-- unit is the whole point: all four locales describe it as the maximum power in
-- kVA the installation can INJECT - the AC inverter/grid-connection limit, not
-- the DC panel peak (kWc) a forecasting model would want. A PV array is
-- routinely oversized against its inverter. live-data snapshots it onto
-- `device.capacity_kva`, named with its unit so the confusion cannot survive a
-- code review (plan deviation 4).
--
-- NOTE: production has NO constraint preventing overlapping ACTIVE windows for
-- one EAN. billing guards against it with a separate find_ownership_overlaps
-- pre-flight before attributing anything. live-data inherits that precondition
-- in ports/crm_core.py, which resolves it differently: a background projection
-- has nobody to refuse to, so it FLAGS the window (device_owner_window.ambiguous)
-- and drops its membership while still counting its energy.
CREATE TABLE IF NOT EXISTS meter_data (
    id                        INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ean                       VARCHAR(64) NOT NULL,
    id_member                 INTEGER,
    status                    INTEGER     NOT NULL,
    sampling_power            DOUBLE PRECISION,
    rate                      INTEGER,
    client_type               INTEGER,
    start_date                DATE        NOT NULL,
    end_date                  DATE,
    injection_status          INTEGER,
    production_chain          INTEGER,
    total_generating_capacity DOUBLE PRECISION,
    -- The window's sharing operation (D-14). NULL when the meter is in none. The
    -- real column has an FK to sharing_operation; omitted here so factories can
    -- use arbitrary ids, the way `id_member` already does.
    id_sharing_operation      INTEGER,
    created_at                TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_meter_data_ean ON meter_data (ean);

-- ---- sharing_operation ------------------------------------------------------
-- Read for the operation names on the dashboard and for the member's own
-- operations (ports/crm_operations.py). The real table has no dates and no
-- status: membership is time-sliced on `meter_data`, not here.
CREATE TABLE IF NOT EXISTS sharing_operation (
    id           INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name         VARCHAR(255) NOT NULL,
    type         INTEGER,
    is_public    BOOLEAN   NOT NULL DEFAULT FALSE,
    id_community INTEGER   NOT NULL,
    created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ---- member -----------------------------------------------------------------
-- The community scope of "my operations" comes from HERE: `user_member_link`
-- carries no id_community.
CREATE TABLE IF NOT EXISTS member (
    id           INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name         VARCHAR(255) NOT NULL,
    member_type  INTEGER   NOT NULL DEFAULT 1,
    status       INTEGER,
    id_community INTEGER   NOT NULL,
    created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ---- app_user ---------------------------------------------------------------
-- Read by core/audit_log/service.py to denormalise the writer's identity onto
-- each audit row.
CREATE TABLE IF NOT EXISTS app_user (
    id           INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    auth_user_id VARCHAR(255) NOT NULL UNIQUE,
    email        VARCHAR(256) NOT NULL,
    locale       VARCHAR(8),
    first_name   TEXT,
    last_name    TEXT
);

-- ---- user_member_link -------------------------------------------------------
-- auth user <-> member, the join billing and administrative-document use for
-- their "mine" reads. No uniqueness and no id_community, like the real table.
CREATE TABLE IF NOT EXISTS user_member_link (
    id          INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_user     INTEGER   NOT NULL REFERENCES app_user (id),
    id_member   INTEGER   NOT NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_user_member_link_member ON user_member_link (id_member);
CREATE INDEX IF NOT EXISTS idx_user_member_link_user ON user_member_link (id_user);

-- ---- audit_log --------------------------------------------------------------
-- The one CRM table live-data WRITES. Mirrors core/database/models.py.
--
-- The grant on it is the classic silent failure on this platform: the annex
-- audit write rides the caller's CRM session inside a SAVEPOINT under a blanket
-- `except Exception`, so a MISSING GRANT DOES NOT RAISE. The business write
-- commits, the API returns 200, and the row vanishes with one log line.
-- postgres/verify/positive-writes.sh is what proves the grant works; never
-- accept "no 500s" as evidence.
--
-- `id` is BIGSERIAL in production, which is why live_data_svc needs USAGE on
-- audit_log_id_seq as well as INSERT on the table - GENERATED ALWAYS AS IDENTITY
-- would need no sequence grant, and that asymmetry is documented in
-- 30-crm-grants.sql.
-- Column-for-column with crm-backend/database_script/2026-05-27_audit_log.sql.
-- Note `timestamp`, NOT `created_at`; `source` and `entity_type` are NOT NULL;
-- `entity_id` is VARCHAR(64). Those four are easy to get subtly wrong, and a
-- wrong mirror produces a suite that passes against a fiction.
CREATE TABLE IF NOT EXISTS audit_log (
    id           BIGSERIAL    PRIMARY KEY,
    id_community INTEGER      REFERENCES community (id) ON DELETE CASCADE,
    timestamp    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    action       VARCHAR(128) NOT NULL,
    source       VARCHAR(32)  NOT NULL,
    entity_type  VARCHAR(64)  NOT NULL,
    entity_id    VARCHAR(64),
    user_id      INTEGER,
    user_email   VARCHAR(256),
    payload      JSONB        NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_audit_log_community_timestamp
    ON audit_log (id_community, timestamp DESC);
