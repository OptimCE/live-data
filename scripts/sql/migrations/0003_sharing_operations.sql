-- ============================================================================
-- 0003_sharing_operations.sql
--
-- Forward-only, applied by hand. The SAME BLOCK is present in
-- scripts/sql/schema.sql - provision.sh applies a schema only to a database
-- with no relations, so schema.sql is never re-applied to a live one and the
-- two files are maintained in parallel. tests/test_schema_migration_parity.py
-- is what keeps them equal.
-- ============================================================================

-- ============================================================================
-- MIGRATION 0003 - sharing operations  (decision D-14, 2026-10-04)
--
-- RE-RUNNABLE, like everything in this directory.
--
-- What it adds:
--   * device_owner_window.id_sharing_operation - the CRM window's operation,
--     projected with the member by worker/ownership.py.
--   * rollup_operation_hour / _day - one row per (community, operation, bucket)
--     with the operation's import, export, production and the ESTIMATED energy
--     shared inside it, plus one REMAINDER row per bucket (id 0) for every
--     device that belongs to no operation at that bucket.
--   * rollup_community_day.n_members_min - the day's LEAST-populated hour.
--
-- ---------------------------------------------------------------------------
-- THE REMAINDER ROW (id_sharing_operation = 0) IS A PRIVACY DEVICE.
--
-- Devices in no operation (none in the CRM, an ambiguous window, no window, a
-- meter still WAITING_GRD) go to row 0. Every device-hour then lands in exactly
-- one row, so the rows of a bucket sum to the community total EXACTLY. The
-- global view is published only when subtracting the visible operations from it
-- cannot expose fewer than k members (domain/kanon.py), and that test needs to
-- know whether anything outside the visible operations exists at all. Counting
-- devices cannot tell: a day's MAX device counts do not add up across rows.
-- A row can. CRM ids are identity columns from 1, so 0 is free.
--
-- shared_wh is NULL exactly on row 0 (nothing is shared outside an operation)
-- and never NULL elsewhere: the CHECK makes both halves structural.
--
-- ---------------------------------------------------------------------------
-- shared_wh IS AN ESTIMATE, AND NOT AN UPPER BOUND.
--
-- Per quarter-hour, LEAST(sum of export, sum of import) over the operation's
-- MONITORED meters, summed into the hour. Never computed from hourly sums: a
-- noon surplus cannot cover an evening offtake, and the hourly form says it
-- can. The DSO's figure may be lower (its allocation key may share less) or
-- higher (consumption at meters with no device). The DSO's data stays the only
-- basis for allocation keys and invoicing.
--
-- ---------------------------------------------------------------------------
-- PARTITIONED, KEPT 13 MONTHS. An operation of two households is two
-- households: unlike the community aggregate, these rows are personal data, so
-- they get the device rollups' retention and its one code path (the registry).
--
-- Their partitions MIRROR the device rollups' existing ones (the DO block):
-- every operation row is derived from a device-hour row, so a month that holds
-- device rollups needs an operation partition too - or the first backfill lands
-- history in *_default and /health/readiness goes red. live_ensure_monthly_
-- partitions alone would only cover two months back.
-- ============================================================================

ALTER TABLE device_owner_window ADD COLUMN IF NOT EXISTS id_sharing_operation INTEGER;

ALTER TABLE rollup_community_day ADD COLUMN IF NOT EXISTS n_members_min SMALLINT;

CREATE TABLE IF NOT EXISTS rollup_operation_hour (
    id_community            INTEGER          NOT NULL,
    -- 0 = the remainder: devices in no operation at this bucket.
    id_sharing_operation    INTEGER          NOT NULL,
    bucket                  TIMESTAMPTZ      NOT NULL,
    import_wh               DOUBLE PRECISION NOT NULL,
    export_wh               DOUBLE PRECISION NOT NULL,
    production_wh           DOUBLE PRECISION,
    shared_wh               DOUBLE PRECISION,
    n_devices               SMALLINT         NOT NULL,
    n_devices_production    SMALLINT         NOT NULL,
    -- Distinct members whose window put a device here, at this bucket's date.
    -- A lower bound, like the community's: an unattributed device adds energy
    -- and no member, so k fails closed.
    n_members               SMALLINT         NOT NULL,
    computed_at             TIMESTAMPTZ      NOT NULL,
    PRIMARY KEY (id_community, id_sharing_operation, bucket),
    CONSTRAINT ck_rollup_operation_hour_operation CHECK (id_sharing_operation >= 0),
    CONSTRAINT ck_rollup_operation_hour_shared
        CHECK ((id_sharing_operation = 0) = (shared_wh IS NULL))
) PARTITION BY RANGE (bucket);

CREATE TABLE IF NOT EXISTS rollup_operation_hour_default
    PARTITION OF rollup_operation_hour DEFAULT;

CREATE TABLE IF NOT EXISTS rollup_operation_day (
    id_community            INTEGER          NOT NULL,
    id_sharing_operation    INTEGER          NOT NULL,
    -- LOCAL midnight (Europe/Brussels), like every day bucket here.
    bucket                  TIMESTAMPTZ      NOT NULL,
    import_wh               DOUBLE PRECISION NOT NULL,
    export_wh               DOUBLE PRECISION NOT NULL,
    production_wh           DOUBLE PRECISION,
    shared_wh               DOUBLE PRECISION,
    n_devices               SMALLINT         NOT NULL,
    n_devices_production    SMALLINT         NOT NULL,
    -- MAX over the day's hours, as for the community: a lower bound on the day.
    n_members               SMALLINT         NOT NULL,
    -- MIN over the day's hours. A day is published only if EVERY hour of it
    -- was: otherwise "day minus its published hours" is the withheld hours.
    n_members_min           SMALLINT         NOT NULL,
    n_hours                 SMALLINT         NOT NULL,
    computed_at             TIMESTAMPTZ      NOT NULL,
    PRIMARY KEY (id_community, id_sharing_operation, bucket),
    CONSTRAINT ck_rollup_operation_day_operation CHECK (id_sharing_operation >= 0),
    CONSTRAINT ck_rollup_operation_day_shared
        CHECK ((id_sharing_operation = 0) = (shared_wh IS NULL))
) PARTITION BY RANGE (bucket);

CREATE TABLE IF NOT EXISTS rollup_operation_day_default
    PARTITION OF rollup_operation_day DEFAULT;

INSERT INTO live_partitioned_table (table_name) VALUES
    ('rollup_operation_hour'),
    ('rollup_operation_day')
ON CONFLICT DO NOTHING;

-- Mirror every monthly partition the device rollups already have, then the
-- usual window. Names and bounds follow live_ensure_monthly_partitions exactly
-- (`<table>_YYYY_MM`, UTC month bounds), so the two never disagree.
DO $$
DECLARE
  pair         TEXT[];
  suffix       TEXT;
  lower_bound  TIMESTAMPTZ;
  upper_bound  TIMESTAMPTZ;
BEGIN
  FOREACH pair SLICE 1 IN ARRAY ARRAY[
      ARRAY['rollup_device_hour', 'rollup_operation_hour'],
      ARRAY['rollup_device_day',  'rollup_operation_day']
  ] LOOP
    FOR suffix IN
      SELECT right(c.relname, 7)
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
       WHERE i.inhparent = pair[1]::regclass
         AND c.relname ~ '_[0-9]{4}_[0-9]{2}$'
    LOOP
      lower_bound := (to_date(suffix, 'YYYY_MM')::timestamp) AT TIME ZONE 'UTC';
      upper_bound := ((to_date(suffix, 'YYYY_MM') + INTERVAL '1 month')::timestamp)
                     AT TIME ZONE 'UTC';
      EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
        pair[2] || '_' || suffix, pair[2], lower_bound, upper_bound
      );
    END LOOP;
  END LOOP;
END $$;

SELECT live_ensure_monthly_partitions(6, 2);

INSERT INTO schema_version (version, description)
VALUES (4, 'Sharing operations: owner-window operation, operation rollups (D-14)')
ON CONFLICT (version) DO NOTHING;
