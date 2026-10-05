-- ============================================================================
-- 0002_device_production_chain.sql
--
-- Forward-only, applied by hand or by provision.sh. The SAME BLOCK is present
-- in scripts/sql/schema.sql - provision.sh applies a schema only to a database
-- with no relations, so schema.sql is never re-applied to a live one and the
-- two files are maintained in parallel. tests/test_schema_migration_parity.py
-- is what keeps them equal.
-- ============================================================================

-- ============================================================================
-- MIGRATION 0002 - device.production_chain  (build step 9)
--
-- RE-RUNNABLE, like everything in this directory. `ADD COLUMN IF NOT EXISTS`
-- rather than a bare ADD: tests/conftest.py applies schema.sql and then every
-- migration on top of it, which is what proves each one can be applied twice.
--
-- shared.const.ProductionChain, mirroring crm-backend's enum:
--   1 PHOTOVOLTAIC, 2 WIND, 3 HYDRO, 4 BIOMASS, 5 BIOGAS, 6 COGEN_FOSSIL, 7 OTHER.
--
-- NULLABLE, and null is a real answer rather than a default: `meter_data`
-- carries no chain for a meter nobody classified, and the registry treats an
-- unknown chain as matching NO method. Defaulting it to PHOTOVOLTAIC because
-- that is the common case would silently forecast a hydro installation with a
-- solar model - the one failure this column exists to make impossible.
--
-- A SNAPSHOT, taken at device creation alongside capacity_kva, for the reason
-- that column states: the forecast job runs per device in the worker, and a CRM
-- round trip per device per run is the cost this service already declined.
-- ============================================================================

ALTER TABLE device ADD COLUMN IF NOT EXISTS production_chain SMALLINT;

INSERT INTO schema_version (version, description)
VALUES (3, 'device.production_chain for the forecast seam (build step 9)')
ON CONFLICT (version) DO NOTHING;
