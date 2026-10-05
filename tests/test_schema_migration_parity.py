"""`schema.sql` and the migrations describe the SAME database.

`migrations/0001_rollups_ownership_forecast.sql` names this file in its header as
the guard that keeps them equal. It did not exist until build step 9; this is it.

----------------------------------------------------------------------------
WHY TWO FILES EXIST AT ALL, AND WHY NOTHING ELSE KEEPS THEM EQUAL.

`postgres/provision/provision.sh` applies a schema ONLY to a database with no
relation of relkind 'r' or 'p'. So `scripts/sql/schema.sql` runs exactly once, on
a database that has never been provisioned, and is NEVER re-applied to a live
one. Every later change therefore has to land twice:

  - in `schema.sql`, for the next fresh environment;
  - in `scripts/sql/migrations/NNNN_*.sql`, for every database that already exists.

The two are maintained by hand and by convention, and the failure is silent in
the direction that matters: a column added to the migration and forgotten in
`schema.sql` works perfectly in dev, in staging and in production - everywhere
that upgraded - and is missing the first time someone provisions a NEW
environment. Which is usually production's disaster-recovery rehearsal.

`tests/conftest.py` applies schema.sql AND then the migrations, so it proves each
migration is RE-RUNNABLE. It cannot prove the two files agree, because it never
builds a database from one of them alone. This does.
----------------------------------------------------------------------------

TWO REAL DATABASES, NOT TWO SCHEMAS.

`schema.sql` names `public` explicitly (its teardown drops and recreates it) and
creates partitions whose names are global to a schema. Two `search_path`-switched
schemas in one database would collide on the first partition and on
`live_ensure_monthly_partitions`. Two databases cost about a second.
"""

import asyncio
from pathlib import Path

import asyncpg
import pytest

from tests.conftest import _ASYNCPG_DSN, CRM_TEST_SCHEMA_SQL, MIGRATIONS_DIR, SCHEMA_SQL

_FRESH_DB = "parity_fresh"
_UPGRADED_DB = "parity_upgraded"

# What "the same database" means, spelled out. Each query returns rows that are
# compared as sorted tuples.
#
# `pg_get_expr(relpartbound)` is in here because a partition attached over the
# wrong month is invisible to every other check: the column list matches, the
# indexes match, and rows quietly land in the default.
_SNAPSHOT_QUERIES = {
    "columns": """
        SELECT table_name, column_name, data_type, is_nullable, column_default
          FROM information_schema.columns
         WHERE table_schema = 'public'
         ORDER BY table_name, column_name
    """,
    "indexes": """
        SELECT tablename, indexname, indexdef
          FROM pg_indexes
         WHERE schemaname = 'public'
         ORDER BY tablename, indexname
    """,
    "constraints": """
        SELECT rel.relname, con.conname, pg_get_constraintdef(con.oid)
          FROM pg_constraint con
          JOIN pg_class rel ON rel.oid = con.conrelid
          JOIN pg_namespace ns ON ns.oid = rel.relnamespace
         WHERE ns.nspname = 'public'
         ORDER BY rel.relname, con.conname
    """,
    "partitions": """
        SELECT parent.relname, child.relname, pg_get_expr(child.relpartbound, child.oid)
          FROM pg_inherits i
          JOIN pg_class child ON child.oid = i.inhrelid
          JOIN pg_class parent ON parent.oid = i.inhparent
         ORDER BY parent.relname, child.relname
    """,
    "functions": """
        SELECT p.proname, pg_get_functiondef(p.oid)
          FROM pg_proc p
          JOIN pg_namespace ns ON ns.oid = p.pronamespace
         WHERE ns.nspname = 'public'
         ORDER BY p.proname, pg_get_functiondef(p.oid)
    """,
    "schema_version": "SELECT version FROM schema_version ORDER BY version",
}


async def _snapshot(database: str) -> dict[str, list[tuple]]:
    conn = await asyncpg.connect(_ASYNCPG_DSN.rsplit("/", 1)[0] + "/" + database)
    try:
        return {
            name: [tuple(row) for row in await conn.fetch(query)]
            for name, query in _SNAPSHOT_QUERIES.items()
        }
    finally:
        await conn.close()


async def _build(database: str, *, apply_migrations: bool) -> None:
    admin = await asyncpg.connect(_ASYNCPG_DSN)
    try:
        # CREATE DATABASE cannot run inside a transaction block, which is why
        # this is a separate connection doing nothing else.
        await admin.execute(f'DROP DATABASE IF EXISTS "{database}"')
        await admin.execute(f'CREATE DATABASE "{database}"')
    finally:
        await admin.close()

    conn = await asyncpg.connect(_ASYNCPG_DSN.rsplit("/", 1)[0] + "/" + database)
    try:
        await conn.execute(Path(SCHEMA_SQL).read_text(encoding="utf-8"))
        if apply_migrations:
            for path in sorted(Path(MIGRATIONS_DIR).glob("*.sql")):
                await conn.execute(path.read_text(encoding="utf-8"))
        await conn.execute(Path(CRM_TEST_SCHEMA_SQL).read_text(encoding="utf-8"))
    finally:
        await conn.close()


async def _drop(database: str) -> None:
    admin = await asyncpg.connect(_ASYNCPG_DSN)
    try:
        await admin.execute(f'DROP DATABASE IF EXISTS "{database}"')
    finally:
        await admin.close()


@pytest.fixture(scope="module")
def snapshots(request):
    """Two databases, built once for the whole module.

    Synchronous and module-scoped for the same reason `test_engine` is: a
    session-scoped ASYNC fixture has its finaliser run after the loop is closed,
    and asyncpg then raises out of the Windows IocpProactor.
    """
    request.getfixturevalue("apply_schema")  # the container is up and provisioned

    async def _run():
        await _build(_FRESH_DB, apply_migrations=False)
        await _build(_UPGRADED_DB, apply_migrations=True)
        return await _snapshot(_FRESH_DB), await _snapshot(_UPGRADED_DB)

    fresh, upgraded = asyncio.run(_run())
    yield fresh, upgraded
    asyncio.run(_drop(_FRESH_DB))
    asyncio.run(_drop(_UPGRADED_DB))


class TestTheTwoPathsAgree:
    def test_the_snapshots_are_not_empty(self):
        """Guards the guard: a query that returned nothing would make every
        comparison below pass by comparing two empty lists."""
        assert set(_SNAPSHOT_QUERIES) >= {"columns", "indexes", "partitions", "functions"}

    @pytest.mark.parametrize("aspect", sorted(_SNAPSHOT_QUERIES))
    def test_a_fresh_database_matches_an_upgraded_one(self, snapshots, aspect: str):
        """The property that matters: provisioning from scratch and upgrading an
        existing database produce the same thing.

        A failure here names the aspect and the difference. The usual cause is a
        change written into a migration and not into schema.sql - which works in
        every environment that already exists, and breaks the next one created.
        """
        fresh, upgraded = snapshots
        assert fresh[aspect], f"the {aspect} snapshot is empty - the query stopped working"
        assert fresh[aspect] == upgraded[aspect]

    def test_both_report_the_version_this_build_expects(self, snapshots):
        from shared.const import LOCAL_SCHEMA_VERSION

        fresh, upgraded = snapshots
        assert max(row[0] for row in fresh["schema_version"]) == LOCAL_SCHEMA_VERSION
        assert max(row[0] for row in upgraded["schema_version"]) == LOCAL_SCHEMA_VERSION
