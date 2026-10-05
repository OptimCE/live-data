"""The database and the code agree about what the database contains.

There is no Alembic and no autogeneration here. `scripts/sql/schema.sql` is the
source of truth, `shared/models/local_models.py` is a hand-written mirror, and
`tests/conftest.py` deliberately applies the SQL rather than
`Base.metadata.create_all()` - so nothing but a test notices when the two drift.

Drift is silent in the direction that matters. A model naming a column the table
does not have raises `UndefinedColumn` the first time some endpoint touches it,
which may be months after the change and in production. These tests touch every
model once, on purpose.
"""

from typing import ClassVar

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from domain.partitions import PARTITIONED_TABLES
from shared import const
from shared.models import local_models
from shared.models.local_models import SchemaVersionModel

# Every mapped model in the local database. Collected by reflection rather than
# listed, so a model added without a test here is still covered.
LOCAL_MODELS = sorted(
    (
        getattr(local_models, name)
        for name in dir(local_models)
        if name.endswith("Model") and hasattr(getattr(local_models, name), "__tablename__")
    ),
    key=lambda model: model.__tablename__,
)


class TestTheModelsMatchTheSchema:
    def test_every_table_in_the_file_is_reachable(self):
        """A sanity floor: reflection found the models rather than nothing.

        Without it, a rename that empties `LOCAL_MODELS` would make every test in
        this class pass by iterating over an empty list - the failure mode that
        makes a suite green and worthless.
        """
        assert len(LOCAL_MODELS) >= 17
        assert "rollup_device_hour" in {m.__tablename__ for m in LOCAL_MODELS}

    @pytest.mark.parametrize("model", LOCAL_MODELS, ids=lambda m: m.__tablename__)
    async def test_every_column_the_model_declares_exists_in_the_database(
        self, db_session: AsyncSession, model
    ):
        """SELECT every mapped column from every mapped table.

        This is the cheapest possible proof that the hand-written mirror matches
        the DDL, and it covers the whole file in one parametrised test. A column
        the model has and the table does not raises `UndefinedColumn` here rather
        than in a read path later.

        LIMIT 0 because the point is the column list, not the rows: the statement
        is planned - which is what resolves the names - and returns nothing.
        """
        await db_session.execute(select(model).limit(0))

    @pytest.mark.parametrize("model", LOCAL_MODELS, ids=lambda m: m.__tablename__)
    async def test_the_database_has_no_column_the_model_is_missing(
        self, db_session: AsyncSession, model
    ):
        """The other direction, which the first test cannot see.

        A column added to schema.sql and not to the model is invisible to every
        query the service writes - so a NOT NULL one with no default makes every
        INSERT fail at runtime while the model looks complete.
        """
        rows = await db_session.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = :t"
            ),
            {"t": model.__tablename__},
        )
        in_database = set(rows.scalars().all())
        in_model = {column.name for column in model.__table__.columns}
        assert not (in_database - in_model), (
            f"{model.__tablename__} has columns the model does not map: "
            f"{sorted(in_database - in_model)}"
        )


class TestSchemaVersion:
    async def test_the_applied_version_is_the_one_this_build_expects(
        self, db_session: AsyncSession
    ):
        """`/health/readiness` compares these two and reports unhealthy on a
        mismatch, so a migration applied without bumping the constant - or the
        reverse - turns the container red. Catch it here instead."""
        applied = await db_session.scalar(select(func.max(SchemaVersionModel.version)))
        assert applied == const.LOCAL_SCHEMA_VERSION

    async def test_every_version_between_one_and_the_current_is_recorded(
        self, db_session: AsyncSession
    ):
        """No gaps. A missing row means a migration that ran without recording
        itself, and the next reader cannot tell which."""
        rows = await db_session.execute(select(SchemaVersionModel.version).order_by("version"))
        assert list(rows.scalars().all()) == list(range(1, const.LOCAL_SCHEMA_VERSION + 1))


class TestPartitionRegistry:
    async def test_every_registry_table_is_actually_partitioned(self, db_session: AsyncSession):
        """A table in the registry that is NOT partitioned makes the create-ahead
        job fail on it every night, and the retention job find nothing to drop."""
        rows = await db_session.execute(
            text(
                "SELECT c.relname FROM pg_class c "
                "WHERE c.relname = ANY(:names) AND c.relkind = 'p'"
            ),
            {"names": list(PARTITIONED_TABLES)},
        )
        assert set(rows.scalars().all()) == set(PARTITIONED_TABLES)

    async def test_every_registry_table_has_a_default_partition(self, db_session: AsyncSession):
        """plan 6.2 rule 2. Without a DEFAULT partition an insert outside every
        range RAISES, which for the rollup sweep would abort the whole
        transaction rather than land one row somewhere visible.

        It is also what `/health/readiness` counts: a non-empty default is the
        only signal that the create-ahead job has stopped.
        """
        for table in PARTITIONED_TABLES:
            found = await db_session.scalar(
                text(
                    "SELECT count(*) FROM pg_class c "
                    "JOIN pg_inherits i ON i.inhrelid = c.oid "
                    "JOIN pg_class p ON p.oid = i.inhparent "
                    "WHERE p.relname = :t "
                    "AND pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT'"
                ),
                {"t": table},
            )
            assert found == 1, f"{table} has no DEFAULT partition"

    async def test_every_default_partition_is_empty(self, db_session: AsyncSession):
        """The invariant readiness reports on, asserted in the suite too.

        A non-empty default is not merely untidy: once it holds rows in a range,
        `ATTACH PARTITION` over that range FAILS, and the create-ahead job starts
        failing at 00:00 UTC on the first of a month, platform-wide.
        """
        for table in PARTITIONED_TABLES:
            count = await db_session.scalar(text(f"SELECT count(*) FROM {table}_default"))  # noqa: S608
            assert count == 0, f"{table}_default holds {count} row(s)"


class TestAdvisoryLockKeys:
    """Four jobs, four locks, so a slow one cannot block a fast one."""

    KEYS: ClassVar[dict[str, int]] = {
        "partitions": const.ADVISORY_LOCK_PARTITIONS,
        "rollups": const.ADVISORY_LOCK_ROLLUPS,
        "retention": const.ADVISORY_LOCK_RETENTION,
        "forecast": const.ADVISORY_LOCK_FORECAST,
        "ownership": const.ADVISORY_LOCK_OWNERSHIP,
    }

    def test_the_keys_are_pairwise_distinct(self):
        assert len(set(self.KEYS.values())) == len(self.KEYS)

    def test_the_keys_are_distinct_in_their_low_thirty_two_bits(self):
        """`pg_locks.objid` is 32 bits. Two keys differing only above that are the
        same lock as far as Postgres is concerned, and the collision would show
        up as one job mysteriously never running."""
        assert len({key & 0xFFFFFFFF for key in self.KEYS.values()}) == len(self.KEYS)

    def test_they_do_not_collide_with_the_sibling_services(self):
        """administrative-document owns 0x0AD3_xxxx and billing 0x0B11_xxxx; each
        of those repos carries the mirror of this test. The databases are
        separate today, but advisory locks are per-CLUSTER, and they share one."""
        for key in self.KEYS.values():
            assert key >> 16 not in (0x0AD3, 0x0B11)
