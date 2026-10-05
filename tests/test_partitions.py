"""Pin domain/partitions.py against the SQL that actually creates the partitions.

`live_ensure_monthly_partitions()` in scripts/sql/schema.sql is the source of
truth; `domain/partitions.py` predicts what it will produce, for tests, for the
ops view of which partition a timestamp lands in, and for build step 8's
retention job, which must identify a partition before it can detach one. If the
two drift, nothing errors until a `CREATE ... PARTITION OF` fails months later at
00:00 UTC on the first of a month.

----------------------------------------------------------------------------
WHY THESE TESTS COMPARE INSTANTS AND NOT STRINGS.

`pg_get_expr(relpartbound)` renders a `timestamptz` bound in the SESSION's
timezone. So the same bound reads back as

    FOR VALUES FROM ('2026-09-01 00:00:00+00')   under TimeZone='UTC'
    FOR VALUES FROM ('2026-09-01 02:00:00+02')   under TimeZone='Europe/Brussels'

- identical instants, different text. The test container runs UTC, so a string
comparison against domain/partitions.py's '+00' literals would pass TRIVIALLY,
under the one condition where it cannot fail, and would then fail spuriously the
day anyone ran it under a Brussels session.

So: these tests SET TimeZone='Europe/Brussels' deliberately - the hostile case -
and compare parsed `timestamptz` values. And `test_the_pinning_can_fail` is the
negative control that proves the comparison is load-bearing rather than
vacuous.
----------------------------------------------------------------------------
"""

import datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from domain.partitions import (
    MONTHS_AHEAD,
    MONTHS_BACK,
    PARTITIONED_TABLES,
    default_partition_name,
    is_aligned,
    month_bounds,
    month_start,
    months_to_provision,
    next_month_start,
    partition_name,
)

_BOUNDS_SQL = text(
    """
    SELECT c.relname,
           pg_get_expr(c.relpartbound, c.oid) AS bound
      FROM pg_class c
      JOIN pg_inherits i ON i.inhrelid = c.oid
      JOIN pg_class p ON p.oid = i.inhparent
     WHERE p.relname = :parent
     ORDER BY c.relname
    """
)


async def _partition_bounds(session: AsyncSession, parent: str) -> dict[str, str]:
    # The hostile timezone, on purpose - see the module docstring.
    await session.execute(text("SET TimeZone='Europe/Brussels'"))
    rows = (await session.execute(_BOUNDS_SQL, {"parent": parent})).all()
    return {str(name): str(bound) for name, bound in rows}


async def _parse_bound(session: AsyncSession, literal: str) -> datetime.datetime:
    """Let Postgres parse the literal, so the comparison is instant-vs-instant."""
    value = await session.scalar(
        # Through `text` first: asyncpg types a bind parameter from the
        # surrounding cast, so `CAST(:lit AS timestamptz)` would demand a
        # datetime and refuse the very string we want Postgres to parse.
        text("SELECT CAST(CAST(:lit AS text) AS timestamptz)"),
        {"lit": literal},
    )
    assert value is not None
    assert isinstance(value, datetime.datetime)
    return value


def _bound_literals(bound_expr: str) -> tuple[str, str]:
    """Pull the two quoted literals out of `FOR VALUES FROM ('x') TO ('y')`."""
    parts = bound_expr.split("'")
    # ["FOR VALUES FROM (", x, ") TO (", y, ")"]
    return parts[1], parts[3]


class TestTheSchemaCreatedItsOwnPartitions:
    @pytest.mark.parametrize("table", PARTITIONED_TABLES)
    async def test_every_registry_table_has_its_months(self, db_session: AsyncSession, table: str):
        """schema.sql creates its OWN partitions (plan 6.2 rule 1), for EVERY
        table in the registry - not just `measurement`.

        Parametrised over `PARTITIONED_TABLES` on purpose. plan 6.2 names the
        failure this prevents: "a job that names only `measurement` lets the
        rollups freeze about four months in while ingestion goes on looking
        perfectly healthy." A test that names only `measurement` permits exactly
        that, and would stay green through it.

        And creating them here rather than leaving it to a running worker is what
        stops every test being blocked on fixture DDL executed inside a
        rolled-back transaction, which fails intermittently and in
        test-order-dependent ways.
        """
        bounds = await _partition_bounds(db_session, table)
        now = datetime.datetime.now(datetime.UTC)
        expected = {partition_name(table, m) for m in months_to_provision(now)}
        missing = expected - bounds.keys()
        assert not missing, f"schema.sql did not create: {sorted(missing)}"
        assert len(expected) == MONTHS_BACK + MONTHS_AHEAD + 1

    async def test_the_python_registry_matches_the_sql_one(self, db_session: AsyncSession):
        """`domain.PARTITIONED_TABLES` and `live_partitioned_table` are two lists
        of the same thing, and nothing else keeps them equal.

        They are read by different consumers - the Python tuple by the retention
        and create-ahead jobs, the SQL table by `live_ensure_monthly_partitions`
        - so a table added to one and not the other is created but never rotated,
        or rotated but never created.
        """
        rows = await db_session.execute(text("SELECT table_name FROM live_partitioned_table"))
        assert set(rows.scalars().all()) == set(PARTITIONED_TABLES)

    async def test_the_default_partition_exists_and_is_empty(self, db_session: AsyncSession):
        """The DEFAULT partition is what keeps a late message rather than raising.

        Its emptiness is the signal that the create-ahead job is still running -
        which is why /health/readiness reports its row count.
        """
        bounds = await _partition_bounds(db_session, "measurement")
        name = default_partition_name("measurement")
        assert bounds.get(name) == "DEFAULT"
        rows = await db_session.scalar(text(f"SELECT count(*) FROM {name}"))  # noqa: S608
        assert rows == 0


class TestThePythonHelperAgreesWithTheSQL:
    async def test_every_bound_matches_month_bounds(self, db_session: AsyncSession):
        bounds = await _partition_bounds(db_session, "measurement")
        now = datetime.datetime.now(datetime.UTC)

        for moment in months_to_provision(now):
            name = partition_name("measurement", moment)
            lower_literal, upper_literal = _bound_literals(bounds[name])
            actual_lower = await _parse_bound(db_session, lower_literal)
            actual_upper = await _parse_bound(db_session, upper_literal)

            predicted_lower, predicted_upper = month_bounds(moment)
            expected_lower = await _parse_bound(db_session, predicted_lower)
            expected_upper = await _parse_bound(db_session, predicted_upper)

            assert actual_lower == expected_lower, f"{name}: lower bound drifted"
            assert actual_upper == expected_upper, f"{name}: upper bound drifted"
            # And the instant really is a UTC month boundary, not merely equal to
            # whatever Python also computed wrongly.
            assert actual_lower.astimezone(datetime.UTC).day == 1
            assert actual_lower.astimezone(datetime.UTC).hour == 0

    async def test_the_pinning_can_fail(self, db_session: AsyncSession):
        """NEGATIVE CONTROL.

        Perturb one predicted bound by an hour and assert the comparison rejects
        it. Without this, a comparison that silently normalised both sides - or
        compared two values that were always equal for a different reason - would
        report success for ever.
        """
        bounds = await _partition_bounds(db_session, "measurement")
        now = datetime.datetime.now(datetime.UTC)
        name = partition_name("measurement", now)
        lower_literal, _ = _bound_literals(bounds[name])
        actual_lower = await _parse_bound(db_session, lower_literal)

        predicted_lower, _ = month_bounds(now)
        wrong = await _parse_bound(db_session, predicted_lower)
        wrong += datetime.timedelta(hours=1)

        assert actual_lower != wrong


class TestTheHelperItself:
    def test_month_start_requires_an_aware_datetime(self):
        """A naive datetime is how a boundary silently moves twice a year."""
        with pytest.raises(ValueError, match="aware"):
            month_start(datetime.datetime(2026, 9, 15, 12, 0))

    def test_month_start_normalises_to_utc(self):
        brussels = datetime.timezone(datetime.timedelta(hours=2))
        # 2026-09-01 01:00+02 IS 2026-08-31 23:00 UTC, so the UTC month is AUGUST.
        moment = datetime.datetime(2026, 9, 1, 1, 0, tzinfo=brussels)
        assert month_start(moment) == datetime.datetime(2026, 8, 1, tzinfo=datetime.UTC)

    def test_december_rolls_the_year(self):
        december = datetime.datetime(2026, 12, 17, tzinfo=datetime.UTC)
        assert next_month_start(december) == datetime.datetime(2027, 1, 1, tzinfo=datetime.UTC)

    def test_bounds_carry_an_explicit_utc_offset(self):
        lower, upper = month_bounds(datetime.datetime(2026, 9, 15, tzinfo=datetime.UTC))
        assert lower == "2026-09-01 00:00:00+00"
        assert upper == "2026-10-01 00:00:00+00"

    def test_months_to_provision_is_contiguous(self):
        months = months_to_provision(
            datetime.datetime(2026, 11, 3, tzinfo=datetime.UTC), months_ahead=3, months_back=0
        )
        assert months == [
            datetime.datetime(2026, 11, 1, tzinfo=datetime.UTC),
            datetime.datetime(2026, 12, 1, tzinfo=datetime.UTC),
            datetime.datetime(2027, 1, 1, tzinfo=datetime.UTC),
            datetime.datetime(2027, 2, 1, tzinfo=datetime.UTC),
        ]

    def test_months_to_provision_reaches_backwards_across_a_year_boundary(self):
        """`months_back` exists because a legitimate message can be 35 days old.

        Without it, a freshly provisioned database has no partition for the month
        that message belongs to, it lands in the DEFAULT partition, and readiness
        goes red on day one looking exactly like a bug in ingest.
        """
        months = months_to_provision(
            datetime.datetime(2027, 1, 20, tzinfo=datetime.UTC), months_ahead=1, months_back=2
        )
        assert months == [
            datetime.datetime(2026, 11, 1, tzinfo=datetime.UTC),
            datetime.datetime(2026, 12, 1, tzinfo=datetime.UTC),
            datetime.datetime(2027, 1, 1, tzinfo=datetime.UTC),
            datetime.datetime(2027, 2, 1, tzinfo=datetime.UTC),
        ]

    def test_months_back_covers_the_ingest_acceptance_window(self):
        """The property MONTHS_BACK is chosen for, asserted rather than assumed.

        28 is February - deliberately the pessimistic month, because the
        assertion has to hold in the worst case rather than the average one.
        """
        assert MONTHS_BACK * 28 >= settings.INGEST_MAX_AGE_DAYS

    def test_a_negative_months_back_is_refused(self):
        with pytest.raises(ValueError, match="months_back"):
            months_to_provision(datetime.datetime(2026, 11, 3, tzinfo=datetime.UTC), months_back=-1)

    @pytest.mark.parametrize(
        ("moment", "expected"),
        [
            (datetime.datetime(2026, 9, 15, 10, 15, tzinfo=datetime.UTC), True),
            (datetime.datetime(2026, 9, 15, 10, 0, tzinfo=datetime.UTC), True),
            (datetime.datetime(2026, 9, 15, 10, 14, tzinfo=datetime.UTC), False),
            (datetime.datetime(2026, 9, 15, 10, 15, 1, tzinfo=datetime.UTC), False),
            (datetime.datetime(2026, 9, 15, 10, 15, 0, 1, tzinfo=datetime.UTC), False),
        ],
    )
    def test_alignment_is_measured_against_the_utc_epoch(self, moment, expected):
        assert is_aligned(moment) is expected
