"""The ownership projection against a real database. Build step 7.

`domain/test_ownership.py` pins the algebra without a session. This file pins the
two things that algebra cannot see: the SQL that fetches windows from the CRM,
and the write path that keeps `device_owner_window` in step with it.

One Postgres backs both schemas under test, so the same session plays both roles
- which is exactly how `tests/conftest.py` wires the HTTP client.
"""

import datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from ports.crm_core import FakeCrmCoreRead, SqlAlchemyCrmCoreRead
from shared.const import DeviceStatus
from tests.factories.device_factory import create_device
from tests.factories.meter_factory import (
    METER_DATA_INACTIVE,
    create_meter,
    create_meter_data,
    create_owned_meter,
)
from tests.factories.subscription_factory import create_community
from worker.ownership import refresh_community, refresh_ownership
from worker.retention import raw_floor

NOW = datetime.datetime(2026, 9, 16, 10, 30, tzinfo=datetime.UTC)


async def _windows(session: AsyncSession, id_community: int) -> list:
    rows = await session.execute(
        text(
            "SELECT ean, id_member, valid_from, valid_to, ambiguous "
            "FROM device_owner_window WHERE id_community = :c "
            "ORDER BY ean, valid_from"
        ),
        {"c": id_community},
    )
    return list(rows.all())


@pytest.fixture
async def community_id(db_session: AsyncSession) -> int:
    created = await create_community(db_session)
    return created.id


class TestTheCrmRead:
    async def test_no_eans_means_no_query_and_no_rows(self, db_session: AsyncSession):
        """The fresh-community case, and the common one. A naive
        `ean = ANY('{}')` is not wrong, merely a wasted round trip per community
        per run - but returning early also means the empty case is exercised
        without a database at all."""
        port = SqlAlchemyCrmCoreRead(db_session)
        assert await port.ownership_windows(eans=[], id_community=1) == []

    async def test_only_active_windows_are_returned(
        self, db_session: AsyncSession, community_id: int
    ):
        """An INACTIVE row is CRM history, not ownership. Counting it would
        attribute energy to a member whose contract ended."""
        ean = await create_meter(db_session, id_community=community_id)
        await create_meter_data(
            db_session, ean=ean, id_member=1, start_date=datetime.date(2026, 1, 1)
        )
        await create_meter_data(
            db_session,
            ean=ean,
            id_member=99,
            start_date=datetime.date(2026, 1, 1),
            status=METER_DATA_INACTIVE,
        )

        found = await SqlAlchemyCrmCoreRead(db_session).ownership_windows(
            eans=[ean], id_community=community_id
        )
        assert [w.id_member for w in found] == [1]

    async def test_a_meter_in_another_community_is_not_returned(self, db_session: AsyncSession):
        """The join through `meter.id_community` is the tenant boundary here -
        `meter_data` carries no community at all. Without it, an EAN (which is
        printed on the physical meter) would read another community's ownership
        history."""
        mine = await create_community(db_session)
        theirs = await create_community(db_session)
        ean = await create_owned_meter(db_session, id_community=theirs.id, id_member=5)

        found = await SqlAlchemyCrmCoreRead(db_session).ownership_windows(
            eans=[ean], id_community=mine.id
        )
        assert found == []

    async def test_overlapping_windows_come_back_already_flagged(
        self, db_session: AsyncSession, community_id: int
    ):
        """Flagged by the PORT, not by the caller. A caller that forgot would get
        `ambiguous=False` from the dataclass default - an unambiguous-looking
        window that is nothing of the kind."""
        ean = await create_meter(db_session, id_community=community_id)
        await create_meter_data(
            db_session,
            ean=ean,
            id_member=1,
            start_date=datetime.date(2026, 1, 1),
            end_date=datetime.date(2026, 3, 31),
        )
        await create_meter_data(
            db_session, ean=ean, id_member=2, start_date=datetime.date(2026, 3, 1)
        )

        found = await SqlAlchemyCrmCoreRead(db_session).ownership_windows(
            eans=[ean], id_community=community_id
        )
        assert [w.ambiguous for w in found] == [True, True]


class TestTheFakeAgreesWithPostgres:
    """The fake is only worth having if it answers what the real adapter answers.

    `FakeCrmCoreRead` was written for tests and then used by none of them - the
    suite reaches for the real adapter against real Postgres throughout, which is
    the better habit. But an UNUSED fake is not a harmless one: the first caller
    to reach for it inherits a second implementation of the ambiguity rule that
    nothing has ever compared against the first.

    That is the shape that once hid an unrevokable device behind 270 green tests
    - a fake more forgiving than the dependency it stands in for makes the bug
    unreachable from the suite. So rather than delete it or leave it unverified,
    this pins it to the real thing.
    """

    async def test_it_flags_overlaps_the_way_the_real_adapter_does(
        self, db_session: AsyncSession, community_id: int
    ):
        ean = await create_meter(db_session, id_community=community_id)
        await create_meter_data(
            db_session,
            ean=ean,
            id_member=1,
            start_date=datetime.date(2026, 1, 1),
            end_date=datetime.date(2026, 3, 31),
        )
        await create_meter_data(
            db_session, ean=ean, id_member=2, start_date=datetime.date(2026, 3, 1)
        )

        real = await SqlAlchemyCrmCoreRead(db_session).ownership_windows(
            eans=[ean], id_community=community_id
        )
        fake = await FakeCrmCoreRead(real).ownership_windows(eans=[ean], id_community=community_id)
        assert [w.ambiguous for w in fake] == [w.ambiguous for w in real] == [True, True]

    async def test_it_scopes_by_community_the_way_the_real_adapter_does(
        self, db_session: AsyncSession, community_id: int
    ):
        """A fake that ignored `id_community` would let a cross-tenant test pass."""
        ean = await create_owned_meter(db_session, id_community=community_id, id_member=7)
        real = await SqlAlchemyCrmCoreRead(db_session).ownership_windows(
            eans=[ean], id_community=community_id
        )
        assert real, "the fixture produced no window - the comparison below would be vacuous"

        fake = FakeCrmCoreRead(real)
        assert await fake.ownership_windows(eans=[ean], id_community=community_id) == real
        assert await fake.ownership_windows(eans=[ean], id_community=community_id + 1) == []
        assert await fake.ownership_windows(eans=[], id_community=community_id) == []


class TestTheProjection:
    async def test_one_device_one_window(self, db_session: AsyncSession, community_id: int):
        ean = await create_owned_meter(db_session, id_community=community_id, id_member=42)
        await create_device(db_session, id_community=community_id, ean=ean)

        written, _ = await refresh_community(
            db_session, SqlAlchemyCrmCoreRead(db_session), id_community=community_id, now=NOW
        )
        assert written == 1
        rows = await _windows(db_session, community_id)
        assert len(rows) == 1
        assert rows[0].id_member == 42
        assert rows[0].valid_to is None
        assert rows[0].ambiguous is False

    async def test_a_clean_transfer_projects_two_unambiguous_windows(
        self, db_session: AsyncSession, community_id: int
    ):
        """The case the whole table exists for, end to end."""
        ean = await create_meter(db_session, id_community=community_id)
        await create_meter_data(
            db_session,
            ean=ean,
            id_member=1,
            start_date=datetime.date(2026, 1, 1),
            end_date=datetime.date(2026, 2, 28),
        )
        await create_meter_data(
            db_session, ean=ean, id_member=2, start_date=datetime.date(2026, 3, 1)
        )
        await create_device(db_session, id_community=community_id, ean=ean)

        await refresh_community(
            db_session, SqlAlchemyCrmCoreRead(db_session), id_community=community_id, now=NOW
        )
        rows = await _windows(db_session, community_id)
        assert [(r.id_member, r.ambiguous) for r in rows] == [(1, False), (2, False)]

    async def test_a_revoked_devices_ean_is_still_projected(
        self, db_session: AsyncSession, community_id: int
    ):
        """Revoking does not delete what the device already sent, so its
        historical buckets still need an owner. Filtering on `status <> REVOKED`
        here makes `n_members` fall and k suppress data that was visible
        yesterday."""
        ean = await create_owned_meter(db_session, id_community=community_id, id_member=8)
        await create_device(
            db_session, id_community=community_id, ean=ean, status=DeviceStatus.REVOKED
        )

        written, _ = await refresh_community(
            db_session, SqlAlchemyCrmCoreRead(db_session), id_community=community_id, now=NOW
        )
        assert written == 1

    async def test_a_meter_with_no_device_is_not_projected(
        self, db_session: AsyncSession, community_id: int
    ):
        """The projection is driven by devices, not by the CRM's meter list. A
        community may have hundreds of meters and three live-data devices."""
        await create_owned_meter(db_session, id_community=community_id, id_member=1)

        written, _ = await refresh_community(
            db_session, SqlAlchemyCrmCoreRead(db_session), id_community=community_id, now=NOW
        )
        assert written == 0

    async def test_running_twice_leaves_the_same_rows(
        self, db_session: AsyncSession, community_id: int
    ):
        """`device_owner_window` has NO unique constraint - it cannot have one,
        because two overlapping ACTIVE rows are exactly what it must be able to
        represent. So idempotence rests entirely on the DELETE, and a missing one
        doubles the table on every tick."""
        ean = await create_owned_meter(db_session, id_community=community_id, id_member=3)
        await create_device(db_session, id_community=community_id, ean=ean)
        port = SqlAlchemyCrmCoreRead(db_session)

        await refresh_community(db_session, port, id_community=community_id, now=NOW)
        first = await _windows(db_session, community_id)
        await refresh_community(db_session, port, id_community=community_id, now=NOW)
        second = await _windows(db_session, community_id)

        assert len(first) == 1
        assert [tuple(r) for r in first] == [tuple(r) for r in second]

    async def test_a_removed_window_disappears(self, db_session: AsyncSession, community_id: int):
        """The direction an upsert would miss. A window deactivated in the CRM
        must stop contributing membership, and an INSERT-only refresh leaves it
        contributing for ever."""
        ean = await create_meter(db_session, id_community=community_id)
        row_id = await create_meter_data(
            db_session, ean=ean, id_member=1, start_date=datetime.date(2026, 1, 1)
        )
        await create_device(db_session, id_community=community_id, ean=ean)
        port = SqlAlchemyCrmCoreRead(db_session)
        await refresh_community(db_session, port, id_community=community_id, now=NOW)
        assert len(await _windows(db_session, community_id)) == 1

        await db_session.execute(
            text("UPDATE meter_data SET status = :s WHERE id = :i"),
            {"s": METER_DATA_INACTIVE, "i": row_id},
        )
        await refresh_community(db_session, port, id_community=community_id, now=NOW)
        assert await _windows(db_session, community_id) == []

    async def test_every_community_with_a_device_is_refreshed(self, db_session: AsyncSession):
        first = await create_community(db_session)
        second = await create_community(db_session)
        for community in (first, second):
            ean = await create_owned_meter(db_session, id_community=community.id, id_member=1)
            await create_device(db_session, id_community=community.id, ean=ean)

        result = await refresh_ownership(
            db_session, SqlAlchemyCrmCoreRead(db_session), now=NOW, communities=None
        )
        assert result.communities >= 2
        assert result.windows_written >= 2

    async def test_the_refresh_can_be_limited_to_a_set_of_communities(
        self, db_session: AsyncSession
    ):
        """What the scheduler does with the ACTIVE set (D-12): a switched-off
        community's windows are left exactly as they were - not rewritten, and not
        deleted either - until it is switched back on."""
        active = await create_community(db_session)
        switched_off = await create_community(db_session)
        meter_data_ids = {}
        for community in (active, switched_off):
            ean = await create_meter(db_session, id_community=community.id)
            meter_data_ids[community.id] = await create_meter_data(
                db_session, ean=ean, id_member=1, start_date=datetime.date(2026, 1, 1)
            )
            await create_device(db_session, id_community=community.id, ean=ean)
        port = SqlAlchemyCrmCoreRead(db_session)
        # Both projected while both were on.
        await refresh_ownership(db_session, port, now=NOW, communities=None)
        before = await _windows(db_session, switched_off.id)
        assert len(before) == 1

        # The CRM changes for BOTH, then only the active one is refreshed.
        await db_session.execute(
            text("UPDATE meter_data SET status = :s WHERE id = ANY(:ids)"),
            {"s": METER_DATA_INACTIVE, "ids": list(meter_data_ids.values())},
        )
        result = await refresh_ownership(db_session, port, now=NOW, communities={active.id})

        assert result.communities == 1
        assert await _windows(db_session, active.id) == []
        assert await _windows(db_session, switched_off.id) == before

    def test_the_community_set_is_a_required_keyword(self):
        """No default, deliberately. A default of "everyone" would let a caller
        that forgot the set refresh switched-off communities, silently."""
        import inspect

        parameter = inspect.signature(refresh_ownership).parameters["communities"]
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        assert parameter.default is inspect.Parameter.empty


class TestDirtyMarking:
    """An ownership correction older than 48 h must still reach `n_members`.

    Without this the correction is invisible: the tick only recomputes the last
    48 hours, and closed buckets are never revisited.
    """

    async def _rollup_row(
        self, session: AsyncSession, *, id_community: int, bucket: datetime.datetime
    ) -> None:
        await session.execute(
            text(
                "INSERT INTO rollup_community_hour (id_community, bucket, import_wh, export_wh, "
                "n_devices, n_devices_production, n_members, n_devices_unattributed, computed_at) "
                "VALUES (:c, :b, 0, 0, 1, 1, 1, 0, :now) ON CONFLICT DO NOTHING"
            ),
            {"c": id_community, "b": bucket, "now": NOW},
        )

    async def test_an_ownership_change_marks_old_buckets_dirty(
        self, db_session: AsyncSession, community_id: int
    ):
        old_bucket = datetime.datetime(2026, 5, 10, 8, tzinfo=datetime.UTC)
        await self._rollup_row(db_session, id_community=community_id, bucket=old_bucket)

        ean = await create_meter(db_session, id_community=community_id)
        await create_meter_data(
            db_session,
            ean=ean,
            id_member=1,
            start_date=datetime.date(2026, 5, 1),
            end_date=datetime.date(2026, 5, 31),
        )
        await create_device(db_session, id_community=community_id, ean=ean)
        port = SqlAlchemyCrmCoreRead(db_session)

        _, marked = await refresh_community(db_session, port, id_community=community_id, now=NOW)
        assert marked == 1

        dirty = await db_session.scalar(
            text("SELECT count(*) FROM rollup_dirty WHERE id_community = :c AND bucket = :b"),
            {"c": community_id, "b": old_bucket},
        )
        assert dirty == 1

    async def test_an_unchanged_refresh_marks_nothing(
        self, db_session: AsyncSession, community_id: int
    ):
        """The self-inflicted denial of service this guards against: including
        `refreshed_at` in the comparison would mark every bucket dirty every 15
        minutes, and the tick would never catch up."""
        old_bucket = datetime.datetime(2026, 5, 10, 8, tzinfo=datetime.UTC)
        await self._rollup_row(db_session, id_community=community_id, bucket=old_bucket)
        ean = await create_owned_meter(
            db_session, id_community=community_id, id_member=1, start_date=datetime.date(2026, 5, 1)
        )
        await create_device(db_session, id_community=community_id, ean=ean)
        port = SqlAlchemyCrmCoreRead(db_session)

        await refresh_community(db_session, port, id_community=community_id, now=NOW)
        await db_session.execute(text("DELETE FROM rollup_dirty"))

        _, marked = await refresh_community(db_session, port, id_community=community_id, now=NOW)
        assert marked == 0

    async def test_buckets_inside_the_window_are_not_marked(
        self, db_session: AsyncSession, community_id: int
    ):
        """Everything at or above `lo` is recomputed unconditionally every tick,
        so marking it is pure churn on the hottest table in the job."""
        recent = datetime.datetime(2026, 9, 16, 6, tzinfo=datetime.UTC)  # inside 48 h of NOW
        await self._rollup_row(db_session, id_community=community_id, bucket=recent)

        ean = await create_meter(db_session, id_community=community_id)
        await create_meter_data(
            db_session, ean=ean, id_member=1, start_date=datetime.date(2026, 9, 1)
        )
        await create_device(db_session, id_community=community_id, ean=ean)

        _, marked = await refresh_community(
            db_session, SqlAlchemyCrmCoreRead(db_session), id_community=community_id, now=NOW
        )
        assert marked == 0

    async def test_a_bucket_outside_the_changed_dates_is_not_marked(
        self, db_session: AsyncSession, community_id: int
    ):
        """NEGATIVE CONTROL for the date span. Without it, a marking query that
        ignored `dirty_from`/`dirty_to` entirely would pass every test above."""
        unrelated = datetime.datetime(2026, 1, 5, 8, tzinfo=datetime.UTC)
        await self._rollup_row(db_session, id_community=community_id, bucket=unrelated)

        ean = await create_meter(db_session, id_community=community_id)
        await create_meter_data(
            db_session,
            ean=ean,
            id_member=1,
            start_date=datetime.date(2026, 5, 1),
            end_date=datetime.date(2026, 5, 31),
        )
        await create_device(db_session, id_community=community_id, ean=ean)

        _, marked = await refresh_community(
            db_session, SqlAlchemyCrmCoreRead(db_session), id_community=community_id, now=NOW
        )
        assert marked == 0

    async def test_a_bucket_below_the_raw_retention_edge_is_never_marked(
        self, db_session: AsyncSession, community_id: int
    ):
        """Its readings are gone, so the tick would recompute it from nothing:
        DELETE the hour, re-insert nothing, and re-derive its day from nothing -
        erasing `rollup_community_day`, the series kept for ever. A window
        corrected back years must mark only what can still be recomputed. The
        bucket above the edge, inside the same changed span, is the control."""
        floor = raw_floor(NOW)
        too_old = floor - datetime.timedelta(days=20)
        recomputable = floor + datetime.timedelta(days=20)
        for bucket in (too_old, recomputable):
            await self._rollup_row(db_session, id_community=community_id, bucket=bucket)

        ean = await create_meter(db_session, id_community=community_id)
        await create_meter_data(
            db_session, ean=ean, id_member=1, start_date=too_old.date() - datetime.timedelta(days=5)
        )
        await create_device(db_session, id_community=community_id, ean=ean)

        _, marked = await refresh_community(
            db_session, SqlAlchemyCrmCoreRead(db_session), id_community=community_id, now=NOW
        )

        dirty = set(
            (
                await db_session.execute(
                    text("SELECT bucket FROM rollup_dirty WHERE id_community = :c"),
                    {"c": community_id},
                )
            ).scalars()
        )
        assert recomputable in dirty
        assert too_old not in dirty
        assert marked == 1

    async def test_a_change_of_operation_alone_marks_old_buckets_dirty(
        self, db_session: AsyncSession, community_id: int
    ):
        """Same member, same dates - only the operation moved (crm-backend's
        same-day correction rewrites it in place). Which operation row the energy
        lands in changed, so the closed buckets must be recomputed (D-14)."""
        old_bucket = datetime.datetime(2026, 5, 10, 8, tzinfo=datetime.UTC)
        await self._rollup_row(db_session, id_community=community_id, bucket=old_bucket)
        ean = await create_meter(db_session, id_community=community_id)
        window = await create_meter_data(
            db_session,
            ean=ean,
            id_member=1,
            start_date=datetime.date(2026, 5, 1),
            id_sharing_operation=101,
        )
        await create_device(db_session, id_community=community_id, ean=ean)
        port = SqlAlchemyCrmCoreRead(db_session)
        await refresh_community(db_session, port, id_community=community_id, now=NOW)
        await db_session.execute(text("DELETE FROM rollup_dirty"))

        await db_session.execute(
            text("UPDATE meter_data SET id_sharing_operation = 202 WHERE id = :id"), {"id": window}
        )
        _, marked = await refresh_community(db_session, port, id_community=community_id, now=NOW)

        assert marked == 1
        projected = await db_session.scalar(
            text("SELECT id_sharing_operation FROM device_owner_window WHERE ean = :e"),
            {"e": ean},
        )
        assert projected == 202
