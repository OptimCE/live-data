"""`find_active_meter` against the real `meter`/`meter_data` tables.

The window is tested against the BRUSSELS-local date of the request instant,
because that is the CRM's "today" (`appTodayISO()`), and the CRM's active-meter
list is what feeds the Add-device picker. It used to be `CURRENT_DATE`, the date
in the database session's timezone, which is UTC. For an hour or two after every
Belgian midnight the picker then offered a meter whose window starts today, the
create refused it with EAN_NOT_FOUND, and a meter whose window ended yesterday
was still accepted.

Every case pins the session to UTC, the deployed default and the hostile one,
and uses an instant whose UTC date is not its Brussels date.
"""

import datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from ports.crm_read import FakeCrmRead, SqlAlchemyCrmRead
from tests.factories.meter_factory import create_meter, create_meter_data
from tests.factories.subscription_factory import create_community

# Half an hour after Brussels midnight, while the UTC date is still the previous
# day. Once in summer time (CEST, UTC+2) and once in winter time (CET, UTC+1).
AFTER_BRUSSELS_MIDNIGHT = [
    pytest.param(
        datetime.datetime(2026, 6, 30, 22, 30, tzinfo=datetime.UTC),
        datetime.date(2026, 7, 1),
        id="cest",
    ),
    pytest.param(
        datetime.datetime(2026, 1, 31, 23, 30, tzinfo=datetime.UTC),
        datetime.date(2026, 2, 1),
        id="cet",
    ),
]

ONE_DAY = datetime.timedelta(days=1)


@pytest.fixture
async def community_id(db_session: AsyncSession) -> int:
    # LOCAL: reverted with the test's rolled-back transaction.
    await db_session.execute(text("SET LOCAL TimeZone = 'UTC'"))
    return (await create_community(db_session)).id


async def _meter(
    session: AsyncSession,
    id_community: int,
    *,
    start_date: datetime.date,
    end_date: datetime.date | None = None,
) -> str:
    ean = await create_meter(session, id_community=id_community)
    await create_meter_data(session, ean=ean, start_date=start_date, end_date=end_date)
    return ean


async def _find(session: AsyncSession, *, ean: str, id_community: int, now: datetime.datetime):
    return await SqlAlchemyCrmRead(session).find_active_meter(
        ean=ean, id_community=id_community, now=now
    )


class TestTheBrusselsDate:
    @pytest.mark.parametrize(("now", "brussels_today"), AFTER_BRUSSELS_MIDNIGHT)
    async def test_a_window_starting_today_in_brussels_is_found(
        self, db_session: AsyncSession, community_id: int, now, brussels_today
    ):
        """The case the picker hit: the CRM lists this meter as active, so the
        create must accept it."""
        assert now.date() != brussels_today
        ean = await _meter(db_session, community_id, start_date=brussels_today)

        found = await _find(db_session, ean=ean, id_community=community_id, now=now)

        assert found is not None
        assert found.ean == ean
        assert found.capacity_kva == 5.0

    @pytest.mark.parametrize(("now", "brussels_today"), AFTER_BRUSSELS_MIDNIGHT)
    async def test_a_window_that_ended_yesterday_in_brussels_is_not_found(
        self, db_session: AsyncSession, community_id: int, now, brussels_today
    ):
        """The other half of the same skew. Under the UTC date, "yesterday" was
        still today, and the device was created on a meter the CRM no longer
        lists as active."""
        ean = await _meter(
            db_session,
            community_id,
            start_date=brussels_today - 30 * ONE_DAY,
            end_date=brussels_today - ONE_DAY,
        )

        assert await _find(db_session, ean=ean, id_community=community_id, now=now) is None

    @pytest.mark.parametrize(("now", "brussels_today"), AFTER_BRUSSELS_MIDNIGHT)
    async def test_a_window_ending_today_in_brussels_is_still_in_force(
        self, db_session: AsyncSession, community_id: int, now, brussels_today
    ):
        """The bounds are CLOSED at both ends, as `meter_data`'s are everywhere."""
        ean = await _meter(
            db_session, community_id, start_date=brussels_today, end_date=brussels_today
        )

        assert await _find(db_session, ean=ean, id_community=community_id, now=now) is not None

    async def test_a_minute_before_brussels_midnight_is_still_the_previous_day(
        self, db_session: AsyncSession, community_id: int
    ):
        """23:59 CEST on 30 June. The fix must move the day at Brussels midnight,
        not simply one day ahead of UTC."""
        now = datetime.datetime(2026, 6, 30, 21, 59, tzinfo=datetime.UTC)
        starts_tomorrow = await _meter(
            db_session, community_id, start_date=datetime.date(2026, 7, 1)
        )
        ends_today = await _meter(
            db_session,
            community_id,
            start_date=datetime.date(2026, 6, 1),
            end_date=datetime.date(2026, 6, 30),
        )

        not_yet = await _find(db_session, ean=starts_tomorrow, id_community=community_id, now=now)
        still = await _find(db_session, ean=ends_today, id_community=community_id, now=now)

        assert not_yet is None
        assert still is not None


class TestTheInstant:
    async def test_a_naive_instant_is_refused_by_the_adapter_and_the_fake(
        self, db_session: AsyncSession
    ):
        """A naive datetime has no Brussels date. The fake refuses it as the
        adapter does, so a caller cannot pass one through a green suite."""
        naive = datetime.datetime(2026, 6, 30, 22, 30)

        with pytest.raises(ValueError, match="aware"):
            await _find(db_session, ean="541448000000000000", id_community=1, now=naive)
        with pytest.raises(ValueError, match="aware"):
            await FakeCrmRead().find_active_meter(
                ean="541448000000000000", id_community=1, now=naive
            )

    async def test_the_fake_records_the_brussels_date(self):
        fake = FakeCrmRead()
        now = datetime.datetime(2026, 6, 30, 22, 30, tzinfo=datetime.UTC)

        await fake.find_active_meter(ean="541448000000000000", id_community=7, now=now)

        assert fake.calls == [
            ("find_active_meter", ("541448000000000000", 7, datetime.date(2026, 7, 1)))
        ]
