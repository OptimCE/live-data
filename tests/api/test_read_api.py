"""The read surface: /summary, /series, /settings. Build step 6.

Driven through the real HTTP stack, so `GatewayScopeMiddleware`,
`resolve_internal_community`, `require_feature` and the role gate all run. Only
the two session dependencies are overridden - never an auth dependency, because
the auth chain is one of the things these tests exist to check.
"""

import datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from domain import buckets
from ports.crm_core import SqlAlchemyCrmCoreRead
from shared.const import FeatureName
from tests.conftest import gateway_headers
from tests.factories.device_factory import (
    create_device,
    create_hour_of_measurements,
    create_measurement,
)
from tests.factories.meter_factory import create_owned_meter
from tests.factories.subscription_factory import create_community, create_subscription
from worker import rollups
from worker.ownership import refresh_community


def utc(value: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(value).replace(tzinfo=datetime.UTC)


async def _populate(
    session: AsyncSession, *, id_community: int, members: int, bucket: datetime.datetime
) -> None:
    """`members` distinct members, one meter and one device each, one full hour."""
    for member in range(1, members + 1):
        ean = await create_owned_meter(session, id_community=id_community, id_member=member)
        device_id = await create_device(session, id_community=id_community, ean=ean)
        await create_hour_of_measurements(
            session,
            id_device=device_id,
            id_community=id_community,
            bucket=bucket,
            import_wh=10.0,
            export_wh=5.0,
            production_wh=100.0,
        )
    now = bucket + datetime.timedelta(hours=2)
    await refresh_community(
        session, SqlAlchemyCrmCoreRead(session), id_community=id_community, now=now
    )
    await rollups.tick_community(session, id_community=id_community, now=now)


@pytest.fixture
async def populated(db_session: AsyncSession, community):
    """A community with five members - one above the default k of 5."""
    bucket = datetime.datetime.now(datetime.UTC).replace(
        minute=0, second=0, microsecond=0
    ) - datetime.timedelta(hours=2)
    await _populate(db_session, id_community=community.id, members=5, bucket=bucket)
    return community, bucket


class TestSummary:
    async def test_a_manager_sees_the_community(self, client, populated, manager_headers):
        _community, bucket = populated
        response = await client.get("/summary", headers=manager_headers)
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["indicative"] is True
        assert data["n_devices"] == 5
        assert data["production_wh"] == pytest.approx(5 * 400.0)
        assert data["bucket"].startswith(bucket.strftime("%Y-%m-%dT%H"))

    async def test_a_member_cannot_read_the_community(self, client, populated, member_headers):
        """D-14: a member sees ONLY their own sharing operation(s), through /mine
        - never the community total. The role gate answers, with the auth code,
        whatever the visibility settings say."""
        response = await client.get("/summary", headers=member_headers)
        assert response.status_code == 403
        assert response.json()["error_code"] == 2

    async def test_there_is_no_consumption_key_at_all(self, client, populated, manager_headers):
        """Not `consumption: null`. The key is absent from the payload and the
        `absent` list names it with a reason - `null` is what a chart renders as
        zero, which is a claim nobody made."""
        data = (await client.get("/summary", headers=manager_headers)).json()["data"]
        assert "consumption_wh" not in data
        assert "consumption" not in data
        reasons = {item["term"]: item["reason"] for item in data["absent"]}
        assert reasons["consumption_wh"] == "no_consumption_in_phase_1"

    async def test_the_grid_terms_are_published_above_k(self, client, populated, manager_headers):
        data = (await client.get("/summary", headers=manager_headers)).json()["data"]
        assert data["n_members"] == 5
        assert data["import_wh"] == pytest.approx(5 * 40.0)
        assert data["export_wh"] == pytest.approx(5 * 20.0)

    async def test_power_is_derived_not_read_from_the_payload(
        self, client, populated, manager_headers
    ):
        """`W = wh * 3600 / interval_s` from the last complete interval. The
        protocol's `power_w` is optional and normally ABSENT for a P1 connector,
        so a summary sourced from it is blank over a full table."""
        data = (await client.get("/summary", headers=manager_headers)).json()["data"]
        # 5 devices x 100 Wh over 900 s = 5 x 400 W.
        assert data["power_w"] == pytest.approx(2000.0)

    async def test_an_empty_community_says_not_measured_rather_than_zero(
        self, client, community, manager_headers
    ):
        data = (await client.get("/summary", headers=manager_headers)).json()["data"]
        assert "production_wh" not in data
        reasons = {item["term"]: item["reason"] for item in data["absent"]}
        assert reasons["production_wh"] == "not_measured"
        assert reasons["import_wh"] == "not_measured"


class TestTheLastClosedHour:
    """The production card is labelled "last full hour". It used to be the NEWEST
    hour - which, from about :16 past every hour, is the hour in progress with a
    quarter or two in it, so the figure dropped at every turn of the hour and
    read as a fall in production.

    Closed means RECOMPUTED AFTER IT ENDED (`computed_at >= bucket + 1h`), not
    merely "in the past": between :00 and the first tick after it, the hour that
    has just ended still holds only the three quarters of its last recompute.
    """

    MEMBERS = 5

    async def _devices(self, session, id_community: int) -> list[int]:
        ids = []
        for member in range(1, self.MEMBERS + 1):
            ean = await create_owned_meter(session, id_community=id_community, id_member=member)
            ids.append(await create_device(session, id_community=id_community, ean=ean))
        return ids

    async def _quarter(self, session, id_community: int, devices: list[int], ts) -> None:
        for device in devices:
            await create_measurement(
                session,
                id_device=device,
                id_community=id_community,
                ts=ts,
                import_wh=10.0,
                export_wh=5.0,
                production_wh=100.0,
            )

    async def _tick(self, session, id_community: int, now) -> None:
        await refresh_community(
            session, SqlAlchemyCrmCoreRead(session), id_community=id_community, now=now
        )
        await rollups.tick_community(session, id_community=id_community, now=now)

    async def test_the_card_reads_the_last_closed_hour_not_the_one_in_progress(
        self, client, db_session, community, manager_headers
    ):
        closed = buckets.hour_floor(datetime.datetime.now(datetime.UTC)) - datetime.timedelta(
            hours=4
        )
        in_progress = closed + datetime.timedelta(hours=1)
        devices = await self._devices(db_session, community.id)
        for minutes in (15, 30, 45, 60):
            await self._quarter(
                db_session, community.id, devices, closed + datetime.timedelta(minutes=minutes)
            )
        # One quarter of the next hour, and the tick that follows it.
        await self._quarter(
            db_session, community.id, devices, in_progress + datetime.timedelta(minutes=15)
        )
        await self._tick(db_session, community.id, in_progress + datetime.timedelta(minutes=20))

        data = (await client.get("/summary", headers=manager_headers)).json()["data"]
        assert data["bucket"].startswith(closed.strftime("%Y-%m-%dT%H")), data["bucket"]
        assert data["production_wh"] == pytest.approx(self.MEMBERS * 400.0)

    async def test_an_hour_closes_once_a_tick_recomputes_it_after_its_end(
        self, client, db_session, community, manager_headers
    ):
        """The positive control: the card moves on, it is not stuck on one hour."""
        closed = buckets.hour_floor(datetime.datetime.now(datetime.UTC)) - datetime.timedelta(
            hours=4
        )
        following = closed + datetime.timedelta(hours=1)
        devices = await self._devices(db_session, community.id)
        for bucket in (closed, following):
            for minutes in (15, 30, 45, 60):
                await self._quarter(
                    db_session, community.id, devices, bucket + datetime.timedelta(minutes=minutes)
                )
        await self._tick(db_session, community.id, following + datetime.timedelta(minutes=65))

        data = (await client.get("/summary", headers=manager_headers)).json()["data"]
        assert data["bucket"].startswith(following.strftime("%Y-%m-%dT%H")), data["bucket"]

    async def test_a_community_whose_only_hour_is_still_open_says_so(
        self, client, db_session, community, manager_headers
    ):
        """Not "not measured": that copy says the meters cannot see production,
        which is false for a community in its first hour. It has been measured;
        the hour is not over."""
        opened = buckets.hour_floor(datetime.datetime.now(datetime.UTC)) - datetime.timedelta(
            hours=1
        )
        devices = await self._devices(db_session, community.id)
        await self._quarter(
            db_session, community.id, devices, opened + datetime.timedelta(minutes=15)
        )
        await self._tick(db_session, community.id, opened + datetime.timedelta(minutes=20))

        data = (await client.get("/summary", headers=manager_headers)).json()["data"]
        assert "bucket" not in data
        assert "production_wh" not in data
        reasons = {item["term"]: item["reason"] for item in data["absent"]}
        assert reasons["production_wh"] == "no_closed_hour_yet"
        assert reasons["import_wh"] == "no_closed_hour_yet"


class TestSummaryFreshness:
    """The same verdict as `/ops/health`, so the dashboard's stale banner and the
    Ops tab cannot disagree - whatever range the chart shows.

    Ticks with a FRESH clock: `populated` ticks on an hour boundary, which would
    make the lag depend on the minute the suite runs.
    """

    async def test_a_recent_tick_is_fresh(self, client, db_session, populated, manager_headers):
        community, _bucket = populated
        await rollups.tick_community(
            db_session, id_community=community.id, now=datetime.datetime.now(datetime.UTC)
        )
        data = (await client.get("/summary", headers=manager_headers)).json()["data"]
        assert data["rollup_freshness"] == "fresh"
        assert data["rollup_lag_minutes"] < 15

    async def test_a_tick_two_hours_ago_is_stale(
        self, client, db_session, community, manager_headers
    ):
        ticked_at = datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=2)
        await _populate(
            db_session,
            id_community=community.id,
            members=5,
            bucket=buckets.hour_floor(ticked_at) - datetime.timedelta(hours=2),
        )
        data = (await client.get("/summary", headers=manager_headers)).json()["data"]
        assert data["rollup_freshness"] == "stale"
        # `_populate` ticks on the hour boundary before `ticked_at`: 2 to 3 h ago.
        assert data["rollup_lag_minutes"] >= 115

    async def test_an_empty_community_has_never_been_rolled_up(
        self, client, community, manager_headers
    ):
        data = (await client.get("/summary", headers=manager_headers)).json()["data"]
        assert data["rollup_freshness"] == "never"
        assert "rollup_lag_minutes" not in data


class TestTheKThreshold:
    @pytest.fixture
    async def small(self, db_session: AsyncSession, community):
        """Two members - below the default k of 5."""
        bucket = datetime.datetime.now(datetime.UTC).replace(
            minute=0, second=0, microsecond=0
        ) - datetime.timedelta(hours=2)
        await _populate(db_session, id_community=community.id, members=2, bucket=bucket)
        return community, bucket

    async def test_production_is_published_below_k(self, client, small, manager_headers):
        """DECIDED 2026-09-16. Production is never subject to k.

        The alternative - suppressing the whole bucket - opens the product to a
        blank chart for every pilot community under five members, which is the
        same "looks broken to its own users" failure plan 9.4 warns about for the
        visibility defaults.
        """
        data = (await client.get("/summary", headers=manager_headers)).json()["data"]
        assert data["n_members"] == 2
        assert data["production_wh"] == pytest.approx(2 * 400.0)

    async def test_the_grid_terms_are_withheld_below_k(self, client, small, manager_headers):
        """Grid exchange IS household behaviour: an hour of a two-member
        community's import curve is essentially one household's occupancy."""
        data = (await client.get("/summary", headers=manager_headers)).json()["data"]
        assert "import_wh" not in data
        assert "export_wh" not in data
        reasons = {item["term"]: item["reason"] for item in data["absent"]}
        assert reasons["import_wh"] == "below_k_threshold"
        assert reasons["export_wh"] == "below_k_threshold"

    async def test_a_manager_is_not_exempt_from_k(self, client, small, manager_headers):
        """k protects members from each other AND from their administrator. A
        manager's privilege is over devices and settings, not over another
        member's consumption curve."""
        data = (await client.get("/summary", headers=manager_headers)).json()["data"]
        assert "import_wh" not in data

    async def test_the_series_omits_grid_terms_per_bucket_and_counts_them(
        self, client, small, manager_headers
    ):
        """PER BUCKET, not per request. An all-or-nothing bit computed as a MIN
        over a caller-chosen window is bisectable on the same grid, and one bad
        bucket would blank a 30-day chart."""
        response = await client.get("/series?resolution=hour", headers=manager_headers)
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["suppressed_buckets"] >= 1
        withheld = [p for p in data["points"] if "import_wh" not in p]
        assert withheld, "at least one bucket must have had its grid terms withheld"
        # And it still carries its production.
        assert withheld[0]["production_wh"] == pytest.approx(2 * 400.0)


class TestCriterionEight:
    """plan 14 criterion 8, WITH BOTH POSITIVE CONTROLS.

    "A member of a community with production visibility disabled gets 403 on
    /live/summary. A nulled field is not acceptable: balance minus consumption
    reconstructs production exactly."

    Since D-14 a member reads nothing community-wide: the criterion is asserted
    on `/mine/operations`, the member-floor read, which runs the same gate.
    `/summary` is now refused to a member by the ROLE gate (code 2) - which is
    exactly why the criterion must not be asserted there any more.

    Without the controls the criterion cannot fail - a service that 403s every
    member always, or 403s everyone always, passes it.
    """

    async def _disable(self, session: AsyncSession, id_community: int) -> None:
        await session.execute(
            text(
                "INSERT INTO community_live_settings (id_community, members_see_production, "
                "members_see_aggregate, k) VALUES (:c, FALSE, TRUE, 5) "
                "ON CONFLICT (id_community) DO UPDATE SET members_see_production = FALSE"
            ),
            {"c": id_community},
        )
        await session.flush()

    async def test_a_member_is_refused_when_production_is_hidden(
        self, client, db_session, populated, member_headers
    ):
        community, _ = populated
        await self._disable(db_session, community.id)
        response = await client.get("/mine/operations", headers=member_headers)
        assert response.status_code == 403
        assert response.json()["error_code"] == 2440

    async def test_positive_control_the_manager_still_gets_200(
        self, client, db_session, populated, manager_headers
    ):
        """CONTROL 1. The switch governs what MEMBERS see; an administrator who
        could lock themselves out of their own dashboard would file it as a bug -
        and a service that 403s everyone would pass the criterion above."""
        community, _ = populated
        await self._disable(db_session, community.id)
        assert (await client.get("/summary", headers=manager_headers)).status_code == 200

    async def test_positive_control_the_member_gets_200_again_when_re_enabled(
        self, client, db_session, populated, member_headers
    ):
        """CONTROL 2. Without it, a service that 403s every member on every
        request passes the criterion."""
        community, _ = populated
        await self._disable(db_session, community.id)
        assert (await client.get("/mine/operations", headers=member_headers)).status_code == 403

        await db_session.execute(
            text(
                "UPDATE community_live_settings SET members_see_production = TRUE "
                "WHERE id_community = :c"
            ),
            {"c": community.id},
        )
        await db_session.flush()
        assert (await client.get("/mine/operations", headers=member_headers)).status_code == 200

    async def test_hiding_the_aggregate_gives_the_same_code(
        self, client, db_session, populated, member_headers
    ):
        """The two switches are indistinguishable in the response. Telling a
        member WHICH one their administrator turned off is information about the
        community's configuration that the refusal exists to withhold."""
        community, _ = populated
        await db_session.execute(
            text(
                "INSERT INTO community_live_settings (id_community, members_see_production, "
                "members_see_aggregate, k) VALUES (:c, TRUE, FALSE, 5)"
            ),
            {"c": community.id},
        )
        await db_session.flush()
        response = await client.get("/mine/operations", headers=member_headers)
        assert response.status_code == 403
        assert response.json()["error_code"] == 2440


class TestSeriesWindows:
    async def test_the_default_window_needs_no_parameters(self, client, populated, manager_headers):
        """The SPA sends only `resolution`, so the default must never trip the
        snapping rule - the defaults are snapped by construction."""
        response = await client.get("/series", headers=manager_headers)
        assert response.status_code == 200
        assert response.json()["data"]["resolution"] == "hour"

    @pytest.mark.parametrize("resolution", ["quarter", "hour", "day"])
    async def test_every_resolution_answers(self, client, populated, manager_headers, resolution):
        response = await client.get(f"/series?resolution={resolution}", headers=manager_headers)
        assert response.status_code == 200
        assert response.json()["data"]["resolution"] == resolution

    async def test_an_unknown_resolution_is_422(self, client, populated, manager_headers):
        response = await client.get("/series?resolution=fortnight", headers=manager_headers)
        assert response.status_code == 422
        assert response.json()["error_code"] == 2443

    async def test_an_unsnapped_bound_is_refused_not_snapped(
        self, client, populated, manager_headers
    ):
        """plan 9.3's differencing-attack refusal, and the reason it is a 422
        rather than a courtesy: an attacker who can move a boundary by one minute
        can request two windows and subtract them. Snapping silently answers
        BOTH - identically, which is exactly what makes them subtractable."""
        response = await client.get(
            "/series?resolution=hour&from=2026-09-01T10:17:00Z", headers=manager_headers
        )
        assert response.status_code == 422
        assert response.json()["error_code"] == 2441

    async def test_a_snapped_bound_is_accepted(self, client, populated, manager_headers):
        """The positive control for the test above - without it, a handler that
        422'd every `from` would pass."""
        response = await client.get(
            "/series?resolution=hour&from=2026-09-01T10:00:00Z&to=2026-09-02T10:00:00Z",
            headers=manager_headers,
        )
        assert response.status_code == 200

    async def test_a_window_beyond_the_cap_is_422(self, client, populated, manager_headers):
        response = await client.get(
            "/series?resolution=quarter&from=2020-01-01T00:00:00Z&to=2026-01-01T00:00:00Z",
            headers=manager_headers,
        )
        assert response.status_code == 422
        assert response.json()["error_code"] == 2442

    async def test_the_hour_series_equals_the_sum_of_its_quarter_hours(
        self, client, populated, manager_headers
    ):
        """THE ROLLUP ACCEPTANCE TEST, read through the API.

        `hour` comes from `rollup_community_hour` and `quarter` straight off
        `measurement`, so this compares the tick's output against the rows it was
        computed from - end to end, through the same endpoint a chart uses.
        """
        _community, bucket = populated
        quarters = (
            await client.get(
                f"/series?resolution=quarter"
                f"&from={bucket.strftime('%Y-%m-%dT%H:%M:%SZ')}"
                f"&to={(bucket + datetime.timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ')}",
                headers=manager_headers,
            )
        ).json()["data"]
        hours = (
            await client.get(
                f"/series?resolution=hour"
                f"&from={bucket.strftime('%Y-%m-%dT%H:%M:%SZ')}"
                f"&to={(bucket + datetime.timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ')}",
                headers=manager_headers,
            )
        ).json()["data"]

        assert len(quarters["points"]) == 4
        assert len(hours["points"]) == 1
        quarter_total = sum(p["production_wh"] for p in quarters["points"])
        assert hours["points"][0]["production_wh"] == pytest.approx(quarter_total)


class TestSettings:
    async def test_get_returns_the_defaults_without_inserting(
        self, client, community, manager_headers, db_session
    ):
        """A GET that writes breaks on a read replica, audits a manager who
        merely opened a panel, and freezes today's default into a row - so a
        later platform change silently does not reach them."""
        response = await client.get("/settings", headers=manager_headers)
        assert response.status_code == 200
        data = response.json()["data"]
        assert data == {
            "members_see_production": True,
            "members_see_aggregate": True,
            "k": 5,
            "is_default": True,
        }
        rows = await db_session.scalar(
            text("SELECT count(*) FROM community_live_settings WHERE id_community = :c"),
            {"c": community.id},
        )
        assert rows == 0

    async def test_the_code_defaults_match_the_database_defaults(self, db_session, community):
        """Two sources for one privacy decision, pinned together.

        `api/live/visibility.DEFAULT_VISIBILITY` serves the community that has
        never saved the panel; `community_live_settings`'s column defaults serve
        the row that is inserted without them. A drift between them is a silent
        privacy change that applies only to communities which never opened the
        settings screen - visible to nobody.
        """
        from api.live.visibility import DEFAULT_VISIBILITY

        await db_session.execute(
            text("INSERT INTO community_live_settings (id_community) VALUES (:c)"),
            {"c": community.id},
        )
        row = (
            await db_session.execute(
                text(
                    "SELECT members_see_production, members_see_aggregate, k "
                    "FROM community_live_settings WHERE id_community = :c"
                ),
                {"c": community.id},
            )
        ).one()
        assert row.members_see_production is DEFAULT_VISIBILITY.members_see_production
        assert row.members_see_aggregate is DEFAULT_VISIBILITY.members_see_aggregate
        assert row.k == DEFAULT_VISIBILITY.k

    async def test_put_replaces_and_returns_the_new_values(
        self, client, community, manager_headers
    ):
        response = await client.put(
            "/settings",
            headers=manager_headers,
            json={"members_see_production": False, "members_see_aggregate": True, "k": 7},
        )
        assert response.status_code == 200
        assert response.json()["data"] == {
            "members_see_production": False,
            "members_see_aggregate": True,
            "k": 7,
            "is_default": False,
        }

    async def test_put_is_a_full_replacement_not_a_patch(self, client, community, manager_headers):
        """`extra="forbid"`, so a misspelled field is a 422 rather than a
        silently ignored privacy change. The failure that matters is a manager
        who believes they turned something off."""
        response = await client.put(
            "/settings",
            headers=manager_headers,
            json={
                "members_see_production": False,
                "members_see_aggregate": True,
                "k": 7,
                "members_see_prodcution": True,
            },
        )
        assert response.status_code == 422

    async def test_k_below_the_floor_is_refused(self, client, community, manager_headers):
        """Floor 3, mirroring `ck_community_live_settings_k_floor`. A k of 1 is
        not a privacy setting."""
        response = await client.put(
            "/settings",
            headers=manager_headers,
            json={"members_see_production": True, "members_see_aggregate": True, "k": 1},
        )
        assert response.status_code == 422

    async def test_a_member_cannot_read_the_settings(self, client, community, member_headers):
        assert (await client.get("/settings", headers=member_headers)).status_code == 403

    async def test_a_member_cannot_write_the_settings(self, client, community, member_headers):
        response = await client.put(
            "/settings",
            headers=member_headers,
            json={"members_see_production": True, "members_see_aggregate": True, "k": 5},
        )
        assert response.status_code == 403


class TestTenancy:
    async def test_another_communitys_data_never_appears(
        self, client, db_session: AsyncSession, community, manager_headers
    ):
        """The chokepoint, end to end. `_scoped` is what stands between these two
        communities, and a summary is where a tenancy bug shows up as someone
        else's energy rather than as an error."""
        other = await create_community(db_session)
        await create_subscription(
            db_session, id_community=other.id, feature=FeatureName.LIVE_DATA, is_active=True
        )
        bucket = datetime.datetime.now(datetime.UTC).replace(
            minute=0, second=0, microsecond=0
        ) - datetime.timedelta(hours=2)
        await _populate(db_session, id_community=other.id, members=5, bucket=bucket)

        mine = (await client.get("/summary", headers=manager_headers)).json()["data"]
        assert mine["n_devices"] == 0
        assert "production_wh" not in mine

        theirs = (
            await client.get(
                "/summary", headers=gateway_headers(other.auth_community_id, role="MANAGER")
            )
        ).json()["data"]
        assert theirs["n_devices"] == 5

    async def test_an_unsubscribed_community_is_refused(self, client, unsubscribed_community):
        response = await client.get(
            "/summary",
            headers=gateway_headers(unsubscribed_community.auth_community_id, role="MANAGER"),
        )
        assert response.status_code == 403
        assert response.json()["error_code"] == 1003
