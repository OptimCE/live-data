"""The ops surface. Build step 11.

plan 16's register, made visible: the third device state, `diag` surfaced, and
the `last_seen_at` coarsening that stops an administrator reading a member's
occupancy out of a maintenance page.
"""

import datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from domain import buckets
from domain.device_health import HINTS, DeviceHealth, classify, ingest_looks_healthy
from shared.const import DeviceStatus, DeviceType
from tests.conftest import gateway_headers
from tests.factories.device_factory import (
    create_device,
    create_hour_of_measurements,
    create_measurement,
)
from tests.factories.meter_factory import create_owned_meter
from tests.factories.subscription_factory import create_community, create_subscription
from worker import rollups

NOW = datetime.datetime.now(datetime.UTC)


async def _seen(
    session: AsyncSession,
    *,
    id_device: int,
    id_community: int,
    last_seen_at: datetime.datetime | None,
    online: bool | None = True,
    diag: str | None = None,
) -> None:
    await session.execute(
        text(
            "INSERT INTO device_last (id_device, id_community, last_seen_at, online, diag, "
            "status_at) VALUES (:d, :c, :seen, :online, :diag, :seen) "
            "ON CONFLICT (id_device) DO UPDATE SET last_seen_at = :seen, online = :online, "
            "diag = :diag"
        ),
        {"d": id_device, "c": id_community, "seen": last_seen_at, "online": online, "diag": diag},
    )
    await session.flush()


async def _rejected(
    session: AsyncSession,
    *,
    id_device: int,
    id_community: int,
    reason: str,
    at: datetime.datetime,
) -> None:
    """The two columns ingest writes when it drops something - see worker/ingest.py."""
    await session.execute(
        text(
            "INSERT INTO device_last (id_device, id_community, last_reject_reason, last_reject_at) "
            "VALUES (:d, :c, :r, :at) "
            "ON CONFLICT (id_device) DO UPDATE SET last_reject_reason = :r, last_reject_at = :at"
        ),
        {"d": id_device, "c": id_community, "r": reason, "at": at},
    )
    await session.flush()


class TestTheThirdState:
    """plan 16: "'Online but reporting zeros for 24 h' is invisible, and it is
    exactly what a connected-but-unenabled P1 looks like."

    These are pure - the classifier takes every input as a parameter, so the
    states can be produced without arranging a database into each one.
    """

    def test_connected_and_reporting_zeros_is_its_own_state(self):
        assert (
            classify(
                now=NOW,
                status=DeviceStatus.ACTIVE,
                last_seen_at=NOW - datetime.timedelta(minutes=5),
                online=True,
                energy_wh_recent=0.0,
            )
            is DeviceHealth.REPORTING_ZEROS
        )

    def test_connected_and_reporting_energy_is_ok(self):
        assert (
            classify(
                now=NOW,
                status=DeviceStatus.ACTIVE,
                last_seen_at=NOW - datetime.timedelta(minutes=5),
                online=True,
                energy_wh_recent=1234.0,
            )
            is DeviceHealth.OK
        )

    def test_no_rollup_at_all_is_not_reported_as_zeros(self):
        """None is "no rollup covers the window", which is NOT the same as zero.

        A device enrolled ten minutes ago has no rollup yet, and calling that
        "reporting zeros" would make every new enrolment look broken on the day
        someone is standing next to it with a phone.
        """
        assert (
            classify(
                now=NOW,
                status=DeviceStatus.ACTIVE,
                last_seen_at=NOW - datetime.timedelta(minutes=5),
                online=True,
                energy_wh_recent=None,
            )
            is DeviceHealth.OK
        )

    def test_never_seen_beats_everything(self):
        assert (
            classify(
                now=NOW,
                status=DeviceStatus.ACTIVE,
                last_seen_at=None,
                online=None,
                energy_wh_recent=None,
            )
            is DeviceHealth.NEVER_SEEN
        )

    def test_silence_beats_offline(self):
        """Two days of silence is a different problem from a clean disconnect,
        and the one an operator should be told about."""
        assert (
            classify(
                now=NOW,
                status=DeviceStatus.ACTIVE,
                last_seen_at=NOW - datetime.timedelta(days=2),
                online=False,
                energy_wh_recent=0.0,
            )
            is DeviceHealth.SILENT
        )

    def test_an_ingest_outage_does_not_condemn_the_whole_fleet(self):
        """`worker/main.py` DISCONNECTS deliberately after N consecutive database
        failures, pushing the backlog into the broker's persistent session. While
        that happens every device stops being seen - and a sweep that reported
        them all as dead would turn one database incident into forty false alarms
        at the moment an operator needs the signal."""
        assert (
            classify(
                now=NOW,
                status=DeviceStatus.ACTIVE,
                last_seen_at=NOW - datetime.timedelta(days=3),
                online=None,
                energy_wh_recent=None,
                ingest_healthy=False,
            )
            is DeviceHealth.UNKNOWN
        )

    def test_a_revoked_device_is_revoked_whatever_it_last_did(self):
        """A revoked device cannot publish again, so "silent", "offline" and "never
        seen" are all true of it and none of them is actionable. Classified from
        `device_last` alone - which revocation never touches - every revoked
        device read SILENT a day later and needed attention for ever."""
        for last_seen_at, online in (
            (NOW - datetime.timedelta(days=3), False),
            (NOW - datetime.timedelta(minutes=2), True),
            (None, None),
        ):
            assert (
                classify(
                    now=NOW,
                    status=DeviceStatus.REVOKED,
                    last_seen_at=last_seen_at,
                    online=online,
                    energy_wh_recent=None,
                )
                is DeviceHealth.REVOKED
            )

    def test_revocation_beats_an_ingest_outage(self):
        """Nothing about the collector changes what a revoked device is."""
        assert (
            classify(
                now=NOW,
                status=DeviceStatus.REVOKED,
                last_seen_at=NOW - datetime.timedelta(days=3),
                online=None,
                energy_wh_recent=None,
                ingest_healthy=False,
            )
            is DeviceHealth.REVOKED
        )

    def test_the_status_is_a_required_keyword(self):
        """Not defaulted. `ingest_healthy` WAS defaulted, and for a while no
        caller passed it - which is how UNKNOWN became unreachable behind a green
        test. A defaulted `status` would fail the same way, silently."""
        with pytest.raises(TypeError):
            classify(now=NOW, last_seen_at=None, online=None, energy_wh_recent=None)

    def test_the_outage_verdict_is_now_actually_produced(self):
        """The test above passed for a while over a path NO CALLER REACHED.

        `classify` has always had the switch; nothing passed it, so `UNKNOWN`
        was unreachable, the runbook's `unknown` triage row described a state
        that could not occur, and this class was green about a guarantee the
        product did not have. `ingest_looks_healthy` is what produces the input,
        and the API tests below are what prove it arrives.
        """
        quiet = [NOW - datetime.timedelta(days=3), NOW - datetime.timedelta(days=3)]
        assert ingest_looks_healthy(now=NOW, last_seen=quiet) is False

    def test_one_recent_report_clears_the_whole_fleet(self):
        """Because the signal is CORRELATION. If anything is arriving, the path
        works, and every quiet device is quiet for its own reason."""
        mixed = [NOW - datetime.timedelta(days=3), NOW - datetime.timedelta(minutes=2)]
        assert ingest_looks_healthy(now=NOW, last_seen=mixed) is True

    def test_a_single_device_is_never_an_outage(self):
        """THE FLOOR OF TWO. With one meter, "the fleet is silent" and "this
        meter is dead" are the same sentence - and answering UNKNOWN hides the
        one an installer can act on behind the one they cannot."""
        alone = [NOW - datetime.timedelta(days=3)]
        assert ingest_looks_healthy(now=NOW, last_seen=alone) is True

    def test_devices_that_never_reported_are_not_evidence(self):
        """Forty never-enrolled devices are a deployment that has not started,
        not an ingest outage - and they carry no timestamp either way."""
        assert ingest_looks_healthy(now=NOW, last_seen=[None, None, None]) is True

    def test_every_state_has_a_hint(self):
        """A state with no hint renders an empty line under a broken device."""
        assert set(HINTS) == set(DeviceHealth)

    def test_every_hint_is_a_key_and_not_a_sentence(self):
        """The wire carries a KEY; the SPA owns the words.

        Deliberately asymmetric with errors, which this service DOES translate
        server-side in `core/errors/handlers.py`. The difference is the consumer:
        an error message goes to whoever called the API - three implementers,
        only one of them a browser - while a hint is a paragraph of installer
        guidance that exists to be rendered on exactly one screen.

        Keeping the words in `crm-frontend/src/assets/i18n/` rather than in both
        repositories removes a copy that would drift, and the parity assertion
        lives beside them in `live-data-format.spec.ts`, where a missing
        translation fails the build that would have shipped it.
        """
        for key in HINTS.values():
            assert key.startswith("LIVE.HINT.")
            assert key.upper() == key, "an i18n key, not a sentence"


@pytest.fixture
async def fleet(db_session: AsyncSession, community):
    """Four devices, one in each interesting state."""
    made = {}
    bucket = NOW.replace(minute=0, second=0, microsecond=0) - datetime.timedelta(hours=2)

    ean = await create_owned_meter(db_session, id_community=community.id, id_member=1)
    healthy = await create_device(db_session, id_community=community.id, ean=ean)
    await create_hour_of_measurements(
        db_session, id_device=healthy, id_community=community.id, bucket=bucket
    )
    await _seen(
        db_session,
        id_device=healthy,
        id_community=community.id,
        last_seen_at=NOW - datetime.timedelta(minutes=2),
    )
    made["healthy"] = healthy

    ean = await create_owned_meter(db_session, id_community=community.id, id_member=2)
    made["never"] = await create_device(db_session, id_community=community.id, ean=ean)

    ean = await create_owned_meter(db_session, id_community=community.id, id_member=3)
    silent = await create_device(db_session, id_community=community.id, ean=ean)
    await _seen(
        db_session,
        id_device=silent,
        id_community=community.id,
        last_seen_at=NOW - datetime.timedelta(days=3),
        diag="no_telegram",
    )
    made["silent"] = silent

    ean = await create_owned_meter(db_session, id_community=community.id, id_member=4)
    zeros = await create_device(db_session, id_community=community.id, ean=ean)
    await create_hour_of_measurements(
        db_session,
        id_device=zeros,
        id_community=community.id,
        bucket=bucket,
        import_wh=0.0,
        export_wh=0.0,
        production_wh=0.0,
    )
    await _seen(
        db_session,
        id_device=zeros,
        id_community=community.id,
        last_seen_at=NOW - datetime.timedelta(minutes=3),
        diag="port_closed",
    )
    made["zeros"] = zeros

    await rollups.tick_community(db_session, id_community=community.id, now=NOW)
    return made


class TestOpsHealth:
    async def test_the_fleet_is_reported_by_state(self, client, fleet, manager_headers):
        response = await client.get("/ops/health", headers=manager_headers)
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["n_devices"] == 4
        assert data["by_health"]["never_seen"] == 1
        assert data["by_health"]["silent"] == 1
        assert data["by_health"]["reporting_zeros"] == 1
        assert data["by_health"]["ok"] == 1

    async def test_a_device_that_has_never_reported_is_still_listed(
        self, client, fleet, manager_headers
    ):
        """THE LEFT-JOIN CASE. `device_last` has no row for it, so scoping the
        join in the WHERE clause would turn the outer join into an inner one and
        drop it - leaving a list that renders perfectly and is missing exactly
        the device an administrator opened the page to find."""
        data = (await client.get("/ops/health", headers=manager_headers)).json()["data"]
        states = [d["health"] for d in data["devices"]]
        assert "never_seen" in states

    async def test_diag_is_surfaced(self, client, fleet, manager_headers):
        """Stored since build step 3 and readable nowhere until now. "The
        difference between a diagnosis and a support call"."""
        data = (await client.get("/ops/health", headers=manager_headers)).json()["data"]
        # `.get`: a device with no diag carries no key at all (exclude_none).
        diags = {d["diag"] for d in data["devices"] if d.get("diag")}
        assert {"no_telegram", "port_closed"} <= diags

    async def test_a_fleetwide_blackout_reads_as_unknown_not_silent(
        self, client, db_session, community, manager_headers
    ):
        """The step-11 promise, end to end and through the gateway headers.

        `worker/main.py` disconnects DELIBERATELY after
        INGEST_DB_FAILURES_BEFORE_DISCONNECT consecutive database failures,
        pushing the backlog into the broker's persistent session. Every device
        stops being seen at once. Reporting them all SILENT turns one database
        incident into a page of false alarms pointing at the hardware, at the
        moment an operator most needs the signal - and each one costs a van.
        """
        for member in (11, 12, 13):
            ean = await create_owned_meter(db_session, id_community=community.id, id_member=member)
            device = await create_device(db_session, id_community=community.id, ean=ean)
            await _seen(
                db_session,
                id_device=device,
                id_community=community.id,
                last_seen_at=NOW - datetime.timedelta(days=2),
            )

        data = (await client.get("/ops/health", headers=manager_headers)).json()["data"]
        assert data["by_health"].get("unknown") == 3
        assert (
            "silent" not in data["by_health"]
        ), "a fleet that went quiet together is one fault, not three"
        assert {d["hint"] for d in data["devices"]} == {"LIVE.HINT.INGEST_DOWN"}

    async def test_one_dead_meter_is_still_reported_as_dead(self, client, fleet, manager_headers):
        """THE POSITIVE CONTROL, and the one that matters more.

        Without it the test above is satisfied by a service that answers
        `unknown` for everything and never sends anyone to a broken meter again.
        The `fleet` fixture has devices reporting seconds ago, so its silent one
        has no excuse.
        """
        data = (await client.get("/ops/health", headers=manager_headers)).json()["data"]
        assert data["by_health"]["silent"] == 1
        assert "unknown" not in data["by_health"]

    async def test_diagnostics_reaches_the_same_verdict(
        self, client, db_session, community, manager_headers
    ):
        """One device cannot see its own fleet, and this is the page from which
        someone decides whether to drive out to a meter."""
        ids = []
        for member in (21, 22):
            ean = await create_owned_meter(db_session, id_community=community.id, id_member=member)
            device = await create_device(db_session, id_community=community.id, ean=ean)
            await _seen(
                db_session,
                id_device=device,
                id_community=community.id,
                last_seen_at=NOW - datetime.timedelta(days=2),
            )
            ids.append(device)

        public_id = await db_session.scalar(
            text("SELECT public_id FROM device WHERE id = :d"), {"d": ids[0]}
        )
        data = (
            await client.get(f"/devices/{public_id}/diagnostics", headers=manager_headers)
        ).json()["data"]
        assert data["health"] == "unknown"

    async def test_the_rollup_freshness_is_reported(self, client, fleet, manager_headers):
        """A stale rollup means the scheduler is not ticking, and nothing else on
        this page would say so - every device can be perfectly healthy while the
        charts quietly stop moving."""
        data = (await client.get("/ops/health", headers=manager_headers)).json()["data"]
        assert data["newest_rollup_bucket"] is not None
        assert data["rollup_age_minutes"] is not None

    async def test_nothing_rolled_up_yet_is_absent_not_null(
        self, client, community, manager_headers
    ):
        """BUG: the Ops tab said "recomputed null minutes ago" on a fresh
        community. The route sent `"rollup_age_minutes": null`; the SPA - like
        every other read route here - expects an unanswered field to be ABSENT,
        and tested `!== undefined`. `null` passed that test and was interpolated."""
        response = await client.get("/ops/health", headers=manager_headers)
        assert response.status_code == 200
        data = response.json()["data"]
        for key in ("rollup_age_minutes", "newest_rollup_bucket", "rollup_lag_minutes"):
            assert key not in data, f"{key} must be absent, not null"
        assert data["rollup_freshness"] == "never"

    async def test_a_member_cannot_read_it(self, client, fleet, member_headers):
        assert (await client.get("/ops/health", headers=member_headers)).status_code == 403


class TestDiagnostics:
    async def test_one_device_with_its_hint(self, client, db_session, community, manager_headers):
        import uuid as _uuid

        public_id = _uuid.uuid4()
        ean = await create_owned_meter(db_session, id_community=community.id, id_member=1)
        device_id = await create_device(
            db_session, id_community=community.id, ean=ean, public_id=public_id
        )
        await _seen(
            db_session,
            id_device=device_id,
            id_community=community.id,
            last_seen_at=NOW - datetime.timedelta(minutes=1),
            diag="port_closed",
        )
        response = await client.get(f"/devices/{public_id}/diagnostics", headers=manager_headers)
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["diag"] == "port_closed"
        assert data["hint"].startswith("LIVE.HINT.")

    async def test_another_communitys_device_is_a_404(
        self, client, db_session, community, manager_headers
    ):
        """404 rather than 403: telling a caller that a device exists elsewhere
        is itself a disclosure, and it is the choice `get_device` already made."""
        import uuid as _uuid

        other = await create_community(db_session)
        await create_subscription(db_session, id_community=other.id)
        public_id = _uuid.uuid4()
        ean = await create_owned_meter(db_session, id_community=other.id, id_member=1)
        await create_device(db_session, id_community=other.id, ean=ean, public_id=public_id)

        response = await client.get(f"/devices/{public_id}/diagnostics", headers=manager_headers)
        assert response.status_code == 404


class TestTheCoarsening:
    """plan 16's privacy item: "`last_seen_at` bypasses `admin_can_view_individual`.
    'This member's device has been offline for three days' is the absence signal
    the consent flag exists to withhold."
    """

    async def _make(self, session, community, device_type: DeviceType):
        import uuid as _uuid

        public_id = _uuid.uuid4()
        ean = await create_owned_meter(session, id_community=community.id, id_member=1)
        device_id = await create_device(
            session,
            id_community=community.id,
            ean=ean,
            public_id=public_id,
            device_type=device_type,
        )
        await _seen(
            session,
            id_device=device_id,
            id_community=community.id,
            last_seen_at=NOW - datetime.timedelta(days=3),
        )
        return public_id

    async def test_a_consumption_device_shows_no_timestamps(
        self, client, db_session, community, manager_headers
    ):
        """`POST /devices` accepts `type: 2` today - "phase 1 enrols production
        only" is stated in three places and enforced in none - so the protection
        is in place BEFORE the path is reachable. The alternative was adding it
        at the moment somebody enrols the first consumption meter, which is the
        change that gets forgotten."""
        public_id = await self._make(db_session, community, DeviceType.CONSUMPTION)
        data = (
            await client.get(f"/devices/{public_id}/diagnostics", headers=manager_headers)
        ).json()["data"]
        # ABSENT from the payload, not null - which is what `DeviceStatusOut`
        # always documented, and what the SPA's optional types were written for.
        assert "last_seen_at" not in data
        assert "last_measurement_at" not in data
        # The state itself is NOT withheld - "offline" is actionable and is not
        # the absence signal. The TIMESTAMPS are what disclose the rhythm.
        assert data["health"] == "silent"
        assert data["online"] is not None

    async def test_positive_control_a_production_device_shows_them(
        self, client, db_session, community, manager_headers
    ):
        """Without this, a service that withheld `last_seen_at` from EVERY device
        would pass the test above while breaking the administrator's only view of
        their own production fleet."""
        public_id = await self._make(db_session, community, DeviceType.PRODUCTION)
        data = (
            await client.get(f"/devices/{public_id}/diagnostics", headers=manager_headers)
        ).json()["data"]
        assert data["last_seen_at"] is not None


class TestOpsTenancy:
    async def test_the_fleet_view_is_scoped(self, client, db_session, community, manager_headers):
        other = await create_community(db_session)
        await create_subscription(db_session, id_community=other.id)
        ean = await create_owned_meter(db_session, id_community=other.id, id_member=1)
        await create_device(db_session, id_community=other.id, ean=ean)

        mine = (await client.get("/ops/health", headers=manager_headers)).json()["data"]
        assert mine["n_devices"] == 0

        theirs = (
            await client.get(
                "/ops/health", headers=gateway_headers(other.auth_community_id, role="MANAGER")
            )
        ).json()["data"]
        assert theirs["n_devices"] == 1


class TestRevokedDevices:
    """A revoked device keeps its history - so it stays on the list - and it can
    never need attention again."""

    async def test_it_is_listed_as_revoked_and_counted_nowhere_else(
        self, client, db_session, community, fleet, manager_headers
    ):
        ean = await create_owned_meter(db_session, id_community=community.id, id_member=5)
        revoked = await create_device(
            db_session, id_community=community.id, ean=ean, status=DeviceStatus.REVOKED
        )
        await _seen(
            db_session,
            id_device=revoked,
            id_community=community.id,
            last_seen_at=NOW - datetime.timedelta(days=3),
        )

        data = (await client.get("/ops/health", headers=manager_headers)).json()["data"]

        # Still LISTED - verify-live-ingest.sh section L: its measurements are
        # still in the table and still need an owner and a place on this page.
        assert data["n_devices"] == 5
        assert data["by_health"]["revoked"] == 1
        # And not silent: only the fixture's genuinely silent device is.
        assert data["by_health"]["silent"] == 1
        row = next(d for d in data["devices"] if d["status"] == int(DeviceStatus.REVOKED))
        assert row["health"] == "revoked"
        assert row["hint"] == "LIVE.HINT.REVOKED"

    async def test_it_is_not_evidence_of_an_ingest_outage(
        self, client, db_session, community, manager_headers
    ):
        """One active meter silent for 30 h, and one device revoked after its last
        report 40 h ago. Revocation freezes `last_seen_at`, so counting the
        revoked device made two quiet devices out of one: the floor of two was
        met, the fleet read as an ingest outage, and the one meter an installer
        could fix was reported UNKNOWN."""
        import uuid as _uuid

        active_id = _uuid.uuid4()
        ean = await create_owned_meter(db_session, id_community=community.id, id_member=31)
        active = await create_device(
            db_session, id_community=community.id, ean=ean, public_id=active_id
        )
        await _seen(
            db_session,
            id_device=active,
            id_community=community.id,
            last_seen_at=NOW - datetime.timedelta(hours=30),
        )
        ean = await create_owned_meter(db_session, id_community=community.id, id_member=32)
        revoked = await create_device(
            db_session, id_community=community.id, ean=ean, status=DeviceStatus.REVOKED
        )
        await _seen(
            db_session,
            id_device=revoked,
            id_community=community.id,
            last_seen_at=NOW - datetime.timedelta(hours=40),
        )

        data = (await client.get("/ops/health", headers=manager_headers)).json()["data"]
        assert data["by_health"] == {"silent": 1, "revoked": 1}

        # The diagnostics page reads its fleet separately - same verdict.
        diagnosed = (
            await client.get(f"/devices/{active_id}/diagnostics", headers=manager_headers)
        ).json()["data"]
        assert diagnosed["health"] == "silent"


class TestRejectedReadings:
    """A per-reading rejection leaves no dead letter - the rest of the batch was
    stored - so the only trace is `device_last.last_reject_*`. A capacity mismatch
    clips every sunny peak this way while the Ops card reports nothing wrong."""

    async def test_devices_dropping_readings_are_counted(
        self, client, db_session, community, manager_headers
    ):
        now = datetime.datetime.now(datetime.UTC)
        cases = (
            # (member, reason, how long ago) - only the first one counts.
            (51, "implausible_production", datetime.timedelta(hours=1)),
            # MESSAGE-scoped: already counted as a dead letter.
            (52, "device_revoked", datetime.timedelta(hours=1)),
            # Outside the 24 h window.
            (53, "over_device_ceiling", datetime.timedelta(days=3)),
            # A reason this build does not know: ignored, never a 500.
            (54, "a_reason_from_the_future", datetime.timedelta(hours=1)),
        )
        for member, reason, ago in cases:
            ean = await create_owned_meter(db_session, id_community=community.id, id_member=member)
            device = await create_device(db_session, id_community=community.id, ean=ean)
            await _rejected(
                db_session,
                id_device=device,
                id_community=community.id,
                reason=reason,
                at=now - ago,
            )

        response = await client.get("/ops/health", headers=manager_headers)
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["n_devices_readings_rejected_24h"] == 1
        assert "implausible_production" in {d.get("last_reject_reason") for d in data["devices"]}

    async def test_a_fleet_that_drops_nothing_reports_zero(self, client, fleet, manager_headers):
        data = (await client.get("/ops/health", headers=manager_headers)).json()["data"]
        assert data["n_devices_readings_rejected_24h"] == 0


class TestRollupFreshness:
    """Is the SCHEDULER keeping up - which the newest bucket cannot say.

    Each test ticks with a FRESH clock rather than the module's `NOW`, because the
    route reads the wall clock: a tick placed on an hour boundary would make the
    age depend on the minute the suite happens to run.
    """

    async def _hour(self, session, community, *, bucket, member: int = 41) -> int:
        ean = await create_owned_meter(session, id_community=community.id, id_member=member)
        device = await create_device(session, id_community=community.id, ean=ean)
        await create_hour_of_measurements(
            session, id_device=device, id_community=community.id, bucket=bucket
        )
        return device

    async def _ops(self, client, headers) -> dict:
        response = await client.get("/ops/health", headers=headers)
        assert response.status_code == 200
        data: dict = response.json()["data"]
        return data

    async def test_a_quiet_fleet_does_not_read_as_a_stalled_scheduler(
        self, client, db_session, community, manager_headers
    ):
        """THE BUG. Nothing has been produced for five hours, so the newest bucket
        is five hours old - and the tick rewrote the whole window seconds ago."""
        now = datetime.datetime.now(datetime.UTC)
        await self._hour(
            db_session, community, bucket=buckets.hour_floor(now) - datetime.timedelta(hours=5)
        )
        await rollups.tick_community(db_session, id_community=community.id, now=now)

        data = await self._ops(client, manager_headers)
        assert data["rollup_age_minutes"] > 90, "the DATA is old..."
        assert data["rollup_freshness"] == "fresh", "...and the scheduler is fine"
        assert data["rollup_lag_minutes"] < 15

    async def test_no_recompute_for_two_hours_is_stale(
        self, client, db_session, community, manager_headers
    ):
        ticked_at = datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=2)
        await self._hour(
            db_session,
            community,
            bucket=buckets.hour_floor(ticked_at) - datetime.timedelta(hours=1),
        )
        await rollups.tick_community(db_session, id_community=community.id, now=ticked_at)

        data = await self._ops(client, manager_headers)
        assert data["rollup_freshness"] == "stale"
        assert data["rollup_lag_minutes"] == pytest.approx(120, abs=3)
        assert "rollup_computed_at" in data

    async def test_readings_nobody_rolled_up_for_two_hours_are_stale(
        self, client, db_session, community, manager_headers
    ):
        """A scheduler dead since the community's first reading: there is no
        rollup row at all, so without the pending marks this read "never" for
        ever - a verdict that could never go red."""
        now = datetime.datetime.now(datetime.UTC)
        await self._hour(
            db_session, community, bucket=buckets.hour_floor(now) - datetime.timedelta(hours=3)
        )
        # The marks ingest wrote with those readings, as if they had waited 2 h.
        await db_session.execute(
            text("UPDATE rollup_dirty SET marked_at = :at WHERE id_community = :c"),
            {"at": now - datetime.timedelta(hours=2), "c": community.id},
        )

        data = await self._ops(client, manager_headers)
        assert data["rollup_freshness"] == "stale"
        assert "rollup_pending_since" in data

    async def test_a_first_reading_before_its_first_tick_is_not_yet_a_fault(
        self, client, db_session, community, manager_headers
    ):
        now = datetime.datetime.now(datetime.UTC)
        ean = await create_owned_meter(db_session, id_community=community.id, id_member=42)
        device = await create_device(db_session, id_community=community.id, ean=ean)
        await create_measurement(
            db_session,
            id_device=device,
            id_community=community.id,
            ts=buckets.hour_floor(now),
            export_wh=10.0,
            production_wh=10.0,
        )

        data = await self._ops(client, manager_headers)
        assert data["rollup_freshness"] == "never"

    async def test_nothing_left_in_the_window_is_idle_not_stale(
        self, client, db_session, community, manager_headers
    ):
        """No bucket inside the 48 h window: the tick has nothing to rewrite, so
        `computed_at` cannot move and its age says nothing about the scheduler."""
        ticked_at = datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=49)
        await self._hour(
            db_session,
            community,
            bucket=buckets.hour_floor(ticked_at) - datetime.timedelta(hours=1),
        )
        await rollups.tick_community(db_session, id_community=community.id, now=ticked_at)

        data = await self._ops(client, manager_headers)
        assert data["rollup_freshness"] == "idle"
        assert "rollup_lag_minutes" not in data
