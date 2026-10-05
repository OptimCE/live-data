"""Step 4 of `handle_message`: a switched-off community's telemetry is discarded (D-12).

Every test runs against BOTH ways a community can be switched off - never
subscribed, and subscribed then unsubscribed. They are different rows in the
CRM: crm-backend's unsubscribe KEEPS the row with `is_active = false`, and a
check written as "a row exists" passes the first and waves the second through.

The active set is read through the real CRM adapter rather than written by
hand, so the fixture's row and the set the worker would hold cannot disagree.

What these pin, in the order the design depends on them:

  * the discard leaves NO trace - no measurement, no dirty mark, no
    `device_last`, no dead letter - and is not a rejection;
  * status is still processed, or a device that reconnected while switched off
    reads OFFLINE for ever after it is switched back on;
  * the three identity steps still run first, so an unknown, revoked or
    mismatched device is dead-lettered whatever the subscription says.
"""

import datetime
import inspect
import json
import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from domain.reasons import RejectReason
from ports.crm_read import SqlAlchemyCrmRead
from shared.const import DeviceStatus, FeatureName
from tests.factories.device_factory import create_device
from tests.factories.meter_factory import create_owned_meter
from worker.ingest import handle_message

NOW = datetime.datetime(2026, 7, 1, 12, 0, tzinfo=datetime.UTC)

TELEMETRY = json.dumps(
    {
        "v": 1,
        "measurements": [
            {
                "ts": "2026-07-01T11:45:00Z",
                "interval_s": 900,
                "import_wh": 0.0,
                "export_wh": 100.0,
                "production_wh": 100.0,
            }
        ],
    }
).encode()

STATUS = json.dumps(
    {"v": 1, "online": True, "connector": "optimce-connector", "version": "0.3.1"}
).encode()


@pytest.fixture(params=["unsubscribed_community", "deactivated_community"])
def switched_off(request):
    """Never subscribed, or subscribed and then switched off."""
    return request.getfixturevalue(request.param)


async def _active(session: AsyncSession) -> frozenset[int]:
    """The set the worker would hold, read the way the worker reads it."""
    return await SqlAlchemyCrmRead(session).active_communities_unscoped(
        feature=FeatureName.LIVE_DATA.value
    )


async def _device(
    session: AsyncSession, id_community: int, status: DeviceStatus = DeviceStatus.ACTIVE
) -> tuple[int, uuid.UUID]:
    ean = await create_owned_meter(session, id_community=id_community, id_member=1)
    public_id = uuid.uuid4()
    id_device = await create_device(
        session, id_community=id_community, ean=ean, status=status, public_id=public_id
    )
    return id_device, public_id


async def _count(session: AsyncSession, sql: str, **params) -> int:
    return int(await session.scalar(text(sql), params) or 0)


class TestTelemetryIsDiscarded:
    async def test_telemetry_from_a_switched_off_community_leaves_no_trace(
        self, db_session: AsyncSession, switched_off
    ):
        id_device, public_id = await _device(db_session, switched_off.id)
        topic = f"ce/{switched_off.id}/{public_id}/telemetry"
        active = await _active(db_session)
        assert switched_off.id not in active

        outcome = await handle_message(
            db_session, topic, TELEMETRY, NOW, None, active_communities=active
        )

        assert outcome.not_subscribed is True
        assert outcome.stored == 0
        assert outcome.rejected == []
        assert outcome.oldest_lateness_s is None
        for sql in (
            "SELECT count(*) FROM measurement WHERE id_device = :d",
            "SELECT count(*) FROM device_last WHERE id_device = :d",
            "SELECT count(*) FROM ingest_dead_letter WHERE id_device = :d",
        ):
            assert await _count(db_session, sql, d=id_device) == 0, sql
        # No dirty mark: the scheduler must find nothing to drain from it.
        assert (
            await _count(
                db_session,
                "SELECT count(*) FROM rollup_dirty WHERE id_community = :c",
                c=switched_off.id,
            )
            == 0
        )

    async def test_the_same_telemetry_is_stored_while_the_community_is_active(
        self, db_session: AsyncSession, community
    ):
        """The positive control. Without it the test above is satisfied by a
        handler that stores nothing for anyone."""
        id_device, public_id = await _device(db_session, community.id)
        active = await _active(db_session)

        outcome = await handle_message(
            db_session,
            f"ce/{community.id}/{public_id}/telemetry",
            TELEMETRY,
            NOW,
            None,
            active_communities=active,
        )

        assert outcome.not_subscribed is False
        assert outcome.stored == 1
        assert (
            await _count(
                db_session, "SELECT count(*) FROM measurement WHERE id_device = :d", d=id_device
            )
            == 1
        )


class TestStatusIsStillProcessed:
    async def test_status_is_still_processed_while_switched_off(
        self, db_session: AsyncSession, switched_off
    ):
        """Status carries no energy, and dropping it breaks reactivation.

        Protocol 3.2 publishes status only on connect and as the LWT. A device
        whose LWT (`online: false`) was processed while the community was on,
        and which reconnected while it was off, would have its `online: true`
        discarded - and then read OFFLINE in `domain/device_health` for as long
        as it stayed connected after being switched back on: recently seen, but
        `online is False`. Keeping one overwritten row current is what makes
        "switching back on resumes collection" true.
        """
        id_device, public_id = await _device(db_session, switched_off.id)
        active = await _active(db_session)

        outcome = await handle_message(
            db_session,
            f"ce/{switched_off.id}/{public_id}/status",
            STATUS,
            NOW,
            None,
            active_communities=active,
        )

        assert outcome.not_subscribed is False
        row = (
            await db_session.execute(
                text("SELECT online, status_at, ts FROM device_last WHERE id_device = :d"),
                {"d": id_device},
            )
        ).one()
        assert row.online is True
        assert row.status_at is not None
        assert row.ts is None, "status must not look like a measurement"
        version = await db_session.scalar(
            text("SELECT connector_version FROM device WHERE id = :d"), {"d": id_device}
        )
        assert version == "0.3.1"


class TestIdentityComesFirst:
    """Steps 1-3 run BEFORE the subscription check, for every community.

    Unknown, revoked and mismatched devices are access-control and operations
    signals, rare, and they must stay visible whatever the billing state. The
    dangerous direction is the mismatch: a check keyed on the TOPIC's community
    would quietly discard a device of an active community publishing under a
    switched-off community's id, and the mismatch counter would never see it.
    """

    async def test_an_unknown_device_is_still_dead_lettered(
        self, db_session: AsyncSession, switched_off
    ):
        topic = f"ce/{switched_off.id}/{uuid.uuid4()}/telemetry"
        outcome = await handle_message(
            db_session, topic, TELEMETRY, NOW, None, active_communities=await _active(db_session)
        )
        assert [reason for reason, _ in outcome.rejected] == [RejectReason.DEVICE_UNKNOWN]
        assert outcome.not_subscribed is False
        assert (
            await _count(
                db_session, "SELECT count(*) FROM ingest_dead_letter WHERE topic = :t", t=topic
            )
            == 1
        )

    async def test_a_revoked_device_is_still_dead_lettered(
        self, db_session: AsyncSession, switched_off
    ):
        id_device, public_id = await _device(
            db_session, switched_off.id, status=DeviceStatus.REVOKED
        )
        outcome = await handle_message(
            db_session,
            f"ce/{switched_off.id}/{public_id}/telemetry",
            TELEMETRY,
            NOW,
            None,
            active_communities=await _active(db_session),
        )
        assert [reason for reason, _ in outcome.rejected] == [RejectReason.DEVICE_REVOKED]
        assert (
            await _count(
                db_session,
                "SELECT count(*) FROM ingest_dead_letter WHERE id_device = :d",
                d=id_device,
            )
            == 1
        )

    async def test_an_active_device_claiming_a_switched_off_community_is_a_mismatch(
        self, db_session: AsyncSession, community, switched_off
    ):
        """THE ONE A TOPIC-KEYED CHECK WOULD HIDE. Discarded as `not_subscribed`
        it would never reach the mismatch counter."""
        id_device, public_id = await _device(db_session, community.id)
        outcome = await handle_message(
            db_session,
            f"ce/{switched_off.id}/{public_id}/telemetry",
            TELEMETRY,
            NOW,
            None,
            active_communities=await _active(db_session),
        )
        assert [reason for reason, _ in outcome.rejected] == [RejectReason.COMMUNITY_MISMATCH]
        assert outcome.not_subscribed is False
        assert (
            await _count(
                db_session,
                "SELECT count(*) FROM ingest_dead_letter WHERE id_device = :d",
                d=id_device,
            )
            == 1
        )

    async def test_a_switched_off_device_claiming_an_active_community_is_a_mismatch(
        self, db_session: AsyncSession, community, switched_off
    ):
        id_device, public_id = await _device(db_session, switched_off.id)
        outcome = await handle_message(
            db_session,
            f"ce/{community.id}/{public_id}/telemetry",
            TELEMETRY,
            NOW,
            None,
            active_communities=await _active(db_session),
        )
        assert [reason for reason, _ in outcome.rejected] == [RejectReason.COMMUNITY_MISMATCH]
        assert (
            await _count(
                db_session,
                "SELECT count(*) FROM measurement WHERE id_device = :d",
                d=id_device,
            )
            == 0
        )


class TestTheShapeOfTheDiscard:
    def test_the_discard_is_not_a_protocol_reason(self):
        """The frozen protocol has thirteen reasons and §4.2's table must equal
        `domain/reasons.py`. A switched-off community's device has done nothing
        wrong, so there is nothing for it to be told."""
        assert "not_subscribed" not in {reason.value for reason in RejectReason}
        assert len(list(RejectReason)) == 13

    def test_the_set_is_a_required_keyword(self):
        """No default, deliberately: a default of "everything" would switch the
        policy off, silently, for any caller that forgot it."""
        parameter = inspect.signature(handle_message).parameters["active_communities"]
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        assert parameter.default is inspect.Parameter.empty


class TestAPerReadingRejectionLeavesATrace:
    """Not about subscriptions - it sits here because this module drives
    `handle_message` end to end.

    A measurement-scoped rejection drops ONE reading and stores the rest of the
    batch, so it never reaches `ingest_dead_letter`. `device_last.last_reject_*`
    is then the only record anywhere that a reading was thrown away, and the
    devices screen and the Ops card read it. Nothing asserted that it is written.
    """

    async def test_a_clipped_reading_is_recorded_on_the_device(
        self, db_session: AsyncSession, community
    ):
        id_device, public_id = await _device(db_session, community.id)
        payload = json.dumps(
            {
                "v": 1,
                "measurements": [
                    {
                        "ts": "2026-07-01T11:30:00Z",
                        "interval_s": 900,
                        "import_wh": 0.0,
                        "export_wh": 100.0,
                        "production_wh": 100.0,
                    },
                    # The factory's device is 5 kVA: 5 x 0.25 h x 1.1 = 1375 Wh is
                    # the ceiling. A sunny peak from an inverter declared too small.
                    {
                        "ts": "2026-07-01T11:45:00Z",
                        "interval_s": 900,
                        "import_wh": 0.0,
                        "export_wh": 2000.0,
                        "production_wh": 2000.0,
                    },
                ],
            }
        ).encode()

        outcome = await handle_message(
            db_session,
            f"ce/{community.id}/{public_id}/telemetry",
            payload,
            NOW,
            None,
            active_communities=await _active(db_session),
        )

        assert outcome.stored == 1
        assert [reason for reason, _ in outcome.rejected] == [RejectReason.IMPLAUSIBLE_PRODUCTION]
        row = (
            await db_session.execute(
                text(
                    "SELECT last_reject_reason, last_reject_at FROM device_last "
                    "WHERE id_device = :d"
                ),
                {"d": id_device},
            )
        ).one()
        assert row.last_reject_reason == RejectReason.IMPLAUSIBLE_PRODUCTION.value
        assert row.last_reject_at is not None
        assert (
            await _count(
                db_session,
                "SELECT count(*) FROM ingest_dead_letter WHERE id_device = :d",
                d=id_device,
            )
            == 0
        ), "measurement-scoped: the batch was stored, so no dead letter"
