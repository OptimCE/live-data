"""The live-data subscription set the worker and the scheduler filter on (D-12).

`worker/subscriptions.SubscriptionCache` is small, and three of its properties
carry the whole design:

  * a failed refresh keeps the last set that loaded - and never RAISES once warm,
    because the worker calls it outside its per-message `try`;
  * while cold, a failed load raises instead of guessing, which is what keeps
    the worker off the broker until the CRM has answered;
  * the next refresh is due one TTL after the clock reading taken BEFORE the
    load, which is the contract the monorepo's verify script waits on - and a
    FAILED one, one TTL after the load gave up.

The CRM read behind it is tested against the real `community_subscription`
table, because the row crm-backend's unsubscribe leaves behind is an
`is_active = false` row, not a missing one.
"""

import asyncio

import pytest
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import Settings, settings
from ports.crm_read import FakeCrmRead, SqlAlchemyCrmRead
from shared.const import FeatureName
from tests.conftest import sessionmaker_for
from tests.factories.subscription_factory import create_community, create_subscription
from worker import subscriptions as subscriptions_module
from worker.subscriptions import (
    SubscriptionCache,
    SubscriptionsUnavailable,
    crm_subscription_loader,
)

TTL = 60.0


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class ScriptedLoader:
    """Answers from a list, in order: a frozenset is returned, an exception raised."""

    def __init__(self, *answers: frozenset[int] | BaseException) -> None:
        self.answers = list(answers)
        self.calls = 0

    async def __call__(self) -> frozenset[int]:
        self.calls += 1
        answer = self.answers.pop(0)
        if isinstance(answer, BaseException):
            raise answer
        return answer


def _refreshes(delta: dict) -> dict[str, float]:
    return {
        dict(attrs)["outcome"]: value
        for (name, attrs), value in delta.items()
        if name == "subscription.refreshes.total"
    }


def _cache(loader, clock=None) -> SubscriptionCache:
    return SubscriptionCache(loader, ttl_seconds=TTL, clock=clock or FakeClock())


class TestTheCache:
    async def test_the_first_read_loads_the_set(self, metric_delta, caplog):
        loader = ScriptedLoader(frozenset({1, 7}))
        with caplog.at_level("INFO", logger="worker.subscriptions"):
            assert await _cache(loader).get() == frozenset({1, 7})
        assert loader.calls == 1
        assert _refreshes(metric_delta()) == {"ok": 1}
        assert any(
            getattr(record, "operation", None) == "subscription:loaded" for record in caplog.records
        )

    async def test_the_crm_is_not_asked_again_inside_the_ttl(self):
        clock = FakeClock()
        loader = ScriptedLoader(frozenset({1}))
        cache = _cache(loader, clock)
        await cache.get()
        for step in (1.0, 30.0, TTL - 0.001):
            clock.now = 1000.0 + step
            assert await cache.get() == frozenset({1})
        assert loader.calls == 1, "one CRM round trip per TTL, not one per message"

    async def test_a_deactivation_is_seen_once_the_ttl_has_passed(self):
        clock = FakeClock()
        cache = _cache(ScriptedLoader(frozenset({1, 2}), frozenset({2})), clock)
        assert await cache.get() == frozenset({1, 2})
        clock.now += TTL
        assert await cache.get() == frozenset({2})

    async def test_the_next_refresh_is_timed_from_before_the_load(self):
        """THE VERIFY SCRIPT'S CONTRACT. A flip at t is seen by the first message
        after t + TTL. Timed from the END of a slow load instead, the deadline
        would slip by the load's own duration on every refresh."""
        clock = FakeClock()
        answers = iter([frozenset({1}), frozenset()])

        async def slow_load() -> frozenset[int]:
            clock.now += 10.0  # the CRM took ten seconds to answer
            return next(answers)

        cache = SubscriptionCache(slow_load, ttl_seconds=TTL, clock=clock)
        await cache.get()  # started at 1000, finished at 1010
        clock.now = 1000.0 + TTL  # due at 1060, not 1070
        assert await cache.get() == frozenset()

    async def test_an_empty_set_is_an_answer_not_a_failure(self, metric_delta):
        """Every community switched off is a legitimate state: counted `ok`, and
        not raised - the worker then discards everything, as asked."""
        assert await _cache(ScriptedLoader(frozenset())).get() == frozenset()
        assert _refreshes(metric_delta()) == {"ok": 1}

    async def test_a_failed_refresh_keeps_the_last_known_good_set(self, metric_delta, caplog):
        clock = FakeClock()
        cache = _cache(ScriptedLoader(frozenset({3}), ConnectionRefusedError("crm down")), clock)
        await cache.get()
        clock.now += TTL

        with caplog.at_level("WARNING", logger="worker.subscriptions"):
            assert await cache.get() == frozenset({3})

        assert _refreshes(metric_delta()) == {"ok": 1, "failed": 1}
        assert any(
            getattr(record, "operation", None) == "subscription:stale" for record in caplog.records
        )

    async def test_a_failed_refresh_is_retried_once_per_ttl_not_per_message(self):
        clock = FakeClock()
        loader = ScriptedLoader(
            frozenset({3}), ConnectionRefusedError("crm down"), frozenset({3, 4})
        )
        cache = _cache(loader, clock)
        await cache.get()
        clock.now += TTL
        await cache.get()  # the failed refresh
        for _ in range(50):
            clock.now += 0.5
            assert await cache.get() == frozenset({3})
        assert loader.calls == 2, "a failing CRM must not be asked once per message"
        clock.now = 1000.0 + 2 * TTL
        assert await cache.get() == frozenset({3, 4})
        assert loader.calls == 3

    async def test_a_hung_refresh_is_retried_one_ttl_after_it_gave_up(self, monkeypatch):
        """Timed from BEFORE a load that hangs to the timeout, the retry is
        already due when the load gives up whenever the TTL is no longer than the
        timeout - 5 s in the dev compose. Every message would then wait out
        another timeout, uncounted as a database failure, while the broker's
        messages queue in the worker's memory. The zero-duration loads above
        cannot see this; here the hung load moves the clock."""
        monkeypatch.setattr(subscriptions_module, "_LOAD_TIMEOUT_SECONDS", 0.05)
        ttl = 5.0
        clock = FakeClock()
        never = asyncio.Event()
        calls = 0

        async def load() -> frozenset[int]:
            nonlocal calls
            calls += 1
            if calls == 1:
                return frozenset({1})
            if calls == 2:
                clock.now += 5.0  # held for the whole production timeout
                await never.wait()
            return frozenset({1, 2})

        cache = SubscriptionCache(load, ttl_seconds=ttl, clock=clock)
        await cache.get()  # 1000
        clock.now += ttl  # 1005: due, and the refresh hangs until 1010
        assert await cache.get() == frozenset({1})
        clock.now += 0.1  # 1010.1: the very next message
        assert await cache.get() == frozenset({1})
        assert calls == 2, "the next message must not start another timeout"
        clock.now = 1010.0 + ttl  # one TTL after the load gave up
        assert await cache.get() == frozenset({1, 2})
        assert calls == 3

    async def test_a_cold_failure_raises_instead_of_guessing(self, metric_delta):
        """Guessing "everything" ingests what the owner switched off; guessing
        "nothing" discards the fleet. Neither is a default, so it raises - and
        the next call retries at once, because the caller's backoff bounds it."""
        loader = ScriptedLoader(ConnectionRefusedError("crm down"), frozenset({5}))
        cache = _cache(loader)

        with pytest.raises(SubscriptionsUnavailable) as caught:
            await cache.get()
        assert isinstance(caught.value.__cause__, ConnectionRefusedError)

        assert await cache.get() == frozenset({5}), "cold, the next call retries immediately"
        assert _refreshes(metric_delta()) == {"failed": 1, "ok": 1}

    async def test_a_hung_crm_read_is_bounded(self, monkeypatch):
        """A CRM that accepts the connection and never answers must not stall
        ingestion behind the one message that triggered the refresh."""
        monkeypatch.setattr(subscriptions_module, "_LOAD_TIMEOUT_SECONDS", 0.05)
        never = asyncio.Event()
        clock = FakeClock()
        calls = []

        async def load() -> frozenset[int]:
            calls.append(1)
            if len(calls) == 1:
                return frozenset({1})
            await never.wait()
            raise AssertionError("unreachable")  # pragma: no cover

        cache = SubscriptionCache(load, ttl_seconds=TTL, clock=clock)
        await cache.get()
        clock.now += TTL
        assert await asyncio.wait_for(cache.get(), timeout=5) == frozenset({1})

    async def test_changes_are_logged_with_the_communities_that_moved(self, caplog):
        clock = FakeClock()
        cache = _cache(ScriptedLoader(frozenset({1, 2}), frozenset({2, 3})), clock)
        await cache.get()
        clock.now += TTL

        with caplog.at_level("INFO", logger="worker.subscriptions"):
            await cache.get()

        changed = [
            record
            for record in caplog.records
            if getattr(record, "operation", None) == "subscription:changed"
        ]
        assert len(changed) == 1
        assert changed[0].activated == [3]
        assert changed[0].deactivated == [1]
        assert changed[0].levelname == "INFO"
        # The operation leads the message too: the dev formatter prints the
        # message alone, and this is the line an operator greps for.
        assert changed[0].getMessage().startswith("subscription:changed")

    async def test_an_unchanged_set_logs_nothing(self, caplog):
        clock = FakeClock()
        loader = ScriptedLoader(frozenset({1}), frozenset({1}))
        cache = _cache(loader, clock)
        await cache.get()
        clock.now += TTL
        caplog.clear()  # the first load's `subscription:loaded`
        with caplog.at_level("DEBUG", logger="worker.subscriptions"):
            await cache.get()
        assert loader.calls == 2, "the refresh did happen - it just had nothing to say"
        assert not [record for record in caplog.records if record.name == "worker.subscriptions"]

    async def test_everything_deactivating_at_once_is_a_warning(self, caplog):
        """The one change worth a warning. A wrong CRM_DATABASE_URL or a renamed
        feature key answers exactly this, without an error, and the worker then
        discards every reading it receives."""
        clock = FakeClock()
        cache = _cache(ScriptedLoader(frozenset({1, 2}), frozenset()), clock)
        await cache.get()
        clock.now += TTL

        with caplog.at_level("INFO", logger="worker.subscriptions"):
            await cache.get()

        (changed,) = [
            record
            for record in caplog.records
            if getattr(record, "operation", None) == "subscription:changed"
        ]
        assert changed.levelname == "WARNING"
        assert changed.deactivated == [1, 2]

    async def test_a_refresh_whose_logging_raises_still_returns_the_last_set(
        self, monkeypatch, metric_delta
    ):
        """Critique R1: `get()` is TOTAL once warm. The worker calls it OUTSIDE
        the per-message `try`, so the diff logging must be inside the same
        `except` as the load - an exception from it would otherwise end the MQTT
        session for the whole fleet."""
        clock = FakeClock()
        cache = _cache(ScriptedLoader(frozenset({1}), frozenset({2}), frozenset({2})), clock)
        await cache.get()
        clock.now += TTL

        def exploding_log(*_args, **_kwargs):
            raise RuntimeError("a log handler blew up")

        monkeypatch.setattr(subscriptions_module.logger, "log", exploding_log)
        assert await cache.get() == frozenset({1}), "the unlogged change is not adopted"
        assert _refreshes(metric_delta()) == {"ok": 1, "failed": 1}

        monkeypatch.undo()
        clock.now += TTL
        assert await cache.get() == frozenset({2}), "and it is picked up on the next refresh"


class TestTheCrmRead:
    async def test_only_active_live_data_rows_are_returned(self, db_session: AsyncSession):
        subscribed = await create_community(db_session)
        await create_subscription(
            db_session, id_community=subscribed.id, feature=FeatureName.LIVE_DATA, is_active=True
        )
        never = await create_community(db_session)
        switched_off = await create_community(db_session)
        await create_subscription(
            db_session,
            id_community=switched_off.id,
            feature=FeatureName.LIVE_DATA,
            is_active=False,
        )
        other_annexe = await create_community(db_session)
        await create_subscription(
            db_session, id_community=other_annexe.id, feature="billing", is_active=True
        )

        result = await SqlAlchemyCrmRead(db_session).active_communities_unscoped(
            feature=FeatureName.LIVE_DATA.value
        )

        created = {subscribed.id, never.id, switched_off.id, other_annexe.id}
        assert result & created == {subscribed.id}
        assert all(isinstance(id_community, int) for id_community in result)

    async def test_the_loader_binds_the_live_data_feature(self, db_session: AsyncSession):
        """Through the production loader and a CRM session, as the worker builds
        it - not through the adapter directly."""
        subscribed = await create_community(db_session)
        await create_subscription(
            db_session, id_community=subscribed.id, feature=FeatureName.LIVE_DATA, is_active=True
        )
        billing_only = await create_community(db_session)
        await create_subscription(
            db_session, id_community=billing_only.id, feature="billing", is_active=True
        )

        result = await crm_subscription_loader(sessionmaker_for(db_session))()

        assert subscribed.id in result
        assert billing_only.id not in result

    async def test_the_fake_agrees_with_the_adapter(self, db_session: AsyncSession):
        subscribed = await create_community(db_session)
        await create_subscription(
            db_session, id_community=subscribed.id, feature=FeatureName.LIVE_DATA, is_active=True
        )
        fake = FakeCrmRead(
            subscribed={(subscribed.id, FeatureName.LIVE_DATA.value), (99, "billing")}
        )

        assert await fake.active_communities_unscoped(feature=FeatureName.LIVE_DATA.value) == {
            subscribed.id
        }
        real = await SqlAlchemyCrmRead(db_session).active_communities_unscoped(
            feature=FeatureName.LIVE_DATA.value
        )
        assert subscribed.id in real
        assert ("active_communities_unscoped", FeatureName.LIVE_DATA.value) in fake.calls

    async def test_an_inactive_row_is_not_subscribed_for_enrolment_either(
        self, db_session: AsyncSession, deactivated_community
    ):
        """The scalar read the public enrolment leg uses answers the same way as
        the set: a kept `is_active = false` row is NOT a subscription."""
        assert (
            await SqlAlchemyCrmRead(db_session).is_feature_active_unscoped(
                id_community=deactivated_community.id, feature=FeatureName.LIVE_DATA.value
            )
            is False
        )


class TestTheSetting:
    @pytest.mark.parametrize("ttl", [0, -1, 3601])
    def test_a_ttl_outside_1_to_3600_is_refused_at_boot(self, ttl):
        """0 reads the CRM on every message; above an hour, switching live data
        off stops nothing for an hour. Unconditional, so this fires under
        ENV=test like everywhere else."""
        with pytest.raises(ValidationError, match="SUBSCRIPTION_CACHE_TTL_SECONDS"):
            Settings(**{**settings.model_dump(), "SUBSCRIPTION_CACHE_TTL_SECONDS": ttl})

    @pytest.mark.parametrize("ttl", [1, 5, 60, 3600])
    def test_the_bounds_themselves_boot(self, ttl):
        """The negative control: 5 is what the dev compose sets on the worker."""
        booted = Settings(**{**settings.model_dump(), "SUBSCRIPTION_CACHE_TTL_SECONDS": ttl})
        assert booted.model_dump()["SUBSCRIPTION_CACHE_TTL_SECONDS"] == ttl
