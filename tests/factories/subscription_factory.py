"""Factories for the CRM rows every authenticated request assumes.

House style: build, attach, FLUSH. Never commit - the per-test session runs
inside a connection-level transaction that is rolled back on teardown, and a
real commit would escape it.
"""

import datetime
from typing import cast

import factory

from core.database.models import Community, CommunitySubscription
from shared.const import FeatureName


class CommunityFactory(factory.Factory):
    class Meta:
        model = Community

    name = factory.Sequence(lambda n: f"Community {n}")
    auth_community_id = factory.Sequence(lambda n: f"auth-community-{n}")
    # Naive UTC: the CRM's community.created_at is TIMESTAMP without time zone.
    created_at = factory.LazyFunction(
        lambda: datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
    )
    updated_at = factory.LazyFunction(
        lambda: datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
    )


class CommunitySubscriptionFactory(factory.Factory):
    class Meta:
        model = CommunitySubscription

    feature = FeatureName.LIVE_DATA
    is_active = True
    created_at = factory.LazyFunction(
        lambda: datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
    )
    updated_at = factory.LazyFunction(
        lambda: datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
    )


async def create_community(session, **kwargs) -> Community:
    """Build a Community, attach to the session, flush. Never commit."""
    community = cast(Community, CommunityFactory.build(**kwargs))
    session.add(community)
    await session.flush()
    return community


async def create_subscription(session, *, id_community: int, **kwargs) -> CommunitySubscription:
    """Build a CommunitySubscription, attach to the session, flush. Never commit."""
    subscription = cast(
        CommunitySubscription,
        CommunitySubscriptionFactory.build(id_community=id_community, **kwargs),
    )
    session.add(subscription)
    await session.flush()
    return subscription
