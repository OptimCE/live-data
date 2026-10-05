"""FastAPI dependency wiring.

Plain `Depends` assembly, no container - the same shape as the other OptimCE
annexes. Ports are injected through trivial provider functions so a test can
override them without a broker or a second database.

Two services rather than one, and the split is the security boundary:
`LiveDataService` serves the AUTHENTICATED surface and reads the tenant from the
request context; `EnrolmentService` serves the PUBLIC leg and must never touch
it. Sharing one class would make "does this code path read the ContextVar?" a
question about a method rather than about a module.
"""

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from api.live.read_service import LiveReadService
from api.live.repository import EnrolmentRepository, LiveRepository
from api.live.service import LiveDataService
from api.live_public.service import EnrolmentService
from core.config import settings
from core.database.database import get_crm_session, get_local_session
from ports.broker import DeviceBrokerPort
from ports.crm_operations import SqlAlchemyCrmOperationsRead
from ports.crm_read import SqlAlchemyCrmRead

# Which adapter backs the port is decided in `ports/providers.py`, not here, so
# the worker can make the same choice without importing fastapi. Re-exported
# because `app.dependency_overrides[deps.get_device_broker]` keys on THIS
# object - two definitions of "the same" provider cannot be overridden together.
from ports.providers import get_device_broker

__all__ = [
    "get_device_broker",
    "get_enrolment_service",
    "get_live_data_service",
    "get_live_read_service",
]


def get_live_data_service(
    local_session: AsyncSession = Depends(get_local_session),
    crm_session: AsyncSession = Depends(get_crm_session),
    broker: DeviceBrokerPort = Depends(get_device_broker),
) -> LiveDataService:
    return LiveDataService(
        local_session=local_session,
        crm_session=crm_session,
        repository=LiveRepository(local_session),
        crm_read=SqlAlchemyCrmRead(crm_session),
        broker=broker,
    )


def get_live_read_service(
    local_session: AsyncSession = Depends(get_local_session),
    crm_session: AsyncSession = Depends(get_crm_session),
) -> LiveReadService:
    """The READ surface. No broker dependency, deliberately.

    `LiveDataService` takes one because it drives dynsec; nothing a member
    can reach should be able to. Omitting the parameter is what makes that
    structural rather than a matter of review.
    """
    return LiveReadService(
        local_session=local_session,
        crm_session=crm_session,
        repository=LiveRepository(local_session),
        # SELECT-only CRM reads for the sharing-operation views (D-14): names,
        # coverage, and the caller's own operations. Nothing here writes.
        crm_operations=SqlAlchemyCrmOperationsRead(crm_session),
    )


def get_enrolment_service(
    local_session: AsyncSession = Depends(get_local_session),
    crm_session: AsyncSession = Depends(get_crm_session),
    broker: DeviceBrokerPort = Depends(get_device_broker),
) -> EnrolmentService:
    return EnrolmentService(
        local_session=local_session,
        crm_session=crm_session,
        # A DIFFERENT repository class, whose methods are all unscoped by
        # design. See api/live/repository.py for why they live apart from the
        # scoped ones rather than as `_unscoped` outliers among them.
        repository=EnrolmentRepository(local_session),
        crm_read=SqlAlchemyCrmRead(crm_session),
        broker=broker,
        lease_seconds=settings.ENROLMENT_CLAIM_LEASE_SECONDS,
    )
