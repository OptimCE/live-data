"""Shared plumbing for the scheduled jobs: advisory locks and sessions.

Framework-free, like `ports/providers.py`. Nothing here imports fastapi, because
`Dockerfile.worker` installs none.

----------------------------------------------------------------------------
WHY THE LOCK HOLDS ITS OWN CONNECTION.

`pg_try_advisory_lock` is SESSION-scoped: it is held until explicitly unlocked or
until the backend connection ends. Two consequences decide the shape of this
module.

1.  IT CANNOT RIDE A POOLED SESSION. Taken on a connection from the engine pool
    and not released, the lock survives the checkin and belongs to whichever
    caller next borrows that connection. The symptom is a job that never runs
    again and no error anywhere - the exact failure the lock exists to make
    impossible. So the lock takes a dedicated connection, and the `finally`
    releases it explicitly rather than relying on close.

2.  IT CANNOT BE THE TRANSACTION-SCOPED VARIANT EITHER. `pg_advisory_xact_lock`
    releases at COMMIT, and every job here spans several transactions on purpose
    - the rollup tick commits per community so one poisoned community cannot
    abort the others. An xact lock would be released by the first commit and the
    remaining communities would run unprotected, which is worse than no lock at
    all because it looks protected.
----------------------------------------------------------------------------
"""

import contextlib
import logging
from collections.abc import AsyncIterator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from core.database.database import crm_engine, local_engine

logger = logging.getLogger(__name__)

_TRY_LOCK = text("SELECT pg_try_advisory_lock(:key)")
_UNLOCK = text("SELECT pg_advisory_unlock(:key)")


@contextlib.asynccontextmanager
async def advisory_lock(
    key: int, *, engine: AsyncEngine | None = None, name: str = ""
) -> AsyncIterator[bool]:
    """Yield True if this process took `key`, False if another holds it.

    NON-BLOCKING. A tick that cannot get the lock skips its run and the next one
    tries again 15 minutes later; queueing behind the holder would pile ticks up
    behind one slow job and then run them all at once against the same rows.

    `engine` is a parameter so tests can pass the fixture engine. It defaults to
    the local one rather than being required, because every caller in `worker/`
    wants exactly that and an argument that is always the same value is an
    argument that eventually gets passed wrongly.
    """
    target = engine if engine is not None else local_engine
    async with target.connect() as connection:
        acquired = bool(await connection.scalar(_TRY_LOCK, {"key": key}))
        if not acquired:
            logger.info("advisory lock %s (%s) is held elsewhere - skipping this run", key, name)
            yield False
            return
        try:
            yield True
        finally:
            # Explicit, and not merely on the way out of the connection: an
            # engine that pools would otherwise hand the lock to the next
            # borrower. Suppressed because a failure here must not mask the
            # exception that is already unwinding.
            with contextlib.suppress(Exception):
                await connection.scalar(_UNLOCK, {"key": key})


def local_sessionmaker(engine: AsyncEngine | None = None) -> async_sessionmaker[AsyncSession]:
    """A sessionmaker for the owned database, bound to `engine` when given.

    The jobs take a sessionmaker rather than a session because each one owns
    several transactions; handing them a single session would make "commit per
    community" impossible to express.
    """
    return async_sessionmaker(
        engine if engine is not None else local_engine, expire_on_commit=False
    )


def crm_sessionmaker(engine: AsyncEngine | None = None) -> async_sessionmaker[AsyncSession]:
    """A sessionmaker for the CRM database. SELECT-only by Postgres role."""
    return async_sessionmaker(engine if engine is not None else crm_engine, expire_on_commit=False)
