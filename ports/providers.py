"""Which adapter backs each port.

FRAMEWORK-FREE ON PURPOSE. This module imports nothing from fastapi, so both
`api/live/deps.py` and `worker/` can call it - and `Dockerfile.worker` installs
no fastapi at all. The sibling annexes learned this the expensive way: a provider
that lived in `api/<domain>/deps.py` pulled the HTTP stack into a worker image
that does not contain it, which passes pytest, ruff and mypy locally and then
crash-loops the container.

`api/live/deps.py` RE-EXPORTS the provider rather than defining its own, because
`app.dependency_overrides` keys on the function OBJECT: two definitions of
"the same" provider cannot be overridden together.
"""

from ports.broker import DeviceBrokerPort, FakeDeviceBroker

# The process-wide control connection, created in main.py's lifespan (API) or
# worker/main.py (worker) and torn down with it.
#
# A module-level singleton rather than a per-request client, for a reason the
# gateway imposes: KrakenD cuts every request at 3000 ms with no per-route
# override, and a TCP plus TLS handshake inside that budget - before the command
# is even sent - is not viable.
#
# It is set rather than constructed here because `aiomqtt.Client.__init__` calls
# `asyncio.get_running_loop()`: constructing one at import time raises
# `RuntimeError: no running event loop`, which is a surprising way to discover
# that a port module ran too early.
_broker: DeviceBrokerPort | None = None


def set_device_broker(broker: DeviceBrokerPort | None) -> None:
    """Install (or clear) the process-wide broker. Called from a lifespan."""
    global _broker
    _broker = broker


def get_device_broker() -> DeviceBrokerPort:
    """The broker port.

    Falls back to the fake ONLY when nothing has been installed, which is the
    case under tests and in the `*-doc-gen` one-shot (whose whole job is to
    import the app and dump its OpenAPI, with placeholder database URLs and no
    broker in sight).

    It does NOT fall back when the real adapter merely cannot connect: that is a
    503 from the adapter, not a silent substitution. A fake that quietly stood in
    for a down broker would report enrolments as succeeding while creating no
    broker client at all - and the device would be handed a credential that
    authenticates nothing, which is the one failure mode the whole ordering rule
    in plan 8.2 exists to prevent.
    """
    if _broker is None:
        return FakeDeviceBroker()
    return _broker
