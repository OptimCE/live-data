"""Adapters to systems this service does not own.

Each port is a Protocol plus at least one concrete implementation, so the callers
depend on the shape rather than the technology and tests can substitute a fake
without a broker or a second database.

- ``broker`` / ``broker_mqtt``  — the Mosquitto dynamic-security control plane.
- ``crm_read``                  — the two scalar CRM reads enrolment needs, and the
                                  set of subscribed communities the worker and
                                  the scheduler filter on (D-12).
- ``crm_core``                  — time-sliced CRM ownership, for the projection.
- ``weather``                   — a Protocol only. No adapter ships in phase 1;
                                  it arrives with the first forecast method, and
                                  ``httpx`` arrives with it (see requirements/base.txt).
- ``providers``                 — which adapter backs each port at runtime.

There is no ``events`` port: this service has no NATS. Nothing publishes and
nothing consumes, and the subject space ``optimce.live.>`` is merely reserved in
``shared/const.py``. The docstring here once advertised one - along with a
``crm_core_sqlalchemy`` module that never existed - because this file arrived as
a copy of a sibling annexe's.
"""
