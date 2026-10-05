"""The two background processes: MQTT ingest, and the scheduled jobs.

``main`` is the ingest entrypoint (``python -m worker.main``) and ``scheduler_main``
the scheduler's (``python -m worker.scheduler_main``). They share this package and
this image, and run as separate containers on purpose - ``worker/scheduler.py``
says why.

This docstring used to read "NATS worker for asynchronous document generation",
which was a copy of a sibling annexe's and wrong on both counts: there is no NATS
here and nothing is generated.
"""
