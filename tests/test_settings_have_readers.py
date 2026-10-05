"""Every knob this service invents must reach code that acts on it.

---------------------------------------------------------------------------
WHY THIS EXISTS.

Two settings shipped with no Python reader at all:

- `MQTT_TLS`, documented in four READMEs and set to `true` in both the staging
  and production templates, was passed to neither `aiomqtt.Client`. Turning it
  on changed nothing;
- `INGEST_DEFAULT_MAX_WH_PER_INTERVAL`, which `scripts/sql/schema.sql` names as
  the value a NULL `max_wh_per_interval` defers to, while `worker/ingest.py`
  carried the number as a literal. Lowering it moved nothing.

Both are the same failure and it is a peculiarly quiet one: the setting exists,
validates, appears in `/health`, is documented, is deployed - and is inert. There
is no error, no warning and no test that can fail, because nothing runs.

This is deliberately a NAME-level check rather than a type or value check. What
rots is the wiring, and the wiring is exactly what a name appearing somewhere
proves and nothing else does.
---------------------------------------------------------------------------
"""

import ast
import pathlib
import re

import pytest

_ROOT = pathlib.Path(__file__).resolve().parents[1]
_CONFIG = _ROOT / "core" / "config.py"

# The prefixes live-data invented. Platform-wide settings (LOCAL_DB_*, LOGGING_*,
# KEYCLOAK_*) are inherited from the sibling template and are read by shared code
# this repository does not own, so holding them to this rule would assert nothing
# about live-data and would fail for reasons outside it.
_OWN_PREFIXES = (
    "INGEST_",
    "MQTT_",
    "ROLLUP_",
    "RETENTION_",
    "MAINTENANCE_",
    "BROKER_",
    "PARTITION_",
    "SUBSCRIPTION_",
)


def _knobs() -> list[str]:
    tree = ast.parse(_CONFIG.read_text(encoding="utf-8"))
    return sorted(
        {
            node.target.id
            for node in ast.walk(tree)
            if isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id.startswith(_OWN_PREFIXES)
        }
    )


def _production_sources() -> str:
    """Everything except `core/config.py` itself - and except `tests/`.

    Excluding the suite is the point. A knob read only by a test that asserts
    its default is precisely the dead knob this file is about, and searching the
    suite would let it pass.
    """
    parts = []
    for path in _ROOT.rglob("*.py"):
        rel = path.relative_to(_ROOT).as_posix()
        if rel.startswith((".venv/", "tests/")) or "__pycache__" in rel:
            continue
        if rel == "core/config.py":
            continue
        parts.append(path.read_text(encoding="utf-8", errors="replace"))
    return "\n".join(parts)


@pytest.fixture(scope="module")
def sources() -> str:
    return _production_sources()


def test_there_are_knobs_to_check(sources):
    """The negative control for the parametrisation below.

    An `_OWN_PREFIXES` that matched nothing, or an `ast` walk that stopped
    finding `AnnAssign`, would make every test below vacuously pass.
    """
    assert len(_knobs()) >= 20
    assert "MQTT_TLS" in _knobs()
    assert "INGEST_DEFAULT_MAX_WH_PER_INTERVAL" in _knobs()


@pytest.mark.parametrize("knob", _knobs())
def test_the_setting_reaches_code_that_acts_on_it(knob, sources):
    assert re.search(r"\b" + re.escape(knob) + r"\b", sources), (
        f"{knob} is declared in core/config.py and read by nothing outside it. "
        f"A setting that validates, deploys and is documented but reaches no code "
        f"is inert, and changing it produces silence rather than an error - which "
        f"is how MQTT_TLS shipped meaning nothing."
    )
