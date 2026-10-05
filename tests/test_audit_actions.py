"""`AuditActions` and the call sites must agree in BOTH directions.

---------------------------------------------------------------------------
WHY BOTH DIRECTIONS.

`core/audit_log/actions.py` arrived as administrative-document's registry
verbatim: fifteen codes for dossiers, document versions and deadline rules, in
a service that has none of them, referenced by nothing and imported at every
boot because the package `__init__` re-exports it. live-data's own five codes
were string literals spread across three service modules.

Either half alone is worthless. A registry nobody uses is the file that was
there before. A "every constant is used" check alone would pass over a fourth
service module that kept writing literals - and the audit table is SHARED
across every annexe, so a literal that drifts does not fail: it files a row
under an action nobody queries, and it is found years later or not at all.
---------------------------------------------------------------------------
"""

import ast
import pathlib

import pytest

from core.audit_log.actions import AuditActions

_ROOT = pathlib.Path(__file__).resolve().parents[1]
_API = _ROOT / "api"

_REGISTERED = {
    value
    for name, value in vars(AuditActions).items()
    if not name.startswith("_") and isinstance(value, str)
}


def _literals_in_api() -> dict[str, str]:
    """Every `"live_data.*"` string constant still written by hand under `api/`."""
    found: dict[str, str] = {}
    for path in _API.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and node.value.startswith("live_data.")
            ):
                found[node.value] = f"{path.relative_to(_ROOT).as_posix()}:{node.lineno}"
    return found


def test_the_registry_is_this_service_s_and_not_a_sibling_s():
    """The negative control for everything below.

    With administrative-document's codes still in place, every other assertion
    here passes vacuously - the literals and the constants simply never meet.
    """
    assert _REGISTERED, "AuditActions is empty"
    assert all(code.startswith("live_data.") for code in _REGISTERED), sorted(_REGISTERED)


def test_no_call_site_still_writes_a_literal():
    stragglers = _literals_in_api()
    assert not stragglers, (
        "these action codes are written as literals rather than taken from "
        f"AuditActions: {stragglers}. The audit table is shared across annexes, "
        "so a drifted literal files a row nobody queries and nothing fails."
    )


@pytest.mark.parametrize("code", sorted(_REGISTERED))
def test_every_registered_code_is_actually_emitted(code):
    """The other direction. A constant nobody calls is how the fifteen foreign
    codes survived: declared, exported, imported at boot, emitted never."""
    blob = "\n".join(p.read_text(encoding="utf-8") for p in _API.rglob("*.py"))
    name = next(n for n, v in vars(AuditActions).items() if v == code)
    assert f"AuditActions.{name}" in blob, (
        f"{code} is registered and emitted by nothing. Either wire it up or "
        f"delete it - a registry that outgrows its call sites is the state this "
        f"file exists to catch."
    )
