"""The API image contains every first-party package the API imports.

`Dockerfile.production` copies an explicit list of directories. The dev
`Dockerfile` does `COPY . .`. So a package the API imports but the production
image does not copy passes pytest, ruff and mypy, runs in the dev container, and
dies only in production, at import, with ModuleNotFoundError.

That shipped. `api/live/read_service.py` imports `forecasting.registry` (build
step 9) and Dockerfile.production did not copy `forecasting/`. Found on
2026-10-01 by building the image and running `python -c "import main"` in it.
Nothing that runs against the source tree could see it.

This is tests/test_worker_import_graph.py's check, applied to the other image.
It needs no import blocker: what differs between this image and `.venv` is
first-party source, not the HTTP stack. It runs in a SUBPROCESS so that what it
observes is what `import main` loads, not what the rest of the suite has
already imported into the pytest interpreter.
"""

import json
import os
import pathlib
import subprocess
import sys

import pytest

SERVICE_ROOT = pathlib.Path(__file__).resolve().parents[1]

# A top-level package is a directory with an `__init__.py`, named the way a
# directory COPY names it; a top-level module is reported as its file name, the
# way `COPY main.py .` names it.
_PROBE = """
import importlib, json, os, sys

importlib.import_module("main")

here = os.getcwd()
first_party = set()
for name in list(sys.modules):
    root = name.split(".")[0]
    if os.path.isfile(os.path.join(here, root, "__init__.py")):
        first_party.add(root)
    elif os.path.isfile(os.path.join(here, root + ".py")):
        first_party.add(root + ".py")
print(json.dumps(sorted(first_party)))
"""


@pytest.fixture(scope="module")
def probe():
    return subprocess.run(  # noqa: S603
        [sys.executable, "-c", _PROBE],
        cwd=SERVICE_ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, "ENV": "test"},
        check=False,
    )


def _imported(probe) -> set[str]:
    assert probe.returncode == 0, probe.stderr
    return set(json.loads(probe.stdout.strip().splitlines()[-1]))


def _copied_into_the_image() -> set[str]:
    """Parse Dockerfile.production's COPY list.

    Parsed rather than restated, so the Dockerfile stays the single source of
    truth and this test cannot drift from it.
    """
    copied: set[str] = set()
    for line in (SERVICE_ROOT / "Dockerfile.production").read_text(encoding="utf-8").splitlines():
        fields = line.split()
        if not fields or fields[0] != "COPY":
            continue
        if any(f.startswith("--") for f in fields):
            continue  # COPY --from=builder, which brings site-packages, not source
        copied.add(fields[1].rstrip("/"))
    return copied


def test_main_imports_in_a_fresh_interpreter(probe):
    assert probe.returncode == 0, f"`import main` fails outside pytest:\n{probe.stderr}"


def test_every_package_the_api_imports_is_copied_into_the_image(probe):
    """The load-bearing one: `forecasting` showed up here as imported-not-copied."""
    missing = _imported(probe) - _copied_into_the_image()
    assert not missing, (
        f"the API imports {sorted(missing)}, which Dockerfile.production does not "
        "COPY, so the production image dies at startup with ModuleNotFoundError. "
        "Add the COPY, and the matching `<dir>/**` to both `paths:` lists in "
        ".github/workflows/build.yml."
    )


def test_the_probe_sees_first_party_imports(probe):
    """NEGATIVE CONTROL.

    A probe that observed nothing - a wrong cwd, a changed layout - would report
    an empty set, and an empty set is always a subset of the COPY list.
    """
    assert {"main.py", "api", "core", "domain"} <= _imported(probe)
