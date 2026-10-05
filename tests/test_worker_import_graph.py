"""The worker image contains everything the worker imports, and no HTTP stack.

`Dockerfile.worker` installs `requirements/worker.txt`, which has no fastapi, no
uvicorn and no starlette. A worker module that reaches something importing them
passes pytest, ruff and mypy LOCALLY - where `.venv` has everything - and then
crash-loops the container at startup with ModuleNotFoundError. That shipped once
in a sibling service and crash-looped 62 times.

This cannot be tested in-process for the same reason it is dangerous: the pytest
interpreter has the whole HTTP stack installed. So the check runs in a SUBPROCESS
with a `sys.meta_path` finder that refuses the absent distributions, and then
compares what was actually imported against the Dockerfile's parsed COPY list.

----------------------------------------------------------------------------
THE API-DIRECTION RULE IS STRUCTURAL HERE, NOT MITIGATED.

administrative-document and billing both `COPY api/ api/` into their worker
images, because their scheduled sweeps construct the real HTTP-layer service
class. Each then carries three mitigations to keep fastapi from being REACHED at
runtime, and a test that only blocks the distributions.

live-data's worker needs none of api/, so `Dockerfile.worker` simply does not
copy it - and `test_every_package_the_worker_imports_is_copied_into_the_image`
then enforces "the worker must not import api.*" for free, with no extra
assertion and nothing to remember. `test_the_api_package_is_not_in_the_image`
pins that decision so a later COPY cannot be added silently.
----------------------------------------------------------------------------
"""

import json
import os
import pathlib
import subprocess
import sys

import pytest

SERVICE_ROOT = pathlib.Path(__file__).resolve().parents[1]

# Every module the worker entrypoint reaches. Listed explicitly, and EAGERLY
# imported by worker/main.py, because the probe can only observe imports that
# actually happen: a deferred `import ports.broker_mqtt` inside a coroutine would
# leave this test green while the container still crash-loops.
WORKER_MODULES = [
    # The ingest entrypoint and its message handler.
    "worker.main",
    "worker.ingest",
    # The SECOND entrypoint - `python -m worker.scheduler_main` - and everything
    # it reaches. Listed for exactly the same reason as the first: this probe can
    # only observe imports that actually happen, and an entrypoint absent from
    # this list is an entrypoint whose `import api.<anything>` ships a
    # crash-looping container that passes pytest, ruff and mypy on the way out.
    "worker.scheduler_main",
    "worker.scheduler",
    "worker.rollups",
    "worker.ownership",
    "worker.partitions",
    "worker.retention",
    "worker.context",
    # Both entrypoints read the live-data subscription set through it (D-12). It
    # reaches `ports.crm_read`, which is SQLAlchemy only - listed so that stays true.
    "worker.subscriptions",
]

# The distributions requirements/worker.txt does NOT install.
ABSENT_FROM_THE_WORKER_IMAGE = [
    "fastapi",
    "starlette",
    "uvicorn",
    "jose",
    "auth0",
    "segno",  # api.txt only - the QR renderer has no business in the worker
]

_PROBE = """
import importlib, json, sys

BLOCKED = %(blocked)r

class NotInTheWorkerImage:
    def find_module(self, fullname, path=None):
        return self.find_spec(fullname, path)

    def find_spec(self, fullname, path=None, target=None):
        root = fullname.split(".")[0]
        if root in BLOCKED:
            raise ImportError(
                f"{root} is not installed in the worker image "
                f"(requirements/worker.txt); reached via {fullname}"
            )
        return None

sys.meta_path.insert(0, NotInTheWorkerImage())

for module in %(modules)r:
    importlib.import_module(module)

import os
here = os.getcwd()
first_party = set()
for name, module in list(sys.modules.items()):
    root = name.split(".")[0]
    if os.path.isdir(os.path.join(here, root)) and os.path.isfile(
        os.path.join(here, root, "__init__.py")
    ):
        first_party.add(root)
print(json.dumps(sorted(first_party)))
"""


@pytest.fixture(scope="module")
def probe():
    source = _PROBE % {
        "blocked": ABSENT_FROM_THE_WORKER_IMAGE,
        "modules": WORKER_MODULES,
    }
    return subprocess.run(  # noqa: S603
        [sys.executable, "-c", source],
        cwd=SERVICE_ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, "ENV": "test"},
        check=False,
    )


def _packages_copied_into_the_image() -> set[str]:
    """Parse Dockerfile.worker's COPY list.

    Parsed rather than restated, so the Dockerfile stays the single source of
    truth and this test cannot drift from it.
    """
    copied: set[str] = set()
    for line in (SERVICE_ROOT / "Dockerfile.worker").read_text(encoding="utf-8").splitlines():
        fields = line.split()
        if not fields or fields[0] != "COPY":
            continue
        if any(f.startswith("--") for f in fields):
            continue  # COPY --from=builder, which brings site-packages, not source
        copied.add(fields[1].rstrip("/"))
    return copied


def test_the_worker_imports_with_the_api_stack_uninstalled(probe):
    assert probe.returncode == 0, (
        "the worker entrypoint reaches a package the worker image does not "
        f"install:\n{probe.stderr}"
    )


def test_every_package_the_worker_imports_is_copied_into_the_image(probe):
    """The load-bearing one.

    Because Dockerfile.worker does not copy api/, this assertion IS the
    "worker must not import api.*" rule - an `import api.live.deps` added to
    worker/ later shows up here as a package that is imported and not copied.
    """
    assert probe.returncode == 0, probe.stderr
    imported = set(json.loads(probe.stdout.strip().splitlines()[-1]))
    missing = imported - _packages_copied_into_the_image()
    assert not missing, (
        f"the worker imports {sorted(missing)}, which Dockerfile.worker does not "
        "COPY. Either the import is wrong or the COPY list is."
    )


def test_the_api_package_is_not_in_the_image():
    """Pin the divergence from the sibling annexes.

    Adding `COPY api/ api/` would silently re-open the whole crash-loop class
    that this service avoids structurally, so the absence is asserted rather
    than left to a comment.
    """
    assert "api" not in _packages_copied_into_the_image(), (
        "Dockerfile.worker now copies api/. live-data's worker has no reason to "
        "reach the HTTP layer, and not copying it is what makes "
        "test_every_package_the_worker_imports_is_copied_into_the_image enforce "
        "the rule. If this is genuinely needed, bring the sibling services' three "
        "mitigations with it."
    )


def test_the_probe_can_fail():
    """NEGATIVE CONTROL.

    A meta_path finder that silently did nothing would make every test above
    pass for ever. Import fastapi under the same finder and require the refusal.
    """
    source = _PROBE % {
        "blocked": ABSENT_FROM_THE_WORKER_IMAGE,
        "modules": ["fastapi"],
    }
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", source],
        cwd=SERVICE_ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, "ENV": "test"},
        check=False,
    )
    assert result.returncode != 0
    assert "not installed in the worker image" in result.stderr
