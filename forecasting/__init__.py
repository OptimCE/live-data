"""Forecast methods. Build step 9 - the extension point, and nothing else.

`methods_implemented/` IS EMPTY, and that is the deliverable: deviation 7 takes
forecasting out of phase 1 and ships only the seam. `GET /forecast` therefore
answers with an empty series and a NAMED reason rather than a 404 or a bare `[]`
- plan 14 criterion 7 is explicit that either of those is a failure, because once
methods do exist they are indistinguishable from a broken job.

Two discovery modes, exactly as `allocation-key-generation/algorithms/__init__.py`:

- `autodiscover()` - imports only each package's `__init__`, which registers
  metadata. No heavy dependencies. Safe from the API process.
- `autodiscover(load_implementations=True)` - also imports each package's
  `method` module, which registers the implementation class and may import numpy
  or similar. For the worker.

A package that fails to import is LOGGED AND SKIPPED, never allowed to take the
others down with it: one broken method must not remove every other method from
the admin screen.
"""

import importlib
import logging
import pkgutil
from pathlib import Path

logger = logging.getLogger(__name__)


def autodiscover(load_implementations: bool = False) -> None:
    """Discover method packages and register their metadata."""
    base = Path(__file__).parent / "methods_implemented"
    package_prefix = f"{__name__}.methods_implemented"

    for module_info in pkgutil.iter_modules([str(base)]):
        if not module_info.ispkg:
            continue

        pkg_name = f"{package_prefix}.{module_info.name}"
        try:
            importlib.import_module(pkg_name)
        except Exception:
            logger.exception("Failed to load forecast metadata for '%s'", module_info.name)
            continue

        if load_implementations:
            try:
                importlib.import_module(f"{pkg_name}.method")
            except Exception:
                logger.exception(
                    "Failed to load forecast implementation for '%s'", module_info.name
                )
