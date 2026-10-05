import json
import logging
from functools import cache
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

LOCALES_DIR = Path(__file__).parent.parent / "locales"
SUPPORTED_LOCALES = {"fr", "en", "de", "nl"}
DEFAULT_LOCALE = "fr"


@cache
def _load_locale(locale: str) -> dict[str, Any]:
    """Loads and caches a locales file. Called once per locales at runtime."""
    path = LOCALES_DIR / f"{locale}.json"
    if not path.exists():
        logger.warning(f"Locale file not found: {path}. Falling back to {DEFAULT_LOCALE}.")
        path = LOCALES_DIR / f"{DEFAULT_LOCALE}.json"
    with open(path, encoding="utf-8") as f:
        data: dict[str, Any] = json.load(f)
        return data


def translate(key: str, locale: str = DEFAULT_LOCALE) -> str:
    """
    Resolves a dot-notation key like "ERRORS.NURSE.NOT_FOUND" against the locales file.

    Falls back to DEFAULT_LOCALE if the locales is unsupported.
    Returns the key itself if the path is not found — never raises.

    Usage:
        translate("ERRORS.NURSE.NOT_FOUND", locales="fr")
        → "Infirmier introuvable (code: 4001)"
    """
    resolved_locale = locale if locale in SUPPORTED_LOCALES else DEFAULT_LOCALE
    data = _load_locale(resolved_locale)

    parts = key.split(".")
    node = data
    for part in parts:
        if not isinstance(node, dict) or part not in node:
            logger.warning(f"Translation key not found: '{key}' in locales '{resolved_locale}'")
            return key
        node = node[part]

    if not isinstance(node, str):
        logger.warning(f"Translation key '{key}' does not resolve to a string")
        return key

    return node
