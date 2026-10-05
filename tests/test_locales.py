"""Every error key resolves in all four locales.

A missing key renders its raw path to the user - `ERRORS.LIVE.TOKEN_NOT_FOUND` -
with no error raised anywhere, in any log, in any test. This file is the only
thing that catches it.

`_ALL_KEYS` is DERIVED from the error registry rather than listed, so adding a
code surfaces here as a missing translation instead of as a silent gap. The
`(errors.auth, errors.subscription, errors.live)` tuple below is the per-service
edit and the one thing that can go stale: a fourth domain class added to
shared/custom_errors.py and not added here would have its keys checked by
nothing at all. `test_every_domain_on_the_registry_is_covered` closes that.
"""

import pytest

from core.i18n import SUPPORTED_LOCALES, translate
from shared.custom_errors import errors

_DOMAINS = (errors.auth, errors.subscription, errors.live)

_ALL_KEYS = sorted(
    {e.key for domain in _DOMAINS for e in domain.__class__.__dict__.values() if hasattr(e, "key")}
)


def test_supported_locales_includes_fr_en_de_nl():
    assert {"fr", "en", "de", "nl"} <= SUPPORTED_LOCALES


def test_there_is_at_least_one_key_to_check():
    """Guards the guard.

    If the reflection below ever stopped finding keys - a refactor of
    `Error`, a rename of `.key` - every parametrised test would silently
    collapse to zero cases and this file would report success while checking
    nothing.
    """
    assert len(_ALL_KEYS) >= 16


def test_every_domain_on_the_registry_is_covered():
    """The `_DOMAINS` tuple matches what `errors` actually exposes.

    Without this, adding `errors.something_new` to shared/custom_errors.py and
    forgetting to add it above would leave its keys untranslated and unchecked.
    """
    exposed = {
        name
        for name in vars(errors)
        if not name.startswith("_") and hasattr(getattr(errors, name), "__class__")
    }
    covered = {name for name in exposed if any(getattr(errors, name) is d for d in _DOMAINS)}
    assert exposed == covered, f"not covered by test_locales: {sorted(exposed - covered)}"


@pytest.mark.parametrize("locale", sorted({"fr", "en", "de", "nl"}))
@pytest.mark.parametrize("key", _ALL_KEYS)
def test_every_key_resolves_in_every_locale(key: str, locale: str):
    message = translate(key, locale=locale)
    # `translate` returns the KEY ITSELF when it cannot resolve it, which is
    # exactly the user-visible failure this test exists to prevent.
    assert message != key, f"{key} is missing from locales/{locale}.json"
    assert isinstance(message, str)
    assert message.strip()


@pytest.mark.parametrize("locale", sorted({"fr", "en", "de", "nl"}))
def test_the_catch_all_500_is_localised(locale: str):
    """`ERRORS.INTERNAL` is NOT on the registry, so `_ALL_KEYS` cannot see it.

    It reached the user through `core/errors/handlers.unhandled_exception_handler`
    as a hard-coded French string, under a `translate(...) if False else ...`
    inherited from the sibling template - the dead branch keeping `locale`
    syntactically used, so neither ruff nor mypy had anything to say, and the key
    itself existing in no locale file at all.

    Every OTHER error on this service is translated. The one an English-speaking
    manager meets when something breaks was the exception.
    """
    message = translate("ERRORS.INTERNAL", locale=locale)
    assert message != "ERRORS.INTERNAL", f"missing from locales/{locale}.json"
    assert message.strip()


def test_the_catch_all_actually_differs_between_locales():
    """The positive control. Four identical strings would satisfy the test above
    and be exactly the bug: one language served to everyone."""
    seen = {translate("ERRORS.INTERNAL", locale=locale) for locale in ("fr", "en", "de", "nl")}
    assert len(seen) == 4


@pytest.mark.parametrize("key", _ALL_KEYS)
def test_every_message_carries_its_numeric_code(key: str):
    """House convention: the code is appended to the message text.

    It is what lets a support conversation get from "it said something went
    wrong" to a grep, and the frontend maps codes per feature rather than
    through a global registry - so the number on screen is often the only
    identifier a user can report.
    """
    code = next(
        e.code
        for domain in _DOMAINS
        for e in domain.__class__.__dict__.values()
        if hasattr(e, "key") and e.key == key
    )
    for locale in ("fr", "en", "de", "nl"):
        assert f"(code: {code})" in translate(key, locale=locale)
