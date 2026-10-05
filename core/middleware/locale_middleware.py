from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from core.context_vars import current_locale
from core.i18n import DEFAULT_LOCALE, SUPPORTED_LOCALES


class LocaleMiddleware(BaseHTTPMiddleware):
    """
    Reads the Accept-Language header and sets the current_locale ContextVar.

    Supported values: "fr", "en", "de", "nl" (more added by dropping a new
    locales/*.json file and adding the key to SUPPORTED_LOCALES in core/i18n.py).

    Falls back to DEFAULT_LOCALE ("fr") if header is absent or unsupported.
    """

    async def dispatch(self, request: Request, call_next):
        raw = request.headers.get("Accept-Language", DEFAULT_LOCALE)
        # Take the first language tag, strip region (e.g. "fr-BE" → "fr")
        locale = raw.split(",")[0].split("-")[0].strip().lower()
        resolved = locale if locale in SUPPORTED_LOCALES else DEFAULT_LOCALE

        token = current_locale.set(resolved)
        try:
            return await call_next(request)
        finally:
            current_locale.reset(token)
