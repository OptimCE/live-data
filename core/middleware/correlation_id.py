import logging
import uuid

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from core.context_vars import current_request_id

logger = logging.getLogger(__name__)

HEADER_NAME = "X-Request-ID"


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """
    Generates or propagates a correlation ID for every request.

    If the incoming request contains an X-Request-ID header (e.g. from a load
    balancer or API gateway), that value is reused. Otherwise a UUID4 is generated.

    The ID is stored in the current_request_id ContextVar (read by logging filter,
    tracing, error handlers) and returned in the X-Request-ID response header.
    """

    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get(HEADER_NAME) or str(uuid.uuid4())

        token = current_request_id.set(request_id)
        try:
            response = await call_next(request)
            response.headers[HEADER_NAME] = request_id
            return response
        finally:
            current_request_id.reset(token)
