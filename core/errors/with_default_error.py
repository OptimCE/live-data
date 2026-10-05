import logging
from functools import wraps

from core.errors.errors import Error, ErrorException

logger = logging.getLogger(__name__)


def with_default_error(default_error: Error):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except ErrorException as e:
                logger.error(f"Unexpected error : {e.error}")
                raise e  # let the global handler deal with it
            except Exception as e:
                logger.error("Unexpected error :", exc_info=e)
                raise ErrorException(default_error) from e  # promote to domain error

        return wrapper

    return decorator
