from pydantic import BaseModel


class ApiResponse[T](BaseModel):
    """
    Standard API response model.

    This class provides a consistent structure for all API responses in the application.
    It includes fields for success status, data payload, and error code.

    Attributes:
        data (T): The response data. Type varies based on the endpoint.
        error_code (int): An error code if the request failed, 0 otherwise.
    """

    data: T
    error_code: int = 0


class Pagination(BaseModel):
    """
    Pagination information model.

    This class provides information about paginated results, including the current page,
    page size, total items, and total pages.

    Attributes:
        page (int): The current page number.
        limit (int): The maximum number of items per page.
        total (int): The total number of items across all pages.
        total_pages (int): The total number of pages.
    """

    page: int
    limit: int
    total: int
    total_pages: int


class ApiResponsePaginated[T](ApiResponse[T]):
    """
    Paginated API response model.

    This class extends the standard Response model to include pagination information.
    It's used for endpoints that return paginated results.

    Attributes:
        pagination (Pagination): Pagination information for the response.
    """

    pagination: Pagination
