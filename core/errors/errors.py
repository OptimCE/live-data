class Error:
    """
    Represents an application error with a code and message.

    This class is used to define all possible errors that can occur in the application.
    Each error has a unique code and a human-readable message.

    Attributes:
        code (int): A unique error code.
        key (str): A key to a localized message in the application's locales files.
    """

    def __init__(self, code: int, key: str):
        self.code = code
        self.key = key


class ErrorException(Exception):  # noqa: N818  # public exception class; renaming would break ~30 call sites
    """
    Exception class for application errors.

    This exception wraps an Error object and is used to raise application-specific
    exceptions that can be caught and handled appropriately.

    Attributes:
        error (Error): The Error object containing the code and message.
    """

    def __init__(self, error: Error, status_code: int = 400):
        """
        Initialize an ErrorException with an Error object.

        Args:
            error (Error): The Error object to wrap in this exception.
        """
        self.error = error
        self.status_code = status_code

    def __str__(self):
        """
        Return a string representation of the exception.

        Returns:
            str: A formatted string containing the error code and message.
        """
        return f"Error {self.error.code}: {self.error.key}"
