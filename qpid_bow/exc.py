"""Exceptions."""


class MessageCorrupt(Exception):
    """Corrupt."""
    pass


class UnroutableMessage(Exception):
    """Origin message has no reply-to address."""
    pass


class RetriableMessage(Exception):
    """Release message back to the queue."""
    pass


class ObjectNotFound(Exception):
    """No object found."""
    def __init__(self, class_name, object_name):
        self.class_name = class_name
        self.object_name = object_name

        super().__init__(f"No {class_name} object found: {object_name}")


class TimeoutReached(Exception):
    """Timeout is reached."""
    pass


class QMF2Exception(Exception):
    """Generic QMF2 exception.

    Args:
        exception_message: Message to identify the reason of exception.
        exception_data: Additional data with error code and error text.
    """
    def __init__(self, exception_message: str, exception_data: dict) -> None:
        super().__init__(exception_message)
        self.exception_data = exception_data

    @staticmethod
    def from_data(exception_data: dict):
        """Try to initialise a specific QMF2Exception based on error code.

        Args:
            exception_data: Additional data with error code and error text.
        """
        error_code = exception_data.get('error_code')
        if error_code == QMF2ObjectExists.error_code:
            if ('object already exists' in
                    exception_data['error_text'].decode()):
                return QMF2ObjectExists(exception_data)
            return QMF2NotFound(exception_data)
        elif error_code == QMF2InvalidParameter.error_code:
            return QMF2InvalidParameter(exception_data)
        elif error_code == QMF2Forbidden.error_code:
            return QMF2Forbidden(exception_data)

        return QMF2Exception("Unknown QMF2 exception", exception_data)


class QMF2NotFound(QMF2Exception):
    """QMF2 object is not found.

    Args:
        exception_data: Additional data with error code and error text.
    """
    error_code = 7

    def __init__(self, exception_data):
        super().__init__("Object not found", exception_data)


class QMF2ObjectExists(QMF2Exception):
    """QMF2 object already exists.

    Args:
        exception_data: Additional data with error code and error text.
    """
    error_code = 7

    def __init__(self, exception_data):
        super().__init__("Object already exists", exception_data)


class QMF2InvalidParameter(QMF2Exception):
    """Invalid parameter is specified.

    Args:
        exception_data: Additional data with error code and error text.
    """
    error_code = 4

    def __init__(self, exception_data):
        super().__init__("Invalid parameter", exception_data)


class QMF2Forbidden(QMF2Exception):
    """Forbidden QMF2 call.

    Args:
        exception_data: Additional data with error code and error text.
    """
    error_code = 6

    def __init__(self, exception_data):
        super().__init__("Forbidden", exception_data)
