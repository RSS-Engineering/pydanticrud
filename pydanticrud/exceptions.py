class DoesNotExist(BaseException):
    """Occurs when a requested record does not exist."""
    pass


class ConditionCheckFailed(BaseException):
    """Occurs when the backend fails to complete an operation with a condition."""
    pass
