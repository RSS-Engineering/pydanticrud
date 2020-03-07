class DoesNotExist(BaseException):
    """Occurs when a requested record does not exist."""
    pass


class RevisionMismatch(BaseException):
    """Occurs when trying to update a record with an old revision."""
    pass
