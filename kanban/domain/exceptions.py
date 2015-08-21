class ConstraintError(Exception):
    """To be raised when an operation would otherwise cause a domain model constraint violation."""
    pass


class ConsistencyError(Exception):
    """To be raised when an internal consistency problem is detected."""
    pass
