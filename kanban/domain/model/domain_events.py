
class DomainEvent:

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __eq__(self, rhs):
        return self.__dict__ == rhs.__dict__

    def __ne__(self, rhs):
        return self.__dict__ != rhs.__dict__

    def __repr__(self):
        """A valid Python expression string representation of the event."""
        return self.__class__.__qualname__ + "(" + ', '.join("{0}={1!r}".format(*item) for item in self.__dict__.items()) + ')'

    def jsonable(self):
        return self.__dict__
