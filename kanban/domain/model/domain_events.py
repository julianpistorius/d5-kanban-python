from abc import abstractmethod, ABCMeta
from utility.utilities import utc_now

_now = object()


class DomainEvent:

    def __init__(self, timestamp=_now, **kwargs):
        self.__dict__['timestamp'] = utc_now() if timestamp is _now else timestamp
        self.__dict__.update(kwargs)

    def __setattr__(self, key, value):
        raise AttributeError("DomainEvent attributes are read-only")

    def __eq__(self, rhs):
        return self.__dict__ == rhs.__dict__

    def __ne__(self, rhs):
        return self.__dict__ != rhs.__dict__

    def __repr__(self):
        return self.__class__.__qualname__ + "(" + ', '.join("{0}={1!r}".format(*item) for item in self.__dict__.items()) + ')'


class AbstractMessageHub:

    __metaclass__ = ABCMeta

    @abstractmethod
    def subscribe(self, event_predicate, subscriber):
        raise NotImplementedError

    @abstractmethod
    def unsubscribe(self, event_predicate, subscriber):
        raise NotImplementedError

    @abstractmethod
    def publish(self, event):
        raise NotImplementedError
