from abc import abstractmethod, ABCMeta
from utility.time import utc_now

_now = object()


class DomainEvent:
    """A base class for all events in this domain.

    DomainEvents are value objects and all attributes are specified as keyword
    arguments at construction time. There is always a timestamp attribute which
    gives the event creation time in UTC, unless specified.  Events are
    equality comparable.
    """

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


_event_handlers = {}


def subscribe(event_predicate, subscriber):
    """Subscribe to events.

    Args:
        event_predicate: A callable predicate which is used to identify the events to which to subscribe.
        subscriber: A unary callable function which handles the passed event.
    """
    if event_predicate not in _event_handlers:
        _event_handlers[event_predicate] = set()
    _event_handlers[event_predicate].add(subscriber)


def unsubscribe(event_predicate, subscriber):
    """Unsubscribe from events.

    Args:
        event_predicate: The callable predicate which was used to identify the events to which to subscribe.
        subscriber: The subscriber to disconnect.
    """
    if event_predicate in _event_handlers:
        _event_handlers[event_predicate].discard(subscriber)


def publish(event):
    """Send an event to all subscribers.

    Each subscriber will receive each event only once, even if it has been subscribed multiple
    times, possibly with different predicates.

    Args:
        event: The object to be tested against by all registered predicate functions and sent to
            all matching subscribers.
    """
    matching_handlers = set()
    for event_predicate, handlers in _event_handlers.items():
        if event_predicate(event):
            matching_handlers.update(handlers)

    for handler in matching_handlers:
        handler(event)
