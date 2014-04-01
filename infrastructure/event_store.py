import json
from infrastructure.transcoders import ObjectJSONEncoder, ObjectJSONDecoder


class EventStore:
    """A simple file-based event store which stores data in a JSON stream.
    """

    def __init__(self, store_path):
        """Open an event store.

        Args:
            store_path: THe path to a new or existing event store.
        """
        self._store_path = store_path

    def append(self, topic, **attributes):
        """Append an event.

        Args:
            topic: A string representing the event type, or topic.

            **attributes: Any attributes associated with the event.
                Attributes must be JSON serializable.
        """
        with open(self._store_path, 'a+t') as store_file:
            event = dict(topic=topic,
                         attributes=attributes)
            json.dump(event, store_file, separators=(',',':'), sort_keys=True, cls=ObjectJSONEncoder)
            store_file.write('\n')

    def open_event_stream(self, predicate=lambda event: True):
        """Open an event stream, optionally filtering for specific events.

        Args:
            predicate: An optional predicate function for filtering events. The predicate should
                accept a single argument which is a deserialized JSON object, that is, a dictionary
                with string keys and arbitrary values.

        Returns:
            An EventStream which can be used as a context manager.
            Iteration over the EventStream yields deserialised events (dictionaries).
        """
        return EventStream(self._store_path, predicate)


class EventStream:
    """A stream of events."""

    def __init__(self, store_path, predicate):
        self._store_path = store_path
        self._predicate = predicate
        self._store_file = None

    def __enter__(self):
        self._store_file = open(self._store_path, 'rt')
        return self

    def __exit__(self, *exc_info):
        self._store_file.close()

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            line = next(self._store_file)
            event = json.loads(line, cls=ObjectJSONDecoder)
            if self._predicate(event):
                return event


