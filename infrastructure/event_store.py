import json
from infrastructure.transcoders import ObjectJSONEncoder, ObjectJSONDecoder


class EventStore:

    def __init__(self, store_path):
        self._store_path = store_path

    def append(self, topic, **attributes):
        with open(self._store_path, 'a+t') as store_file:
            event = dict(topic=topic,
                         attributes=attributes)
            json.dump(event, store_file, separators=(',',':'), sort_keys=True, cls=ObjectJSONEncoder)
            store_file.write('\n')

    def open_event_stream(self, predicate=lambda event: True):
        return EventStream(self._store_path, predicate)


class EventStream:

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


