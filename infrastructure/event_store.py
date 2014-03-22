import datetime
import json

class EventStore:

    def __init__(self, store_path):
        self._store_path = store_path


    def append(self, topic, *args, **kwargs):
        with open(self._store_path, 'a+t') as store_file:
            event = dict(timestamp=datetime.datetime.now(datetime.timezone.utc).timestamp(),
                         topic=topic,
                         args=args, kwargs=kwargs)
            json.dump(event, store_file, separators=(',',':'), sort_keys=True)
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
            event = json.loads(line)
            if self._predicate(event):
                return event
