class PersistenceSubscriber:

    def __init__(self, hub, event_store):
        self._hub = hub
        self._event_store = event_store
        self._hub.subscribe(lambda topic: True, self.store_event)
        self._event_store = event_store

    @staticmethod
    def qualified_name(topic):
        return topic.__module__ + '.' + topic.__qualname__

    def store_event(self, topic, *args, **kwargs):
        self._event_store.append(*args, topic=self.qualified_name(topic), **kwargs)
