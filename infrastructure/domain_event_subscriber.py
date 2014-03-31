from kanban.domain.model.events import DomainEvent, subscribe


class PersistenceSubscriber:

    def __init__(self, event_store):
        self._event_store = event_store
        subscribe(lambda event: isinstance(event, DomainEvent), self.store_event)
        self._event_store = event_store

    @staticmethod
    def qualified_name(topic):
        return topic.__module__ + '#' + topic.__class__.__qualname__

    def store_event(self, event):
        topic = self.qualified_name(event)
        attributes = event.__dict__
        self._event_store.append(topic=topic, **attributes)
