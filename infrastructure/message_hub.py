class MessageHub:

    def __init__(self):
        self._event_handlers = {}

    def subscribe(self, event_predicate, subscriber):
        if event_predicate not in self._event_handlers:
            self._event_handlers[event_predicate] = set()
        self._event_handlers[event_predicate].add(subscriber)

    def unsubscribe(self, event_predicate, subscriber):
        if event_predicate in self._event_handlers:
            self._event_handlers[event_predicate].discard(subscriber)

    def publish(self, event):
        for event_predicate, handlers in self._event_handlers.items():
            if event_predicate(event):
                for handler in handlers:
                    handler(event)
