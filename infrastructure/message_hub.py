class MessageHub:

    def __init__(self):
        self._topic_handlers = {}

    def subscribe(self, topic_selector, subscriber):
        if topic_selector not in self._topic_handlers:
            self._topic_handlers[topic_selector] = set()
        self._topic_handlers[topic_selector].add(subscriber)

    def unsubscribe(self, topic_selector, subscriber):
        if topic_selector in self._topic_handlers:
            self._topic_handlers[topic_selector].discard(subscriber)

    def publish(self, topic, *args, **kwargs):
        for topic_selector, handlers in self._topic_handlers.items():
            if topic_selector(topic):
                for handler in handlers:
                    handler(topic, *args, **kwargs)
