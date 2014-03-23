from kanban.domain.model.domain_events import DomainEvent


class Entity:

    class Created(DomainEvent):
        pass

    class Discarded(DomainEvent):
        pass

    class AttributeChanged(DomainEvent):
        pass

    def __init__(self, id, version, hub):
        self._id = id
        self._version = version
        self._hub = hub
        self._discarded = False

    def _increment_version(self):
        self._version += 1

    @property
    def id(self):
        return self._id

    @property
    def version(self):
        return self._version

    def _publish(self, event):
        if self._hub:
            self._hub.publish(event)

    def _validate_event_originator(self, event):
        if event.originator_id != self.id:
            raise RuntimeError("Event mismatch id mismatch")
        if event.originator_version != self.version:
            raise RuntimeError("Event version mismatch")

    # TODO: Removing a board implies removing it's columns

