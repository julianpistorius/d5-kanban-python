from kanban.domain.model.domain_events import DomainEvent


class Entity:

    class Created(DomainEvent):
        pass

    class Deleted(DomainEvent):
        pass

    class AttributeChanged(DomainEvent):
        pass

    def __init__(self, id, version, hub):
        self._id = id
        self._version = version
        self._hub = hub

    def _increment_version(self):
        self._version += 1

    @property
    def id(self):
        return self._id

    @property
    def version(self):
        return self._version

    def _publish(self, topic, *args, **kwargs):
        if self._hub:
            self._hub.publish(topic, *args, **kwargs)

    def _attribute_changed(self, attr):
        self._increment_version()
        self._publish(Entity.AttributeChanged,
                      id=self.id,
                      version=self.version,
                      name=attr.fget.__name__,
                      value=attr.fget(self))

    def discard(self):
        self._publish(Entity.Deleted,
                    id=self.id,
                    version=self.version)
        self._hub = None
