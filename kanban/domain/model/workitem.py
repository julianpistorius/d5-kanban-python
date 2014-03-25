
# ======================================================================================================================
# Aggregate root entity
#
from abc import ABCMeta, abstractmethod
import reprlib
import uuid
from singledispatch import singledispatch
from kanban.domain.model.entity import Entity
from utility.utilities import exactly_one


class WorkItem(Entity):

    class Created(Entity.Created):
        pass

    class Discarded(Entity.Discarded):
        pass

    def __init__(self, event, hub=None):
        """DO NOT CALL DIRECTLY.
        """
        super().__init__(event.originator_id, event.originator_version, hub)
        self._name = event.name
        self._due_date=event.due_date
        self._content = event.content

    def __repr__(self):
        return "{d}WorkItem(id={id!r}, name={name!r}, due_date={date!r}, content={content})".format(
            d="*Discarded* " if self._discarded else "",
            id=self.id,
            name=self._name,
            date=self._due_date.isoformat(),
            content=reprlib.repr(self._content))

    @property
    def name(self):
        self._check_not_discarded()
        return self._name

    @staticmethod
    def _validate_name(name):
        if len(name) < 1:
            raise ValueError("WorkItem name cannot be empty")
        return name

    @name.setter
    def name(self, value):
        self._check_not_discarded()

        event = Entity.AttributeChanged(originator_id=self.id,
                                        originator_version=self.version,
                                        name='_name',
                                        value=WorkItem._validate_name(value))
        self._apply(event)
        self._publish(event)

    @property
    def due_date(self):
        self._check_not_discarded()
        return self._due_date

    @due_date.setter
    def due_date(self, value):
        self._check_not_discarded()
        event = Entity.AttributeChanged(originator_id=self.id,
                                        originator_version=self.version,
                                        name='_due_date',
                                        value=value)
        self._apply(event)
        self._publish(event)

    @property
    def content(self):
        self._check_not_discarded()
        return self._content

    @content.setter
    def content(self, value):
        self._check_not_discarded()
        event = Entity.AttributeChanged(originator_id=self.id,
                                        originator_version=self.version,
                                        name='_content',
                                        value=value)
        self._apply(event)
        self._publish(event)

# ======================================================================================================================
# Factories - the aggregate root factory
#

def register_new_work_item(name, due_date=None, content=None, hub=None):
    work_item_id = uuid.uuid4().hex

    event = WorkItem.Created(originator_id=work_item_id,
                             originator_version=0,
                             name=name,
                             due_date=due_date,
                             content=content)

    work_item = _when(event, hub)
    work_item._publish(event)
    return work_item


# ======================================================================================================================
# Mutators - all aggregate creation and mutation is performed by the generic _when() function.
#

def mutate(obj, event):
    return _when(event, obj)

# These dispatch on the type of the first arg, hence (event, self)
@singledispatch
def _when(event, entity):
    """Modify an entity (usually an aggregate root) by replaying an event."""
    raise NotImplemented("No _when() implementation for {!r}".format(event))


@_when.register(Entity.AttributeChanged)
def _(event, entity):
    entity._validate_event_originator(event)
    setattr(entity, event.name, event.value)
    entity._increment_version()
    return entity


@_when.register(WorkItem.Created)
def _(event, hub):
    work_item = WorkItem(event, hub)
    work_item._increment_version()
    return work_item


@_when.register(WorkItem.Discarded)
def _(event, work_item):
    work_item._validate_event_originator(event)

    work_item._discarded = True
    work_item._increment_version()


# ======================================================================================================================
# Mutators - all aggregate creation and mutation is performed by the generic _when() function.
#

class Repository:
    __metaclass__ = ABCMeta

    def __init__(self, **kwargs):
        # noinspection PyArgumentList
        super().__init__(**kwargs)

    def all_work_items(self):
        return self.work_items_where(lambda work_item: True)

    def works_items_with_name(self, name):
        return self.work_items_where(lambda work_item: work_item.name == name)

    @abstractmethod
    def work_items_where(self, predicate):
        raise NotImplemented

    def work_items_with_id(self, id):
        try:
            return exactly_one(self.work_items_where(lambda board: board.id == id))
        except ValueError as e:
            raise ValueError("No WorkItem with id {}".format(id)) from e