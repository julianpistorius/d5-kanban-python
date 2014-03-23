from abc import ABCMeta, abstractmethod
import uuid

from singledispatch import singledispatch

from kanban.events import hub as the_hub
from infrastructure.itertools import exactly_one
from kanban.domain.model.domain_events import DomainEvent
from kanban.domain.model.entity import Entity


class Board(Entity):

    class Created(Entity.Created):
        pass

    class Discarded(Entity.Discarded):
        pass

    class NewColumnInserted(DomainEvent):
        pass

    class NewColumnAdded(DomainEvent):
        pass

    def __init__(self, event, hub=None):
        super().__init__(event.originator_id, event.originator_version, hub)
        self._name = event.name
        self._description = event.description
        self._columns = []

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        if len(value) < 1:
            raise ValueError("Board name cannot be empty")
        event = Entity.AttributeChanged(originator_id=self.id,
                                        originator_version=self.version,
                                        name='_name',
                                        value=value)
        self._mutate(event)
        self._publish(event)

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, value):
        if len(value) < 1:
            raise ValueError("Board description cannot be empty")
        event = Entity.AttributeChanged(originator_id=self.id,
                                        originator_version=self.version,
                                        name='_description',
                                        value=value)
        self._mutate(event)
        self._publish(event)

    def discard(self):
        event = Board.Discarded(originator_id=self.id,
                                originator_version=self.version)

        self._mutate(event)
        self._publish(event)

    def add_new_column(self, name, wip_limit):
        event = Board.NewColumnAdded(originator_id=self.id,
                                     originator_version=self.version,
                                     column_id=uuid.uuid4().hex,
                                     column_version=0,
                                     name=name,
                                     wip_limit=wip_limit)
        self._mutate(event)
        self._publish(event)

    def insert_new_column_before(self, succeeding_column, name, wip_limit):
        event = Board.NewColumnInserted(originator_id=self.id,
                                        originator_version=self.version,
                                        column_id=uuid.uuid4().hex,
                                        column_version=0,
                                        column_name=name,
                                        wip_limit=wip_limit,
                                        succeeding_column_id=succeeding_column.id)
        self._mutate(event)
        self._publish(event)

    # TODO: Remove a column

    def _column_index_with_id(self, id):
        for index, column in enumerate(self._columns):
            if column.id == id:
                return index
        raise ValueError("No column with id {}".format(id))

    def _mutate(self, event):
        _when(event, self)

class Column(Entity):

    class Created(Entity.Created):
        pass

    def __init__(self, event, hub):
        "NOT PART OF API"
        super().__init__(event.column_id, event.column_version, hub)
        # TODO: Should a column have a reference to its parent Board?
        # TODO: Validation - and point out refactoring opportunity stymied by event design
        self._name = event.name
        self._wip_limit = event.wip_limit
        self._work_item_ids = []

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        if len(value) < 1:
            raise ValueError("Column name cannot be empty")

        event = Entity.AttributeChanged(originator_id=self.id,
                                        originator_version=self.version,
                                        name='_name',
                                        value=value)
        self._mutate(self, event)
        self._publish(self, event)


    @property
    def wip_limit(self):
        return self._wip_limit

    @wip_limit.setter
    def wip_limit(self, value):
        if value < len(self._work_item_ids):
            raise ValueError("Requested WIP limit ({}) cannot cannot be less than the "
                             "current amount of WIP ({})".format(value, len(self._work_item_ids)))

        event = Entity.AttributeChanged(originator_id=self.id,
                                        originator_version=self.version,
                                        name='_wip_limit',
                                        value=value)
        self._mutate(self, event)
        self._publish(self, event)

    def _mutate(self, event):
        _when(event, self)


# These dispatch on the type of the first arg, hence (event, self)
@singledispatch
def _when(event, obj):
    raise NotImplemented("No _when() implementation for {}".format(repr(event)))


@_when.register(Entity.AttributeChanged)
def _(event, entity):
    entity._validate_event_originator(event)
    setattr(entity, event.name, event.value)
    entity._increment_version()


@_when.register(Board.NewColumnAdded)
def _(event, board):
    board._validate_event_originator(event)

    column = Column(event, hub=board._hub)

    board._columns.append(column)
    board._increment_version()


@_when.register(Board.NewColumnInserted)
def _(event, board):
    board._validate_event_originator(event)

    index = board._column_index_with_id(event.succeeding_column_id)

    column = Column(event, hub=board._hub)

    board._columns.insert(index, column)
    board._increment_version()

@_when.register(Board.Discarded)
def _(event, board):
    board._validate_event_originator(event)
    board._discarded = True
    # TODO: Need to remove any child columns too
    # TODO: Should we set some sort of flag in the Entity base to prevent further use?
    # TODO: Remove any cached aggregates from the repository

def start_project(name, description):
    board_id = uuid.uuid4().hex

    event = Board.Created(originator_id=board_id,
                          originator_version=0,
                          name=name,
                          description=description)

    board = Board(event, hub=the_hub()) # Inject the hub!
    board._publish(event)
    return board


class Repository:
    __metaclass__ = ABCMeta

    def all_boards(self):
        return self.boards_where(lambda board: True)

    def board_with_id(self, id):
        try:
            return exactly_one(self.boards_where(lambda board: board.id == id))
        except ValueError as e:
            raise ValueError("No Board with id {}".format(id)) from e

    def board_with_name(self, name):
        try:
            return exactly_one(self.boards_where(lambda board: board.name == name))
        except ValueError as e:
            raise ValueError("No Board with name {}".format(name)) from e

    @abstractmethod
    def boards_where(self, predicate):
        raise NotImplemented
