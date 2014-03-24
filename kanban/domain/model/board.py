from abc import ABCMeta, abstractmethod
from functools import reduce
import uuid

from singledispatch import singledispatch

from utility.utilities import exactly_one
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

    class ColumnRemoved(DomainEvent):
        pass

    def __init__(self, event, hub=None):
        super().__init__(event.originator_id, event.originator_version, hub)
        self._name = event.name
        self._description = event.description
        self._columns = []
        self._increment_version()  # Required at the end of all mutators - this is no exception

    def __repr__(self):
        return "{d}Board(id={b._id}, name={b._name!r}, description={b._description!r}, columns=[0..{n}])".format(
            d="*Discarded* " if self._discarded else "",
            b=self,
            n=len(self._columns))

    @property
    def name(self):
        self._check_not_discarded()
        return self._name

    @name.setter
    def name(self, value):
        self._check_not_discarded()
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
        self._check_not_discarded()
        return self._description

    @description.setter
    def description(self, value):
        self._check_not_discarded()
        if len(value) < 1:
            raise ValueError("Board description cannot be empty")
        event = Entity.AttributeChanged(originator_id=self.id,
                                        originator_version=self.version,
                                        name='_description',
                                        value=value)
        self._mutate(event)
        self._publish(event)

    def discard(self):
        self._check_not_discarded()
        event = Board.Discarded(originator_id=self.id,
                                originator_version=self.version)

        self._mutate(event)
        self._publish(event)

    def add_new_column(self, name, wip_limit):
        self._check_not_discarded()
        event = Board.NewColumnAdded(originator_id=self.id,
                                     originator_version=self.version,
                                     column_id=uuid.uuid4().hex,
                                     column_version=0,
                                     name=name,
                                     wip_limit=wip_limit)
        self._mutate(event)
        self._publish(event)
        return self.column_with_name(name) # Convenience?

    def insert_new_column_before(self, succeeding_column, name, wip_limit):
        self._check_not_discarded()
        event = Board.NewColumnInserted(originator_id=self.id,
                                        originator_version=self.version,
                                        column_id=uuid.uuid4().hex,
                                        column_version=0,
                                        column_name=name,
                                        wip_limit=wip_limit,
                                        succeeding_column_id=succeeding_column.id)
        self._mutate(event)
        self._publish(event)
        return self.column_with_name(name) # Convenience?

    def remove_column_by_name(self, name):
        self._check_not_discarded()
        column_id = self._column_id_with_name(name)

        event = Board.ColumnRemoved(originator_id=self.id,
                                    originator_version=self.version,
                                    column_id=column_id)
        self._mutate(event)
        self._publish(event)

    def remove_column(self, column):
        self._check_not_discarded()

        event = Board.ColumnRemoved(originator_id=self.id,
                                    originator_version=self.version,
                                    column_id=column.id)
        self._mutate(event)
        self._publish(event)


    def column_names(self):
        self._check_not_discarded()
        for column in self._columns:
            yield column.name

    def columns(self):
        self._check_not_discarded()
        return iter(self._columns)

    def column_with_name(self, name):
        self._check_not_discarded()
        for column in self._columns:
            if column.name == name:
                 return column
        raise ValueError("No column with name '{}'".format(name))

    def _column_id_with_name(self, name):
        return self.column_with_name(name).id

    def _column_index_with_id(self, id):
        for index, column in enumerate(self._columns):
            if column.id == id:
                return index
        raise ValueError("No column with id '{}'".format(id))

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
        self._increment_version()  # Required at the end of all mutators - this is no exception

    def __repr__(self):
        return "{d}Column(id={c._id}, name={c._name!r}, wip_limit={c._wip_limit}, work_items=[0..{n}])".format(
            d="*Discarded* " if self._discarded else "",
            c=self,
            n=len(self._work_item_ids))

    @property
    def name(self):
        self._check_not_discarded()
        return self._name

    @name.setter
    def name(self, value):
        self._check_not_discarded()
        if len(value) < 1:
            raise ValueError("Column name cannot be empty")

        event = Entity.AttributeChanged(originator_id=self.id,
                                        originator_version=self.version,
                                        name='_name',
                                        value=value)
        self._mutate(event)
        self._publish(event)


    @property
    def wip_limit(self):
        self._check_not_discarded()
        return self._wip_limit

    @wip_limit.setter
    def wip_limit(self, value):
        self._check_not_discarded()
        if value < len(self._work_item_ids):
            raise ValueError("Requested WIP limit ({}) cannot cannot be less than the "
                             "current amount of WIP ({})".format(value, len(self._work_item_ids)))

        event = Entity.AttributeChanged(originator_id=self.id,
                                        originator_version=self.version,
                                        name='_wip_limit',
                                        value=value)
        self._mutate(event)
        self._publish(event)

    def number_of_work_items(self):
        self._check_not_discarded()
        return len(self._work_item_ids)

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

    if event.name in (column.name for column in board._columns):
        raise ValueError("Column name {} is not distinct from existing column names".format(event.name))

    column = Column(event, hub=board._hub)

    board._columns.append(column)
    board._increment_version()


@_when.register(Board.NewColumnInserted)
def _(event, board):
    board._validate_event_originator(event)

    if event.name in (column.name for column in board._columns):
        raise ValueError("Column name {} is not distinct from existing column names".format(event.name))

    index = board._column_index_with_id(event.succeeding_column_id)

    column = Column(event, hub=board._hub)

    board._columns.insert(index, column)
    board._increment_version()


@_when.register(Board.ColumnRemoved)
def _(event, board):
    board._validate_event_originator(event)

    index = board._column_index_with_id(event.column_id)
    column = board._columns[index]
    if column.number_of_work_items() > 0:
        raise RuntimeError("Cannot remove non-empty {}".repr(column))
    column._discarded = True
    del board._columns[index]
    board._increment_version()


@_when.register(Board.Discarded)
def _(event, board):
    board._validate_event_originator(event)

    for column in board._columns:
        column._discarded = True
    board._columns.clear()

    board._discarded = True
    board._increment_version()


def start_project(name, description, hub=None):
    board_id = uuid.uuid4().hex

    event = Board.Created(originator_id=board_id,
                          originator_version=0,
                          name=name,
                          description=description)

    board = Board(event, hub=hub)
    board._publish(event)
    return board


class Repository:
    __metaclass__ = ABCMeta

    def __init__(self, hub):
        self._hub = hub

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

    def _apply_event(self, board, event):
        if isinstance(event, Board.Created):
            if board is not None:
                raise RuntimeError("Inconsistent event stream.")
            return Board(event, self._hub)
        _when(event, board)
        return board

    def _apply_events(self, event_stream):
        """Current State is the left fold over previous behaviours - Greg Young"""
        return reduce(self._apply_event, event_stream, None)