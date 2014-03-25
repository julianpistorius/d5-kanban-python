from abc import ABCMeta, abstractmethod

import uuid

from singledispatch import singledispatch

from utility.utilities import exactly_one
from kanban.domain.model.domain_events import DomainEvent
from kanban.domain.model.entity import Entity


# ======================================================================================================================
# Aggregate root entity
#

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

    class ScheduleWorkItem(DomainEvent):
        pass

    class AbandonWorkItem(DomainEvent):
        pass

    def __init__(self, event, hub=None):
        """DO NOT CALL DIRECTLY.
        """
        super().__init__(event.originator_id, event.originator_version, hub)
        # Validation not necessary here - never called directly
        self._name = event.name
        self._description = event.description
        self._columns = []

    def __repr__(self):
        return "{d}Board(id={b._id}, name={b._name!r}, description={b._description!r}, columns=[0..{n}])".format(
            d="*Discarded* " if self._discarded else "",
            b=self,
            n=len(self._columns))

    @property
    def name(self):
        self._check_not_discarded()
        return self._name

    @staticmethod
    def _validate_name(name):
        if len(name) < 1:
            raise ValueError("Board name cannot be empty")
        return name

    @name.setter
    def name(self, value):
        self._check_not_discarded()

        event = Entity.AttributeChanged(originator_id=self.id,
                                        originator_version=self.version,
                                        name='_name',
                                        value=Board._validate_name(value))
        self._apply(event)
        self._publish(event)

    @property
    def description(self):
        self._check_not_discarded()
        return self._description

    @staticmethod
    def _validate_description(description):
        if len(description) < 1:
            raise ValueError("Board description cannot be empty")
        return description

    @description.setter
    def description(self, value):
        self._check_not_discarded()

        event = Entity.AttributeChanged(originator_id=self.id,
                                        originator_version=self.version,
                                        name='_description',
                                        value=Board._validate_description(value))
        self._apply(event)
        self._publish(event)

    def discard(self):
        self._check_not_discarded()
        event = Board.Discarded(originator_id=self.id,
                                originator_version=self.version)

        self._apply(event)
        self._publish(event)

    def _validate_column_name(self, name):
        if len(name) < 1:
            raise ValueError("Column name cannot be empty")
        if name in {column.name for column in self._columns}:
            raise ValueError("Column name {!r} is not distinct from existing column "
                             "names in {!r}".format(name, self))
        return name

    @staticmethod
    def _validate_column_wip_limit(wip_limit):
        if wip_limit is not None and wip_limit < 0:
            raise ValueError("Work-in-progress limit {!r} is neither None (no limit) nor non-negative".format(wip_limit))
        return wip_limit

    def add_new_column(self, name, wip_limit):
        self._check_not_discarded()
        event = Board.NewColumnAdded(originator_id=self.id,
                                     originator_version=self.version,
                                     column_id=uuid.uuid4().hex,
                                     column_version=0,
                                     column_name=self._validate_column_name(name),
                                     wip_limit=Board._validate_column_wip_limit(wip_limit))
        self._apply(event)
        self._publish(event)
        return self.column_with_name(name)

    def insert_new_column_before(self, succeeding_column, name, wip_limit):
        self._check_not_discarded()
        event = Board.NewColumnInserted(originator_id=self.id,
                                        originator_version=self.version,
                                        column_id=uuid.uuid4().hex,
                                        column_version=0,
                                        column_name=self._validate_column_name(name),
                                        wip_limit=Board._validate_column_wip_limit(wip_limit),
                                        succeeding_column_id=succeeding_column.id)
        self._apply(event)
        self._publish(event)
        return self.column_with_name(name)

    def remove_column_by_name(self, name):
        self._check_not_discarded()
        column = self.column_with_name(name)
        self._check_column_removable(column)

        event = Board.ColumnRemoved(originator_id=self.id,
                                    originator_version=self.version,
                                    column_id=column.id)
        self._apply(event)
        self._publish(event)

    def _check_column_removable(self, column):
        if column.number_of_work_items() > 0:
            raise RuntimeError("Cannot remove non-empty {!r}".format(column))

    def remove_column(self, column):
        self._check_not_discarded()
        self._check_column_removable(column)

        event = Board.ColumnRemoved(originator_id=self.id,
                                    originator_version=self.version,
                                    column_id=column.id)
        self._apply(event)
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

    def _column_index_with_id(self, id):
        for index, column in enumerate(self._columns):
            if column.id == id:
                return index
        raise ValueError("No column with id '{}'".format(id))

    def schedule_work_item(self, work_item):
        """Enqueue a work item in the first column"""
        self._check_not_discarded()

        if work_item.discarded:
            raise RuntimeError("Cannot schedule {!r}".format(work_item))

        if len(self._columns) < 1:
            raise RuntimeError("Cannot schedule a {!r} to board with no columns: {!r}".format(work_item, self))

        if work_item in self:
            raise RuntimeError("{!r} is already scheduled".format(work_item))

        if not self._columns[0].check_can_accept_work_item():
            raise RuntimeError("Cannot schedule a work item to {}, "
                               "at or exceeding its work-in-progress limit".format(self._columns[0]))

        event = Board.ScheduleWorkItem(originator_id=self.id,
                                       originator_version=self.version,
                                       work_item_id=work_item.id)
        self._apply(event)
        self._publish(event)

    def __contains__(self, work_item):
        for column in self._columns:
            if work_item in column:
                return True
        return False

    def abandon_work_item(self, work_item):
        """Abandon a work item"""
        self._check_not_discarded()

        if work_item.discarded:
            raise RuntimeError("Cannot abandon {!r}".format(work_item))

        column_index, priority = self._find_work_item_by_id(work_item.id)

        event = Board.AbandonWorkItem(originator_id=self.id,
                                      originator_version=self.version,
                                      work_item_id=work_item.id,
                                      column_index=column_index,
                                      priority=priority)
        self._apply(event)
        self._publish(event)

    def _find_work_item_by_id(self, work_item_id):
        for column_index, column in enumerate(self._columns):
            try:
                priority = column._work_item_ids.index(work_item_id)
                return column_index, priority
            except ValueError:
                pass
        raise ValueError("Work Item with id={!r} is not on {!r}".format(work_item_id, self))



    def _apply(self, event):
        mutate(self, event)


# ======================================================================================================================
# Entities
#

class Column(Entity):

    def __init__(self, event, board, hub):
        "DO NOT CALL DIRECTLY"
        super().__init__(event.column_id, event.column_version, hub)
        # Validation not necessary here - never called directly
        self._board = board
        self._name = event.column_name
        self._wip_limit = event.wip_limit
        self._work_item_ids = []

    def __repr__(self):
        return ("{d}Column(id={c._id}, board_id={c._board.id!r} name={c._name!r}, "
                "wip_limit={c._wip_limit}, work_items=[0..{n}])".format(
                d="*Discarded* " if self._discarded else "",
                c=self,
                n=len(self._work_item_ids)))

    def __contains__(self, work_item):
        return work_item.id in self._work_item_ids

    @property
    def name(self):
        self._check_not_discarded()
        return self._name

    @name.setter
    def name(self, value):
        self._check_not_discarded()

        event = Entity.AttributeChanged(originator_id=self.id,
                                        originator_version=self.version,
                                        name='_name',
                                        value=self._board._validate_column_name(value))
        self._apply(event)
        self._publish(event)


    @property
    def wip_limit(self):
        self._check_not_discarded()
        return self._wip_limit


    @wip_limit.setter
    def wip_limit(self, value):
        self._check_not_discarded()

        event = Entity.AttributeChanged(originator_id=self.id,
                                        originator_version=self.version,
                                        name='_wip_limit',
                                        value=Board._validate_column_wip_limit(value))
        self._apply(event)
        self._publish(event)

    @property
    def number_of_work_items(self):
        self._check_not_discarded()
        return len(self._work_item_ids)

    def check_can_accept_work_item(self):
        return (self.wip_limit is not None) and (self.number_of_work_items < self.wip_limit)

    def _apply(self, event):
        mutate(self, event)

    def work_item_ids(self):
        return iter(self._work_item_ids)


# ======================================================================================================================
# Factories - the aggregate root factory
#

def start_project(name, description, hub=None):
    board_id = uuid.uuid4().hex

    event = Board.Created(originator_id=board_id,
                          originator_version=0,
                          name=Board._validate_name(name),
                          description=Board._validate_description(description))

    board = _when(event, hub)
    board._publish(event)
    return board


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


@_when.register(Board.Created)
def _(event, hub):
    """Create a new aggregate root"""
    board = Board(event, hub)
    board._increment_version()
    return board


@_when.register(Board.NewColumnAdded)
def _(event, board):
    board._validate_event_originator(event)

    column = Column(event, board, hub=board._hub)

    board._columns.append(column)
    board._increment_version()
    return board


@_when.register(Board.NewColumnInserted)
def _(event, board):
    board._validate_event_originator(event)
    index = board._column_index_with_id(event.succeeding_column_id)

    column = Column(event, board, hub=board._hub)

    board._columns.insert(index, column)
    board._increment_version()
    return board


@_when.register(Board.ColumnRemoved)
def _(event, board):
    board._validate_event_originator(event)

    index = board._column_index_with_id(event.column_id)
    column = board._columns[index]
    column._discarded = True
    del board._columns[index]
    board._increment_version()
    return board


@_when.register(Board.Discarded)
def _(event, board):
    board._validate_event_originator(event)

    for column in board._columns:
        column._discarded = True
    board._columns.clear()

    board._discarded = True
    board._increment_version()
    return board


@_when.register(Board.ScheduleWorkItem)
def _(event, board):
    board._validate_event_originator(event)
    column = board._columns[0]
    column._work_item_ids.append(event.work_item_id)
    column._increment_version()
    board._increment_version()
    return board


@_when.register(Board.AbandonWorkItem)
def _(event, board):
    board._validate_event_originator(event)
    column = board._columns[event.column_index]
    designated_work_item_id = column._work_item_ids[event.priority]
    assert designated_work_item_id == event.work_item_id
    del column._work_item_ids[event.priority]
    column._increment_version()
    board._increment_version()
    return board


# ======================================================================================================================
# Repository - for retrieving existing aggregates
#

class Repository:
    __metaclass__ = ABCMeta

    def __init__(self, **kwargs):
        # noinspection PyArgumentList
        super().__init__(**kwargs)

    def all_boards(self):
        return self.boards_where(lambda board: True)

    def board_with_name(self, name):
        try:
            return exactly_one(self.boards_where(lambda board: board.name == name))
        except ValueError as e:
            raise ValueError("No Board with name {}".format(name)) from e

    @abstractmethod
    def boards_where(self, predicate):
        raise NotImplemented

    def board_with_id(self, id):
        try:
            return exactly_one(self.boards_where(lambda board: board.id == id))
        except ValueError as e:
            raise ValueError("No Board with id {}".format(id)) from e



