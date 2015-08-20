from abc import ABCMeta, abstractmethod

import uuid

from singledispatch import singledispatch

from utility.itertools import exactly_one

from kanban.domain.exceptions import ConstraintError
from kanban.domain.model.events import DomainEvent, publish
from kanban.domain.model.entity import Entity, DiscardedEntityError


# ======================================================================================================================
# Aggregate root entity
#

class Board(Entity):
    """A Kanban board which can track the progress of work items through a step-wise process.
    """

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

    class WorkItemScheduled(DomainEvent):
        pass

    class WorkItemAbandoned(DomainEvent):
        pass

    class WorkItemAdvanced(DomainEvent):
        pass

    class WorkItemRetired(DomainEvent):
        pass

    def __init__(self, event):
        """Initialize a Board.

        Do NOT call directly. Use the start_project() factory method.
        """
        super().__init__(event.originator_id, event.originator_version)
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
        """The name of this board."""
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
        publish(event)

    @property
    def description(self):
        """A description of this board."""
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
        publish(event)

    def discard(self):
        """Discard this board.

        After a call to this method, the board can no longer be used.
        """
        self._check_not_discarded()
        event = Board.Discarded(originator_id=self.id,
                                originator_version=self.version)

        self._apply(event)
        publish(event)

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
        """Add a new column at the right side of the board representing a work process state.

        Args:
            name: The column name, which must be distinct from existing names on this board.
            wip_limit: The maximum number work items permitted in this column, or None for unlimited.

        Returns:
            The new column.

        Raises:
            ValueError: If the column name is not valid.
            ValueError: If the wip_limit is not valid.
        """
        self._check_not_discarded()
        event = Board.NewColumnAdded(originator_id=self.id,
                                     originator_version=self.version,
                                     column_id=uuid.uuid4().hex,
                                     column_version=0,
                                     column_name=self._validate_column_name(name),
                                     wip_limit=Board._validate_column_wip_limit(wip_limit))
        self._apply(event)
        publish(event)
        return self.column_with_name(name)

    def _validate_column(self, column):
        column._check_not_discarded()
        if column not in self._columns:
            raise ValueError("{!r} is not part of {!r}".format(column, self))
        return column

    def insert_new_column_before(self, succeeding_column, name, wip_limit):
        """Insert a new column to the left of an existing column.

        Args:
            succeeding_column: An existing column to the right of which the new column will be inserted.
            name: The column name, which must be distinct from existing names on this board.
            wip_limit: The maximum number work items permitted in this column, or None for unlimited.

        Returns:
            The new column.

        Raises:
            ValueError: If the succeeding column is not valid.
            ValueError: If the new column name is not valid.
            ValueError: If the wip_limit is not valid.
        """
        self._check_not_discarded()
        event = Board.NewColumnInserted(originator_id=self.id,
                                        originator_version=self.version,
                                        column_id=uuid.uuid4().hex,
                                        column_version=0,
                                        column_name=self._validate_column_name(name),
                                        wip_limit=Board._validate_column_wip_limit(wip_limit),
                                        succeeding_column_id=self._validate_column(succeeding_column).id)
        self._apply(event)
        publish(event)
        return self.column_with_name(name)

    def _validate_column_removable(self, column):
        column = self._validate_column(column)
        if column.number_of_work_items > 0:
            raise ConstraintError("Cannot remove non-empty {!r}".format(column))
        return column

    def remove_column_by_name(self, name):
        """Remove a named column.

        Args:
            name: The name of the column to remove.

        Raises:
            ValueError: If there is no column of that name.
            ConstraintError: If the column contains work items.
        """
        self._check_not_discarded()
        column = self.column_with_name(name)

        event = Board.ColumnRemoved(originator_id=self.id,
                                    originator_version=self.version,
                                    column_id=self._validate_column_removable(column).id)
        self._apply(event)
        publish(event)

    def remove_column(self, column):
        """Remove a column.

        Args:
            column: The column to remove.

        Raises:
            DiscardedEntityError: This this board or the column has been discarded.
            ConstraintError: If the column contains work items.
        """
        self._check_not_discarded()

        event = Board.ColumnRemoved(originator_id=self.id,
                                    originator_version=self.version,
                                    column_id=self._validate_column_removable(column).id)
        self._apply(event)
        publish(event)

    def column_names(self):
        """Obtain an iterator over the column names.
        """
        self._check_not_discarded()
        for column in self._columns:
            yield column.name

    def columns(self):
        """Obtain an iterator over the columns.
        """
        self._check_not_discarded()
        return iter(self._columns)

    def column_with_name(self, name):
        """Obtain a column by name.

        Args:
            name: The name of the column to return.

        Returns:
            The column with the specified name.

        Raises:
            DiscardedEntityError: If this board has been discarded.
            ValueError: If there is no column with the specified name.
        """
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
        """Enqueue a work item in the first column.

        Args:
            work_item: The WorkItem to be scheduled on this board.

        Raises:
            DiscardedEntityError: If this board or the work item has been discarded.
            ConstraintError: If this board has no columns.
            ConstraintError: If this work item has already been scheduled on this board.
            WorkLimitError: If the first column is at or above its work-in-progress limit.
        """
        self._check_not_discarded()

        if work_item.discarded:
            raise DiscardedEntityError("Cannot schedule {!r}".format(work_item))

        if len(self._columns) < 1:
            raise ConstraintError("Cannot schedule a {!r} to board with no columns: {!r}".format(work_item, self))

        if work_item in self:
            raise ConstraintError("{!r} is already scheduled".format(work_item))

        first_column = self._columns[0]

        if not first_column.can_accept_work_item():
            raise WorkLimitError("Cannot schedule a work item to {}, "
                                 "at or exceeding its work-in-progress limit".format(first_column))

        event = Board.WorkItemScheduled(originator_id=self.id,
                                        originator_version=self.version,
                                        work_item_id=work_item.id)
        self._apply(event)
        publish(event)

    def __contains__(self, work_item):
        for column in self._columns:
            if work_item in column:
                return True
        return False

    def abandon_work_item(self, work_item):
        """Abandon a work item.

        Args:
            work_item: The work item be to be abandoned.

        Raises:
            DiscardedEntityError: If this board or the work item has been discarded.
            ValueError: If the work item is not present on this board.
        """
        self._check_not_discarded()

        if work_item.discarded:
            raise DiscardedEntityError("Cannot abandon {!r}".format(work_item))

        column_index, priority = self._find_work_item_by_id(work_item.id)

        event = Board.WorkItemAbandoned(originator_id=self.id,
                                        originator_version=self.version,
                                        work_item_id=work_item.id,
                                        column_index=column_index,
                                        priority=priority)
        self._apply(event)
        publish(event)

    def _find_work_item_by_id(self, work_item_id):
        for column_index, column in enumerate(self._columns):
            try:
                priority = column._work_item_ids.index(work_item_id)
                return column_index, priority
            except ValueError:
                pass
        raise ValueError("Work Item with id={!r} is not on {!r}".format(work_item_id, self))

    def advance_work_item(self, work_item):
        """Advance a work item to the next column.

        Args:
            work_item: The work item be to advanced to the next column.

        Raises:
            ConstraintError: If the work_item is in the last column of the board.
            WorkLimitError: If the next column is at or has exceeded its work-in-progress limit.
        """
        self._check_not_discarded()

        if work_item.discarded:
            raise DiscardedEntityError("Cannot advance {!r}".format(work_item))

        source_column_index, priority = self._find_work_item_by_id(work_item.id)

        destination_column_index = source_column_index + 1
        if destination_column_index >= len(self._columns):
            raise ConstraintError("Cannot advance {!r} from last column of {!r}".format(work_item, self))

        if not self._columns[destination_column_index].can_accept_work_item():
            raise WorkLimitError("Cannot schedule a work item to {}, "
                                 "at or exceeding its work-in-progress limit".format(self._columns[destination_column_index]))

        event = Board.WorkItemAdvanced(originator_id=self.id,
                                       originator_version=self.version,
                                       work_item_id=work_item.id,
                                       source_column_index=source_column_index,
                                       priority=priority)
        self._apply(event)
        publish(event)

    def retire_work_item(self, work_item):
        """Retire a work item, removing it from the final column.

        Raises:
            DiscardedEntityError: If this board or the supplied work item have been discarded.
            ConstraintError: If this board has no columns.
            ConstraintError: If the specified work item is not in the last column.
        """
        self._check_not_discarded()

        if work_item.discarded:
            raise DiscardedEntityError("Cannot retire {!r}".format(work_item))

        try:
            priority = self._columns[-1]._work_item_ids.index(work_item.id)
        except IndexError:
            raise ConstraintError("Cannot retire a {!r} from a board with no columns".format(work_item))
        except ValueError:
            raise ConstraintError("{!r} not available for retiring from last column of {!r}".format(work_item, self))

        event = Board.WorkItemRetired(originator_id=self.id,
                                      originator_version=self.version,
                                      work_item_id=work_item.id,
                                      priority=priority)
        self._apply(event)
        publish(event)

    def _apply(self, event):
        mutate(self, event)


# ======================================================================================================================
# Entities
#

class Column(Entity):

    def __init__(self, event, board):
        "DO NOT CALL DIRECTLY"
        super().__init__(event.column_id, event.column_version)
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
        """Determine whether a particular work item is present in this column.
        """
        self._check_not_discarded()
        return work_item.id in self._work_item_ids

    @property
    def name(self):
        """The current name.

        Raises:
            DiscardedEntityError: When setting or getting, if this column has been discarded.
            ValueError: When setting, if the column name is set to an empty string.
            ValueError: When setting, if the column name is not distinct for the board.
        """
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
        publish(event)

    @property
    def wip_limit(self):
        """The current work-in-progress limit.

        The number of work items in this column must be lower than this limit before new work items
        can be scheduled to, or advanced to, this column. An unlimited column has a wip_limit of None.

        Raises:
            DiscardedEntityError: When setting or getting, if this column has been discarded.
            ValueError: When setting, if the value is neither None nor a non-negative integer.
        """
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
        publish(event)

    @property
    def number_of_work_items(self):
        """The amount of work in progress (read-only).

        The number of work items currently in this column.

        Raises:
            DiscardedEntityError: When getting, if this column has been discarded.
        """
        self._check_not_discarded()
        return len(self._work_item_ids)

    def can_accept_work_item(self):
        """Determine whether this column can accept an additional work item.

        Returns:
            True if the column can accept an additional work item, otherwise False.

        Raises:
            DiscardedEntityError: When getting, if this column has been discarded.
        """
        self._check_not_discarded()
        return (self.wip_limit is None) or (self.number_of_work_items < self.wip_limit)

    def work_item_ids(self):
        """Obtain an iterator over the identifiers of work items in this column.

        Returns:
            An iterator over a series of work item ids.

        Raises:
            DiscardedEntityError: If this column has been discarded.
        """
        self._check_not_discarded()
        return iter(self._work_item_ids)

    def _apply(self, event):
        mutate(self, event)


# ======================================================================================================================
# Factories - the aggregate root factory
#

def start_project(name, description):
    board_id = uuid.uuid4().hex

    event = Board.Created(originator_id=board_id,
                          originator_version=0,
                          name=Board._validate_name(name),
                          description=Board._validate_description(description))

    board = _when(event)
    publish(event)
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
    raise NotImplementedError("No _when() implementation for {!r}".format(event))


@_when.register(Entity.AttributeChanged)
def _(event, entity):
    entity._validate_event_originator(event)
    setattr(entity, event.name, event.value)
    entity._increment_version()
    return entity


@_when.register(Board.Created)
def _(event, obj=None):
    """Create a new aggregate root"""
    board = Board(event)
    board._increment_version()
    return board


@_when.register(Board.NewColumnAdded)
def _(event, board):
    board._validate_event_originator(event)
    column = Column(event, board)
    board._columns.append(column)
    board._increment_version()
    return board


@_when.register(Board.NewColumnInserted)
def _(event, board):
    board._validate_event_originator(event)
    index = board._column_index_with_id(event.succeeding_column_id)
    column = Column(event, board)
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


@_when.register(Board.WorkItemScheduled)
def _(event, board):
    board._validate_event_originator(event)
    column = board._columns[0]
    column._work_item_ids.append(event.work_item_id)
    column._increment_version()
    board._increment_version()
    return board


@_when.register(Board.WorkItemAbandoned)
def _(event, board):
    board._validate_event_originator(event)
    column = board._columns[event.column_index]
    designated_work_item_id = column._work_item_ids[event.priority]
    assert designated_work_item_id == event.work_item_id
    del column._work_item_ids[event.priority]
    column._increment_version()
    board._increment_version()
    return board


@_when.register(Board.WorkItemAdvanced)
def _(event, board):
    board._validate_event_originator(event)
    source_column = board._columns[event.source_column_index]
    designated_work_item_id = source_column._work_item_ids[event.priority]
    assert designated_work_item_id == event.work_item_id
    source_column._work_item_ids.remove(event.work_item_id)
    destination_column_index = event.source_column_index + 1
    destination_column = board._columns[destination_column_index]
    destination_column._work_item_ids.append(event.work_item_id)
    board._increment_version()
    source_column._increment_version()
    destination_column._increment_version()
    return board


@_when.register(Board.WorkItemRetired)
def _(event, board):
    board._validate_event_originator(event)
    last_column = board._columns[-1]
    designated_work_item_id = last_column._work_item_ids[event.priority]
    assert designated_work_item_id == event.work_item_id
    del last_column._work_item_ids[event.priority]
    last_column._increment_version()
    board._increment_version()
    return board


# ======================================================================================================================
# Repository - for retrieving existing aggregates
#

class Repository(metaclass=ABCMeta):

    def __init__(self, **kwargs):
        # noinspection PyArgumentList
        super().__init__(**kwargs)

    def all_boards(self, board_ids=None):
        return self.boards_where(lambda board: True, board_ids)

    def boards_with_name(self, name, board_ids=None):
        try:
            return self.boards_where(lambda board: board.name == name, board_ids)
        except ValueError as e:
            raise ValueError("No Board with name {}".format(name)) from e

    def board_with_id(self, board_id):
        try:
            return exactly_one(self.all_boards(board_ids=(board_id,)))
        except ValueError as e:
            raise ValueError("No Board with id {}".format(board_id)) from e

    @abstractmethod
    def boards_where(self, predicate, board_ids=None):
        raise NotImplementedError


# ======================================================================================================================
# Exceptions - for signalling errors
#

class WorkLimitError(ConstraintError):
    pass

