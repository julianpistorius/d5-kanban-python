from abc import ABCMeta, abstractmethod
import uuid

from kanban.events import hub as the_hub
from infrastructure.itertools import exactly_one
from kanban.domain.model.domain_events import DomainEvent
from kanban.domain.model.entity import Entity


class Board(Entity):

    class Created(Entity.Created):
        pass

    class NewColumnInserted(DomainEvent):
        pass

    class NewColumnAdded(DomainEvent):
        pass

    def __init__(self, id, version, name, description, hub=None):
        super().__init__(id, version, hub)
        self._name = name
        self._description = description
        self._columns = []
        self._publish(Board.Created,
                      id=self.id,
                      version=self.version,
                      name=name,
                      description=self._description)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        if len(value) < 1:
            raise ValueError("Board name cannot be empty")
        self._name = value
        self._attribute_changed(Board.name)

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, value):
        if len(value) < 1:
            raise ValueError("Board description cannot be empty")
        self._description = value
        self._attribute_changed(Board.description)

    def insert_new_column_before(self, succeeding_column, name, wip_limit):
        index = self._columns.index(succeeding_column)
        column = Column(id=uuid.uuid4().hex,
                        version=0,
                        name=name,
                        wip_limit=wip_limit,
                        hub=self._hub)
        self._columns.insert(index, column)
        self._publish(Board.NewColumnInserted,
                      id=self.id,
                      new_column_id=column.id,
                      succeeding_column_id=succeeding_column.id)

    def add_new_column(self, name, wip_limit):
        column = Column(id=uuid.uuid4().hex,
                        version=0,
                        name=name,
                        wip_limit=wip_limit,
                        hub=self._hub)
        self._columns.append(column)
        self._publish(Board.NewColumnAdded,
                      id=self.id,
                      new_column_id=column.id)

    # TODO: Removing a board implies removing it's columns




class Column(Entity):

    class Created(Entity.Created):
        pass

    def __init__(self, id, version, name, wip_limit, hub):
        super().__init__(id, version, hub)
        # TODO: Validation - and point out refactoring opportunity stymied by event design
        self._name = name
        self._wip_limit = wip_limit
        self._work_item_ids = []
        self._publish(Column.Created,
                      id=self.id,
                      version=self.version,
                      name=name,
                      wip_limit=wip_limit)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        if len(value) < 1:
            raise ValueError("Column name cannot be empty")
        self._name = value
        self._attribute_changed(Column.name)

    @property
    def wip_limit(self):
        return self._wip_limit

    @wip_limit.setter
    def wip_limit(self, value):
        if value < len(self._work_item_ids):
            raise ValueError("Requested WIP limit ({}) cannot cannot be less than the "
                             "current amount of WIP ({})".format(value, len(self._work_item_ids)))
        self._wip_limit = value
        self._attribute_changed(Column.wip_limit)



def start_project(name, description):
    board_id = uuid.uuid4().hex
    return Board(id=board_id, version=0, name=name, description=description, hub=the_hub()) # Inject the hub!


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
