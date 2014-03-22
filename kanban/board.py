from abc import ABCMeta, abstractmethod
import uuid

from .events import hub
from infrastructure.itertools import exactly_one
from kanban.entity import Entity


class Board(Entity):

    class Created(Entity.Created):
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




def start_project(name, description):
    board_id = uuid.uuid4().hex
    return Board(id=board_id, version=0, name=name, description=description, hub=hub())


class Repository:
    __metaclass__ = ABCMeta

    def all_boards(self):
        return self.boards_where(lambda board: True)

    def board_with_id(self, id):
        return exactly_one(self.boards_where(lambda board: board.id == id))

    def board_with_name(self, name):
        return exactly_one(self.boards_where(lambda board: board.name == name))

    @abstractmethod
    def boards_where(self, predicate):
        raise NotImplemented
