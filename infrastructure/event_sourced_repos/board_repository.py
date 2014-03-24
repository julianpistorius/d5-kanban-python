#from infrastructure.repository import reconstitute
import importlib
from utility.lrudict import LRUDict
from utility.utilities import resolve_attr
from kanban.domain.model.board import Repository


class BoardRepository(Repository):

    def __init__(self, event_store, hub=None):
        super().__init__(hub)
        self._event_store = event_store
        self._cache = LRUDict(capacity=1000)

    def _extant_board_ids(self):
        # Scan events to get a list of extant board ids
        board_ids = set()
        with self._event_store.open_event_stream() as events:
            for event in events:
                topic = event['topic']
                if topic.endswith('Board.Created'):
                    board_id = event['attributes']['originator_id']
                    if board_id in board_ids:
                        raise RuntimeError("Inconsistent event stream: Duplicate Board creation for id {}".format(board_id))
                    board_ids.add(board_id)

                elif topic.endswith('Board.Discarded'):
                    board_id = event['attributes']['originator_id']
                    if board_id not in board_ids:
                        raise RuntimeError("Inconsistent event stream: Discarding non-existent Board for id {}".format(board_id))
                    board_ids.discard(board_id)
        return board_ids

    def _reconstitute_boards(self, board_ids):
        grouped_board_events = {board_id: [] for board_id in board_ids}
        with self._event_store.open_event_stream() as events:
            for event in events:
                id = event['attributes']['originator_id']
                if id in grouped_board_events:
                    grouped_board_events[id].append(event)
        all_boards = map(self._cached_reconstitute, grouped_board_events.values())
        return all_boards

    def all_boards(self):
        board_ids = self._extant_board_ids()
        return self._reconstitute_boards(board_ids)

    def boards_where(self, predicate):
        return filter(predicate, self.all_boards())

    def _reconstitute(self, stored_event_sequence):
        deserialized_events = map(deserialize_event, stored_event_sequence)
        board = self._apply_events(deserialized_events)
        return board

    # TODO: Turn this into a decorator
    def _cached_reconstitute(self, stored_event_sequence):
        # If the  last event is the same as the cached entity,
        # return the cached entity
        most_recent_event = stored_event_sequence[-1]
        id = most_recent_event['attributes']['originator_id']
        version = most_recent_event['attributes']['originator_version']
        if id in self._cache:
            if self._cache[id].version == version + 1:
                return self._cache[id]
            del self._cache[id]

        board = self._reconstitute(stored_event_sequence)

        self._cache[board.id] = board

        return board


# TODO: Find a better place for this function
def deserialize_event(stored_event):
    """Recreate an event object"""
    topic = stored_event['topic']
    module_name, _, class_name = topic.partition('#')
    module = importlib.import_module(module_name)
    cls = resolve_attr(module, class_name)
    attributes = stored_event['attributes']
    event = cls(**attributes)
    return event
