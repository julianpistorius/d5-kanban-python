from infrastructure.repository import reconstitute
from kanban.board import Repository


class BoardRepository(Repository):

    def __init__(self, event_store):
        self._event_store = event_store

    def _extant_board_ids(self):
        # Scan events to get a list of extant board ids
        board_ids = set()
        with self._event_store.open_event_stream() as events:
            for event in events:
                topic = event['topic']
                if topic.endswith('Board.Created'):
                    board_id = event['kwargs']['id']
                    if board_id in board_ids:
                        raise RuntimeError("Inconsistent event stream.")
                    board_ids.add(board_id)

                elif topic.endswith('Entity.Deleted'):
                    board_id = event['kwargs']['id']
                    board_ids.discard(board_id)
        return board_ids

    def _reconstitute_boards(self, board_ids):
        grouped_board_events = {board_id: [] for board_id in board_ids}
        with self._event_store.open_event_stream() as events:
            for event in events:
                id = event['kwargs']['id']
                if id in grouped_board_events:
                    grouped_board_events[id].append(event)
        all_boards = map(reconstitute, grouped_board_events.values())
        return all_boards

    def all_boards(self):
        board_ids = self._extant_board_ids()
        return self._reconstitute_boards(board_ids)

    def boards_where(self, predicate):
        return filter(predicate, self.all_boards())
