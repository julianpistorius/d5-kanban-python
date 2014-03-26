from infrastructure.event_processing import EventPlayer, extant_entity_ids
from kanban.domain.model import board


class BoardRepository(board.Repository, EventPlayer):

    def __init__(self, event_store, hub, **kwargs):
        """
        Args:
            event_store:
            hub:
        """
        super().__init__(event_store=event_store,
                         mutator=board.mutate,
                         stream_primer=hub,
                         **kwargs)

    def all_boards(self, board_ids=None):
        if board_ids is None:
            board_ids = extant_entity_ids(self._event_store, entity_class_name='Board')
        return self._replay_events(board_ids)

    def boards_where(self, predicate, board_ids=None):
        if board_ids is None:
            board_ids = extant_entity_ids(self._event_store, entity_class_name='Board')
        boards = self._replay_events(board_ids)
        return filter(predicate, boards)
