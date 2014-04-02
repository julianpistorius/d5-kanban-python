from infrastructure.event_processing import EventPlayer, extant_entity_ids
from kanban.domain.model import board


class BoardRepository(board.Repository, EventPlayer):
    """Concrete repository for Boards in terms of an event store.
    """

    def __init__(self, event_store, **kwargs):
        """Create a new BoardRepository.

        Args:
            event_store: An EventStore instance from which boards can be reconstituted.

        """
        super().__init__(event_store=event_store,
                         mutator=board.mutate,
                         **kwargs)

    def all_boards(self, board_ids=None):
        """Obtain all Boards.

        Args:
            board_ids: An optional list of work item ids used to restrict the results.
                If not provided, all work items will be returned.

        Returns:
            An iterable series of Boards.
        """
        if board_ids is None:
            board_ids = extant_entity_ids(self._event_store, entity_class_name='Board')
        return self._replay_events(board_ids)

    def boards_where(self, predicate, board_ids=None):
        """Obtain all Boards which match a predicate.

        Args:
            predicate: A single argument function used to identify the boards to be returned.

            board_ids: An optional list of work item ids used to restrict the results.
                If not provided, all boards which match the predicate will be returned.

        Returns:
            An iterable series of Boards.
        """
        if board_ids is None:
            board_ids = extant_entity_ids(self._event_store, entity_class_name='Board')
        boards = self._replay_events(board_ids)
        return filter(predicate, boards)
