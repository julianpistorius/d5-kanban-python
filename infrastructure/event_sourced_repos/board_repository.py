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
            board_ids = extant_entity_ids(
                event_store=self._event_store,
                entity_class_name='Board')
        return self._replay_events(board_ids)

    def boards_where(self, predicate, board_ids=None):
        """Obtain Board instances.

        Retrieve Board instances which satisfy a predicate function. The
        series of Boards to be tested against the predicate can be further
        constrained by an optional series of board_ids.

        Args:
            predicate: A unary callable against which candidate Boards will be
                tested. Only those Boards for which the function returns True
                will be in the result collection.

            board_ids: An optional iterable series of Board ids. If
                not None, only those Boards whose ids are in this series will
                be in the result collection.

        Returns:
            An iterable series of Boards.
        """
        if board_ids is None:
            board_ids = extant_entity_ids(
                event_store=self._event_store,
                entity_class_name='Board')
        boards = self._replay_events(board_ids)
        return filter(predicate, boards)
