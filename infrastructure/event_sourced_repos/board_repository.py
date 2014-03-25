from infrastructure.event_sourced_repos.stored_event_repository import StoredEventRepository
from kanban.domain.model import board


class BoardRepository(board.Repository, StoredEventRepository):

    def __init__(self, event_store, hub, **kwargs):
        """
        Args:
            event_store:
            hub:
        """
        super().__init__(event_store=event_store,
                         entity_class_name='Board',
                         mutator=board.mutate,
                         stream_primer=hub,
                         **kwargs)

    def all_boards(self):
        board_ids = self._extant_entity_ids()
        return self._reconstitute_entities(board_ids)

    def boards_where(self, predicate):
        return filter(predicate, self.all_boards())

