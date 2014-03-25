from infrastructure.event_sourced_repos.stored_event_repository import StoredEventRepository
from kanban.domain.model import metrics

class LeadTimeProjection(metrics.LeadTimeProjection, StoredEventRepository):

    def __init__(self, board_id, event_store, hub, **kwargs):
        super().__init__(board_id=board_id,
                         hub=hub,
                         event_store=event_store,
                         entity_class_name=None, # TODO: Smell!
                         mutator=metrics.mutate,
                         stream_primer=self,
                         **kwargs)

        list(self._reconstitute_entities([self._board_id]))
        pass
