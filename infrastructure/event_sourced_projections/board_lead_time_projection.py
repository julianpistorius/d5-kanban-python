from infrastructure.event_processing import EventPlayer
from kanban.domain.model import metrics
from utility.utilities import consume


class LeadTimeProjection(metrics.LeadTimeProjection, EventPlayer):
    """

    """

    def __init__(self, board_id, event_store, hub, **kwargs):
        super().__init__(board_id=board_id,
                         hub=hub,
                         event_store=event_store,
                         mutator=metrics.mutate,
                         stream_primer=self,
                         **kwargs)

        consume(self._replay_events([self._board_id]))
