from infrastructure.event_processing import EventPlayer
from kanban.domain.model import lead_time
from utility.utilities import consume


class LeadTimeProjection(lead_time.LeadTimeProjection, EventPlayer):
    """

    """

    def __init__(self, board_id, event_store, hub, **kwargs):
        super().__init__(board_id=board_id,
                         hub=hub,
                         event_store=event_store,
                         mutator=lead_time.mutate,
                         stream_primer=self,
                         **kwargs)

    def _load_events(self):
        """Initialize the projection with historical events."""
        consume(self._replay_events([self._board_id]))
