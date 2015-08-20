"""Projections for tracking project management metrics such as lead time."""

from abc import ABCMeta, abstractmethod
import datetime

from singledispatch import singledispatch
from kanban.domain.exceptions import ConsistencyError

from kanban.domain.model.board import Board
from kanban.domain.model.events import subscribe, unsubscribe


class LeadTimeProjection(metaclass=ABCMeta):
    """A projection which tracks the lead time for work items with respect to a
    specified Board.
    """

    def __init__(self, board_id, **kwargs):
        """Create a new LeadTimeProjection.

        Args:
            board_id: The id of the board for which to report lead times.
        """
        self._board_id = board_id
        # noinspection PyArgumentList
        super().__init__(**kwargs)
        self._work_item_start_times = {}
        self._lead_times = {}

        self._load_events()
        subscribe(self._event_filter, self._handler)

    @property
    def board_id(self):
        """The id of the Board this projection is tracking."""
        return self._board_id

    @property
    def average_lead_time(self):
        """The average lead time."""
        mean_lead_time = sum(self._lead_times.values()) / len(self._lead_times)
        return datetime.timedelta(seconds=mean_lead_time)

    def lead_times(self):
        """A dynamic view onto collection of lead times."""
        return self._lead_times.values()

    def close(self):
        """No longer keep this projection up-to-date."""
        unsubscribe(self._event_filter, self._handler)

    @abstractmethod
    def _load_events(self):
        """Initalize the projection with historical events."""
        raise NotImplementedError

    def _event_filter(self, event):
        """A predicate to identify events in which this projection is interested."""
        return (event.originator_id == self.board_id
                and isinstance(event, (Board.WorkItemScheduled,
                                       Board.WorkItemAbandoned,
                                       Board.WorkItemRetired)))

    def _handler(self, event):
        """The event handler which when triggered updates the projection state."""
        mutate(self, event)


# ======================================================================================================================
# Mutators - all projection mutation is performed by the generic _when() function.
#

def mutate(obj, event):
    return _when(event, obj)

@singledispatch
def _when(event, projection):
    return projection


@_when.register(Board.WorkItemScheduled)
def _(event, projection):
    if event.work_item_id in projection._work_item_start_times:
        raise ConsistencyError("Inconsistent event stream: Duplicate WorkItem scheduled "
                               "with id {}".format(event.work_item_id))
    projection._work_item_start_times[event.work_item_id] = event.timestamp
    return projection


@_when.register(Board.WorkItemRetired)
def _(event, projection):
    if event.work_item_id not in projection._work_item_start_times:
        raise ConsistencyError("Inconsistent event stream: Retiring non-existent WorkItem "
                               "with id {}".format(event.work_item_id))
    lead_time = event.timestamp - projection._work_item_start_times[event.work_item_id]
    projection._lead_times[event.work_item_id] = lead_time
    del projection._work_item_start_times[event.work_item_id]
    return projection


@_when.register(Board.WorkItemAbandoned)
def _(event, projection):
    if event.work_item_id not in projection._work_item_start_times:
        raise ConsistencyError("Inconsistent event stream: Abandoning non-existent {} "
                               "for id {}".format(event.work_item_id))
    del projection._work_item_start_times[event.work_item_id]
    return projection
