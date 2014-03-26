from abc import ABCMeta
import datetime

from singledispatch import singledispatch

from kanban.domain.model.board import Board


class LeadTimeProjection:

    __metaclass__ = ABCMeta

    def __init__(self, board_id, hub, **kwargs):
        self._board_id = board_id
        self._hub = hub
        # noinspection PyArgumentList
        super().__init__(**kwargs)
        self._work_item_start_times = {}
        self._lead_times = {}

        self._hub.subscribe(self.event_filter, self.handler)

    @property
    def board_id(self):
        return self._board_id

    @property
    def lead_time(self):
        "A timedelta object representing the lead time for work items"
        mean_lead_time = sum(self._lead_times.values()) / len(self._lead_times)
        return datetime.timedelta(seconds=mean_lead_time)

    def event_filter(self, event):
        return (event.originator_id == self.board_id
                and isinstance(event, (Board.WorkItemScheduled,
                                       Board.WorkItemAbandoned,
                                       Board.WorkItemRetired)))

    def handler(self, event):
        mutate(self, event)

    def close(self):
        self._hub.unsubscribe(self.event_filter, self.handler)


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
        raise RuntimeError("Inconsistent event stream: Duplicate WorkItem scheduled "
                           "with id {}".format(event.work_item_id))
    projection._work_item_start_times[event.work_item_id] = event.timestamp
    return projection


@_when.register(Board.WorkItemRetired)
def _(event, projection):
    if event.work_item_id not in projection._work_item_start_times:
        raise RuntimeError("Inconsistent event stream: Retiring non-existent WorkItem "
                           "with id {}".format(event.work_item_id))
    lead_time = event.timestamp - projection._work_item_start_times[event.work_item_id]
    projection._lead_times[event.work_item_id] = lead_time
    del projection._work_item_start_times[event.work_item_id]
    return projection


@_when.register(Board.WorkItemAbandoned)
def _(event, projection):
    if event.work_item_id not in projection._work_item_start_times:
        raise RuntimeError("Inconsistent event stream: Abandoning non-existent {} "
                           "for id {}".format(event.work_item_id))
    del projection._work_item_start_times[event.work_item_id]
    return projection
