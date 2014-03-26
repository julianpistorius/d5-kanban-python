from infrastructure.event_processing import EventPlayer, extant_entity_ids
from kanban.domain.model import workitem


class WorkItemRepository(workitem.Repository, EventPlayer):

    def __init__(self, event_store, hub, **kwargs):
        """
        Args:
            event_store: An EventStore instance from which entities can be reconstituted.
            hub: MessageHub
        """
        super().__init__(event_store=event_store,
                         mutator=workitem.mutate,
                         stream_primer=hub,
                         **kwargs)

    all_extant = object()

    def all_work_items(self, work_item_ids=None):
        if work_item_ids is None:
            work_item_ids = extant_entity_ids(self._event_store, entity_class_name='WorkItem')
        return self._replay_events(work_item_ids)

    def work_items_where(self, predicate, work_item_ids=None):
        if work_item_ids is None:
            work_item_ids = extant_entity_ids(self._event_store, entity_class_name='WorkItem')
        work_items = self._replay_events(work_item_ids)
        return filter(predicate, work_items)
