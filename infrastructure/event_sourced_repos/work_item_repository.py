from infrastructure.event_sourced_repos.stored_event_repository import StoredEventRepository
from kanban.domain.model import workitem


class WorkItemRepository(workitem.Repository, StoredEventRepository):

    def __init__(self, event_store, hub, **kwargs):
        """
        Args:
            event_store: An EventStore instance from which entities can be reconstituted.
            hub: MessageHub
        """
        super().__init__(event_store=event_store,
                         entity_class_name='WorkItem',
                         mutator=workitem.mutate,
                         stream_primer=hub,
                         **kwargs)

    def all_work_items(self):
        work_item_ids = self._extant_entity_ids()
        return self._reconstitute_entities(work_item_ids)

    def work_items_where(self, predicate):
        return filter(predicate, self.all_work_items())









