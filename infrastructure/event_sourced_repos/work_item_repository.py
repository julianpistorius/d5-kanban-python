from infrastructure.event_processing import EventPlayer, extant_entity_ids
from kanban.domain.model import workitem


class WorkItemRepository(workitem.Repository, EventPlayer):
    """Concrete repository for WorkItems in terms of an event store.
    """

    def __init__(self, event_store, **kwargs):
        """Create a new WorkItemRepository.

        Args:
            event_store: An EventStore instance from which work items can be reconstituted.

        """
        super().__init__(event_store=event_store,
                         mutator=workitem.mutate,
                         **kwargs)

    def all_work_items(self, work_item_ids=None):
        """Obtain all WorkItems.

        Args:
            work_items_ids: An optional list of work item ids used to restrict the results.
                If not provided, all work items will be returned.

        Returns:
            An iterable series of WorkItems.
        """

        if work_item_ids is None:
            work_item_ids = extant_entity_ids(self._event_store, entity_class_name='WorkItem')
        return self._replay_events(work_item_ids)

    def work_items_where(self, predicate, work_item_ids=None):
        """Obtain all WorkItems which match a predicate.

        Args:
            predicate: A single argument function used to identify the work items to be returned.

            work_items_ids: An optional list of work item ids used to restrict the results.
                If not provided, all work items which match the predicate will be returned.

        Returns:
            An iterable series of WorkItems.
        """
        if work_item_ids is None:
            work_item_ids = extant_entity_ids(self._event_store, entity_class_name='WorkItem')
        work_items = self._replay_events(work_item_ids)
        return filter(predicate, work_items)
