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
            work_item_ids = extant_entity_ids(
                event_store=self._event_store,
                entity_class_name='WorkItem')
        return self._replay_events(work_item_ids)

    def work_items_where(self, predicate, work_item_ids=None):
        """Obtain WorkItem instances.

        Retrieve WorkItem instances which satisfy a predicate function. The
        series of WorkItems to be tested against the predicate can be further
        contrained by an optional series of work_item_ids.

        Args:
            predicate: A unary callable agaist which candidate WorkItems will be
                tested. Only those WorkItems for which the function returns True
                will be in the result collection.

            work_item_ids: An optional iterable series of WorkItem ids. If
                not None, only those WorkItems whose ids are in this series will
                be in the result collection.

        Returns:
            An iterable series of WorkItems.
        """
        if work_item_ids is None:
            work_item_ids = extant_entity_ids(
                event_store=self._event_store,
                entity_class_name='WorkItem')
        work_items = self._replay_events(work_item_ids)
        return filter(predicate, work_items)
