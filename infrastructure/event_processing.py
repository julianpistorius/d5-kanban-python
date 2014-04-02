from functools import reduce
import importlib
from utility.utilities import resolve_attr


class InconsistentEventStreamError(Exception):
    pass


class EventPlayer:
    """Mixin class for replaying events from an Event Store.
    """

    def __init__(self, event_store, mutator, stream_primer=None, **kwargs):
        """Create a new EventPlayer.

        Args:
            event_store: The event store from which to play back events.

            mutator: A function of two arguments which is used to apply changes represented by
                events to the state (e.g. entity) . The function must accept the current state
                as its left argument, and event which will cause the state to be modified as its
                right argument. The function must return a new state, which may or may not be the
                same object as the state argument.

            stream_primer: An optional initial value for the state, otherwise None.

            **kwargs: Any additional arguments will be forwarded to the superclass.
        """
        self._event_store = event_store
        self._mutator = mutator
        self._stream_primer = stream_primer
        # noinspection PyArgumentList
        super().__init__(**kwargs)

    def _replay_events(self, originator_ids):
        """Replay all events or the supplied originator_ids.

        Args:
            originator_ids: An iterable series of originator_ids for which events will be replayed.

        Returns:
            An iterable series of entities reconstituted from the event stream.
        """
        grouped_entity_events = {entity_id: [] for entity_id in originator_ids}
        with self._event_store.open_event_stream() as events:
            for event in events:
                originator_id = event['attributes']['originator_id']
                if originator_id in grouped_entity_events:
                    grouped_entity_events[originator_id].append(event)
        all_entities = map(self._reconstitute, grouped_entity_events.values())
        return all_entities

    def _reconstitute(self, stored_events):
        """Reconstitute an object from a series of events.

        Args:
            stored_events: An iterable series of stored events (deserialised JSON dictionaries).
                All events in the supplied stream must pertain to the same originator object.

        Returns:
            The object obtained by applying the stored events.
        """
        deserialized_events = map(deserialize_event, stored_events)
        obj = self._apply_events(deserialized_events)
        return obj

    def _apply_events(self, event_stream):
        """Current state is the left fold over previous behaviours - Greg Young"""
        return reduce(self._mutator, event_stream, self._stream_primer)


def deserialize_event(stored_event):
    """Recreate an event object.

    Converts a stored event (deserialized JSON object consisting of a dictionary)
    and converts it to a full-blow Python object using the module and class
    information stored in under the 'topic' key.

    Args:
        stored_event: A dictionary resulting from deserializing a JSON event.

    Returns:
        An event object.
    """
    topic = stored_event['topic']
    module_name, _, class_name = topic.partition('#')
    module = importlib.import_module(module_name)
    cls = resolve_attr(module, class_name)
    attributes = stored_event['attributes']
    event = cls(**attributes)
    return event


def extant_entity_ids(event_store, entity_class_name):
    """Scan events to get a list of extant entity ids.

    Args:
        event_store: The event store to search.

        entity_class_name. The entity class name within which particular Created and Discarded
            events are to be found,

    Return:
        A set of extant entity ids.
    """
    entity_ids = set()
    with event_store.open_event_stream() as events:
        for event in events:
            topic = event['topic']
            if topic.endswith(entity_class_name + '.Created'):
                entity_id = event['attributes']['originator_id']
                if entity_id in entity_ids:
                    raise InconsistentEventStreamError("Inconsistent event stream: Duplicate {} creation "
                                                       "for id {}".format(entity_class_name, entity_id))
                entity_ids.add(entity_id)

            elif topic.endswith(entity_class_name + '.Discarded'):
                entity_id = event['attributes']['originator_id']
                if entity_id not in entity_ids:
                    raise InconsistentEventStreamError("Inconsistent event stream: Discarding non-existent {} "
                                                       "for id {}".format(entity_class_name, entity_id))
                entity_ids.discard(entity_id)
    return entity_ids