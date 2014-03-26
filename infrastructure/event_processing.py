from functools import reduce
import importlib
from utility.utilities import resolve_attr


class EventPlayer:

    def __init__(self, event_store, mutator, stream_primer=None, **kwargs):
        """
        Args:
            event_store:
            entity_class_name:
            mutator:
        """
        self._event_store = event_store
        self._mutator = mutator
        self._stream_primer = stream_primer
        # noinspection PyArgumentList
        super().__init__(**kwargs)

    def _replay_events(self, originator_ids):
        grouped_entity_events = {entity_id: [] for entity_id in originator_ids}
        with self._event_store.open_event_stream() as events:
            for event in events:
                originator_id = event['attributes']['originator_id']
                if originator_id in grouped_entity_events:
                    grouped_entity_events[originator_id].append(event)
        all_entities = list(map(self._reconstitute, grouped_entity_events.values()))
        return all_entities

    def _reconstitute(self, stored_event_sequence):
        deserialized_events = map(deserialize_event, stored_event_sequence)
        obj = self._apply_events(deserialized_events)
        return obj

    def _apply_events(self, event_stream):
        """Current State is the left fold over previous behaviours - Greg Young"""
        return reduce(self._mutator, event_stream, self._stream_primer)


def deserialize_event(stored_event):
    """Recreate an event object"""
    topic = stored_event['topic']
    module_name, _, class_name = topic.partition('#')
    module = importlib.import_module(module_name)
    cls = resolve_attr(module, class_name)
    attributes = stored_event['attributes']
    event = cls(**attributes)
    return event


def extant_entity_ids(event_store, entity_class_name):
    """Scan events to get a list of extant entity ids
    """
    entity_ids = set()
    with event_store.open_event_stream() as events:
        for event in events:
            topic = event['topic']
            if topic.endswith(entity_class_name + '.Created'):
                entity_id = event['attributes']['originator_id']
                if entity_id in entity_ids:
                    raise RuntimeError("Inconsistent event stream: Duplicate {} creation "
                                       "for id {}".format(entity_class_name, entity_id))
                entity_ids.add(entity_id)

            elif topic.endswith(entity_class_name + '.Discarded'):
                entity_id = event['attributes']['originator_id']
                if entity_id not in entity_ids:
                    raise RuntimeError("Inconsistent event stream: Discarding non-existent {} "
                                       "for id {}".format(entity_class_name, entity_id))
                entity_ids.discard(entity_id)
    return entity_ids