import importlib


# TODO: This needs to be some sort of base class, which can be extended
def reconstitute(events_stream):
    entity = None

    def create_entity(topic, id, version, name, description):
        assert topic.endswith('Created')
        nonlocal entity
        module_name = '.'.join(topic.split('.')[:-2])
        class_name = topic.split('.')[-2]
        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
        entity = cls(*args, **kwargs)

    def change_attribute(topic, id, version, name, value):
        assert topic.endswith('AttributeChanged')
        if entity.id != id:
            raise RuntimeError("Inconsistent event ID")
        if version != entity.version + 1:
            raise RuntimeError("Unexpected version")
        setattr(entity, name, value)

    def delete_entity(topic, id, version):
        assert topic.endswith('Deleted')
        nonlocal entity
        if entity.id != id:
            raise RuntimeError("Inconsistent event ID")
        if version != entity.version:
            raise RuntimeError("Unexpected version")
        entity = None

    event_mutators = {'Created': create_entity,
                      'AttributeChanged': change_attribute,
                      'Deleted': delete_entity}

    for event in events_stream:
        topic = event['topic']
        topic_tail = topic.split('.')[-1]
        args = event['args']
        kwargs = event['kwargs']
        event_mutators[topic_tail](topic, *args, **kwargs)

    if entity is None:
        raise ValueError("No matching entity at event stream termination")

    # Enable message hub -  find a nicer way to do this
    #entity.hub_ = hub()
    return entity

