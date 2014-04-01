def resolve_attr(obj, path):
    """A recursive version of getattr for navigating dotted paths.

    Args:
        obj: An object for which we want to retrieve a nested attribute.
        path: A dot separated string containing zero or more attribute names.

    Returns:
        The attribute referred to by obj.a1.a2.a3...

    Raises:
        AttributeError: If there is no such attribute.
    """
    if not path:
        return obj
    head, _, tail = path.partition('.')
    head_obj = getattr(obj, head)
    return resolve_attr(head_obj, tail)
