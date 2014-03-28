def resolve_attr(obj, path):
    """A recursive version of getattr for navigating dotted paths"""
    if not path:
        return obj
    head, _, tail = path.partition('.')
    head_obj = getattr(obj, head)
    return resolve_attr(head_obj, tail)

