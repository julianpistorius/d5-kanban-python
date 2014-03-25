import datetime

def exactly_one(iterable):
    i = iter(iterable)
    try:
        item = next(i)
    except StopIteration:
        raise ValueError("Too few items. Expected exactly one.")
    try:
        next(i)
    except StopIteration:
        return item
    raise ValueError("Too many items. Expected exactly one.")


def resolve_attr(obj, path):
    """A recursive version of getattr for navigating dotted paths"""
    if not path:
        return obj
    head, _, tail = path.partition('.')
    head_obj = getattr(obj, head)
    return resolve_attr(head_obj, tail)

def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).timestamp()