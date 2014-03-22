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