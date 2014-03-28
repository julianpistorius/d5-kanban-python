import collections
from itertools import islice


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


def consume(iterator, n=None):
    "Advance the iterator n-steps ahead. If n is none, consume entirely."
    # Use functions that consume iterators at C speed.
    if n is None:
        # feed the entire iterator into a zero-length deque
        collections.deque(iterator, maxlen=0)
    else:
        # advance to the empty slice starting at position n
        next(islice(iterator, n, n), None)
