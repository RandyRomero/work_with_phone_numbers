import typing as tp

VALUE = tp.TypeVar("VALUE")

from itertools import islice


def chunked(iterable: tp.Iterable[VALUE], chunk_size: int) -> tp.Iterator[tp.List[VALUE]]:
    """Returns one chunk of the given iterable at a time"""
    # we need to be sure that we store an iterator in closure in order to keep
    # the state of iteration
    iterable = iter(iterable)
    def wrapper() -> tp.List[VALUE]:
        # we need a closure to save state of iterator, otherwise it will
        # always start over and return first n elements on every call
        return list(islice(iterable, chunk_size))
    # The second argument of iter is what will return on stop iteration.
    return iter(wrapper, [])