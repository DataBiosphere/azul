from typing import (
    TypeVar,
    Mapping,
    Union,
)
from enum import Enum

K = TypeVar('K')
V = TypeVar('V')


class LookupDefault(Enum):
    RAISE = 0


def lookup(d: Mapping[K, V], k: K, *ks: K, default: Union[V, LookupDefault] = LookupDefault.RAISE) -> V:
    """
    Look up a value in the specified dictionary given one or more candidate keys.

    This function raises a key error for the first (!) key if none of the keys are present and the `default` keyword
    argument absent. If the `default` keyword argument is present (None is a valid default), this function returns
    that argument instead of raising an KeyError in that case. This is notably different to dict.get() whose default
    default is `None`. This function does not have a default default.

    If the first key is present, return its value ...
    >>> lookup({1:2}, 1)
    2

    ... and ignore the other keys.
    >>> lookup({1:2}, 1, 3)
    2

    If the first key is absent, try the fallbacks.
    >>> lookup({1:2}, 3, 1)
    2

    If the key isn't present, raise a KeyError referring to that key.
    >>> lookup({1:2}, 3)
    Traceback (most recent call last):
    ...
    KeyError: 3

    If neither the first key nor the fallbacks are present, raise a KeyError referring to the first key.
    >>> lookup({1:2}, 3, 4)
    Traceback (most recent call last):
    ...
    KeyError: 3

    If the key isn't present but a default was passed, return the default.
    >>> lookup({1:2}, 3, default=4)
    4

    None is a valid default.
    >>> lookup({1:2}, 3, 4, default=None) is None
    True
    """
    try:
        return d[k]
    except KeyError:
        for k in ks:
            try:
                return d[k]
            except KeyError:
                pass
        else:
            if default is LookupDefault.RAISE:
                raise
            else:
                return default
