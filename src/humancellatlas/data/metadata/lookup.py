from typing import TypeVar, Mapping

K = TypeVar('K')
V = TypeVar('V')


def lookup(d: Mapping[K, V], k: K, *ks: K) -> V:
    """
    Look up a value in the specified dictionary given one or more candidate keys.

    Raises a key error for the first (!) key if none of the keys are present.

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
            raise
