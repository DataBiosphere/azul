from itertools import chain
from typing import (
    Iterable,
    Mapping,
)


def dict_merge(dicts: Iterable[Mapping]) -> Mapping:
    """
    Merge all dictionaries yielded by the argument.

    >>> a = 0  # supress false PyCharm warning
    >>> dict_merge({a: a + 1, a + 1: a} for a in (0, 2))
    {0: 1, 1: 0, 2: 3, 3: 2}

    Entries from later dictionaries take precedence over those from earlier ones:

    >>> dict_merge([{1: 2}, {1: 3}])
    {1: 3}

    >>> dict_merge([])
    {}
    """
    items = chain.from_iterable(map(lambda d: d.items(), dicts))
    return dict(items)


def none_safe_key(v):
    """
    A sort key that handles None values.

    >>> sorted([1, None, 2])
    Traceback (most recent call last):
    ...
    TypeError: '<' not supported between instances of 'NoneType' and 'int'

    >>> sorted([1, None, 2], key=none_safe_key)
    [None, 1, 2]
    """
    return v is not None, v


def none_safe_tuple_key(t):
    """
    A sort key that handles tuples containing None value.

    >>> sorted([(2, 'c'), (None, 'a'), (2, None), (1, 'b')])
    Traceback (most recent call last):
    ...
    TypeError: '<' not supported between instances of 'NoneType' and 'int'

    >>> sorted([(2, 'c'), (None, 'a'), (2, None), (1, 'b')], key=none_safe_tuple_key)
    [(None, 'a'), (1, 'b'), (2, None), (2, 'c')]
    """
    assert isinstance(t, tuple)
    return tuple(map(none_safe_key, t))


def compose_keys(f, g):
    """
    Composes unary functions.

    >>> from operator import itemgetter
    >>> key = itemgetter('a', 'b')
    >>> v = dict(b=1, a=None)
    >>> key(v)
    (None, 1)
    >>> composed_key = compose_keys(none_safe_tuple_key, key)
    >>> composed_key(v)
    ((False, None), (True, 1))

    """
    return lambda v: f(g(v))
