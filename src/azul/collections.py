from itertools import (
    chain,
    product,
)
from typing import (
    Dict,
    Iterable,
    List,
    Mapping,
    Set,
    Tuple,
    TypeVar,
    Union,
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


K = TypeVar('K')
V = TypeVar('V')


def explode_dict(d: Mapping[K, Union[V, List[V], Set[V], Tuple[V]]]) -> Iterable[Dict[K, V]]:
    """
    An iterable of dictionaries, one dictionary for every possible combination
    of items from iterable values in the argument dictionary. Only instances of
    set, list and tuple are considered iterable. Values of other types will be
    treated as if they were a singleton iterable. All returned dictionaries have
    exactly the same set of keys as the parameter.

    >>> list(explode_dict({'a': [1, 2, 3], 'b': [4, 5], 'c': 6})) # doctest: +NORMALIZE_WHITESPACE
    [{'a': 1, 'b': 4, 'c': 6},
     {'a': 1, 'b': 5, 'c': 6},
     {'a': 2, 'b': 4, 'c': 6},
     {'a': 2, 'b': 5, 'c': 6},
     {'a': 3, 'b': 4, 'c': 6},
     {'a': 3, 'b': 5, 'c': 6}]
    """
    vss = (
        vs if isinstance(vs, (list, tuple, set)) else (vs,)
        for vs in d.values()
    )
    for t in product(*vss):
        yield dict(zip(d.keys(), t))


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
