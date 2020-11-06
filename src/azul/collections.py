from collections import (
    defaultdict,
)
from functools import (
    partial,
)
from itertools import (
    chain,
    product,
)
from typing import (
    Any,
    Callable,
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

    >>> a = 0  # suppress false PyCharm warning
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


def none_safe_key(none_last: bool = False) -> Callable[[Any], Any]:
    """
    Returns a sort key that handles None values.

    >>> sorted([1, None, 2])
    Traceback (most recent call last):
    ...
    TypeError: '<' not supported between instances of 'NoneType' and 'int'

    >>> sorted([1, None, 2], key=none_safe_key())
    [None, 1, 2]

    >>> sorted([1, None, 2], key=none_safe_key(none_last=True))
    [1, 2, None]
    """

    def inner_func(v):
        return (v is None, v) if none_last else (v is not None, v)

    return inner_func


def none_safe_tuple_key(none_last: bool = False) -> Callable[[Tuple[Any]], Any]:
    """
    Returns a sort key that handles tuples containing None values.

    >>> sorted([(2, 'c'), (None, 'a'), (2, None), (1, 'b')])
    Traceback (most recent call last):
    ...
    TypeError: '<' not supported between instances of 'NoneType' and 'int'

    >>> sorted([(2, 'c'), (None, 'a'), (2, None), (1, 'b')], key=none_safe_tuple_key())
    [(None, 'a'), (1, 'b'), (2, None), (2, 'c')]

    >>> sorted([(2, 'c'), (None, 'a'), (2, None), (1, 'b')], key=none_safe_tuple_key(none_last=True))
    [(1, 'b'), (2, 'c'), (2, None), (None, 'a')]
    """

    def inner_func(t):
        assert isinstance(t, tuple)
        return tuple(map(none_safe_key(none_last=none_last), t))

    return inner_func


def compose_keys(f, g):
    """
    Composes unary functions.

    >>> from operator import itemgetter
    >>> key = itemgetter('a', 'b')
    >>> v = dict(b=1, a=None)
    >>> key(v)
    (None, 1)
    >>> composed_key = compose_keys(none_safe_tuple_key(), key)
    >>> composed_key(v)
    ((False, None), (True, 1))

    """
    return lambda v: f(g(v))


def adict(seq: Union[Mapping[K, V], Iterable[Tuple[K, V]]] = None,
          /,
          **kwargs: V
          ) -> Dict[K, V]:
    """
    Like dict() but ignores keyword arguments that are None. Really only useful
    for literals. May be inefficient for large arguments.

    >>> adict(a=None, b=42)
    {'b': 42}

    None values in the positional argument are retained.

    >>> adict({'a':None}, b=None, c=42)
    {'a': None, 'c': 42}

    Just like dict(), …

    >>> dict(**{' ': 42})
    {' ': 42}

    … this function allows syntactically invalid keyword argument names

    >>> adict(**{' ': 42})
    {' ': 42}

    The positional-only argument doesn't collide with a keyword argument of the
    same name.

    >>> adict(seq=1)
    {'seq': 1}

    >>> adict(seq=None)
    {}
    """
    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    return kwargs if seq is None else dict(seq, **kwargs)


def atuple(*args: V) -> Tuple[V, ...]:
    """
    >>> atuple()
    ()

    >>> atuple(None)
    ()

    >>> atuple(0, None)
    (0,)
    """
    return tuple(arg for arg in args if arg is not None)


def alist(*args: V) -> List[V]:
    """
    >>> alist()
    []

    >>> alist(None)
    []

    >>> alist(0, None)
    [0]
    """
    return list(arg for arg in args if arg is not None)


class NestedDict(defaultdict):
    """
    A defauldict of defaultdict's up to the given depth, then of a defaultdict
    whose values are created using the given factory.

    With a depth of 0 it's equivalent to defaultdict:

    >>> d = NestedDict(0, int)
    >>> d[0] += 1
    >>> d
    NestedDict(<class 'int'>, {0: 1})

    >>> d.to_dict()
    {0: 1}

    >>> d = NestedDict(1, int)
    >>> d[0][1] += 2
    >>> d
    NestedDict(..., {0: NestedDict(<class 'int'>, {1: 2})})

    >>> d.to_dict()
    {0: {1: 2}}
    """

    def __init__(self, depth: int, leaf_factory):
        super().__init__(partial(NestedDict, depth - 1, leaf_factory)
                         if depth else
                         leaf_factory)

    def to_dict(self):
        return {
            k: v.to_dict() if isinstance(v, NestedDict) else v
            for k, v in self.items()
        }
