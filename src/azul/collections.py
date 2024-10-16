from collections import (
    defaultdict,
)
from collections.abc import (
    Iterable,
    Iterator,
    Mapping,
    MutableSet,
)
from functools import (
    partial,
)
from itertools import (
    chain,
    product,
)
from operator import (
    itemgetter,
)
from typing import (
    Any,
    Callable,
    TypeVar,
    Union,
)


def dict_merge(dicts: Iterable[Mapping]) -> dict:
    """
    Merge all dictionaries yielded by the argument.

    >>> a = 0  # suppress false PyCharm warning
    >>> dict_merge({a: a + 1, a + 1: a} for a in (0, 2))
    {0: 1, 1: 0, 2: 3, 3: 2}

    Entries from later dictionaries take precedence over those from earlier
    ones:

    >>> dict_merge([{1: 2}, {1: 3}])
    {1: 3}

    >>> dict_merge([])
    {}
    """
    return dict(chain.from_iterable(d.items() for d in dicts))


# noinspection PyPep8Naming
class deep_dict_merge:
    """
    Recursively merge the given dictionaries. If more than one dictionary
    contains a given key, and all values associated with this key are themselves
    dictionaries, then the value present in the result is the recursive merging
    of those nested dictionaries.

    >>> deep_dict_merge({0: 1}, {1: 0})
    {0: 1, 1: 0}

    To merge all dictionaries in an iterable, use this form:

    >>> deep_dict_merge.from_iterable([{0: 1}, {1: 0}])
    {0: 1, 1: 0}

    >>> deep_dict_merge({0: {'a': 1}}, {0: {'b': 2}})
    {0: {'a': 1, 'b': 2}}

    Key collisions where either value is not a dictionary raise an exception,
    unless the values compare equal to each other, in which case the entries
    from *earlier* dictionaries takes precedence. This behavior is the opposite
    of `dict_merge`, where later entries take precedence.

    >>> deep_dict_merge({0: 1}, {0: 2})
    Traceback (most recent call last):
    ...
    ValueError: 1 != 2

    >>> l1, l2 = [], []
    >>> d = deep_dict_merge({0: l1}, {0: l2})
    >>> d
    {0: []}
    >>> id(d[0]) == id(l1)
    True

    >>> deep_dict_merge()
    {}
    """

    def __new__(cls, *dicts: Mapping) -> dict:
        return cls.from_iterable(dicts)

    @classmethod
    def from_iterable(cls, dicts: Iterable[Mapping], /) -> dict:
        merged = {}
        for m in dicts:
            for k, v2 in m.items():
                v1 = merged.setdefault(k, v2)
                if v1 != v2:
                    if isinstance(v1, Mapping) and isinstance(v2, Mapping):
                        merged[k] = deep_dict_merge(v1, v2)
                    else:
                        raise ValueError(f'{v1!r} != {v2!r}')
        return merged


K = TypeVar('K')
V = TypeVar('V')


def explode_dict(d: Mapping[K, Union[V, list[V], set[V], tuple[V]]]
                 ) -> Iterable[dict[K, V]]:
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


def none_safe_apply(f: Callable[[K], V], o: K | None) -> V | None:
    """
    >>> none_safe_apply(str, 123)
    '123'

    >>> none_safe_apply(str, None) is None
    True
    """
    return None if o is None else f(o)


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


def none_safe_tuple_key(none_last: bool = False
                        ) -> Callable[[tuple[Any, ...]], Any]:
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


def none_safe_itemgetter(*items: str) -> Callable:
    """
    Like `itemgetter` except that the returned callable returns `None`
    (or a tuple of `None`) if it's passed None.

    >>> f = none_safe_itemgetter('foo', 'bar')
    >>> f({'foo': 1, 'bar': 2})
    (1, 2)

    >>> f(None)
    (None, None)

    >>> none_safe_itemgetter('foo')({'foo':123})
    123

    >>> none_safe_itemgetter('foo')(None) is None
    True

    >>> none_safe_itemgetter()
    Traceback (most recent call last):
    ...
    TypeError: none_safe_itemgetter expected 1 argument, got 0
    """
    if len(items) > 1:
        n = (None,) * len(items)
    elif len(items) == 1:
        n = None
    else:
        raise TypeError('none_safe_itemgetter expected 1 argument, got 0')
    g = itemgetter(*items)

    def f(v):
        return n if v is None else g(v)

    return f


def compose_keys(f: Callable, g: Callable) -> Callable:
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


def adict(seq: Union[Mapping[K, V], Iterable[tuple[K, V]]] = None,
          /,
          **kwargs: V
          ) -> dict[K, V]:
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


def _athing(cls: type, *args):
    return cls(arg for arg in args if arg is not None)


def atuple(*args: V) -> tuple[V, ...]:
    """
    >>> atuple()
    ()

    >>> atuple(None)
    ()

    >>> atuple(0, None)
    (0,)
    """
    return _athing(tuple, *args)


def alist(*args: V) -> list[V]:
    """
    >>> alist()
    []

    >>> alist(None)
    []

    >>> alist(0, None)
    [0]
    """
    return _athing(list, *args)


def aset(*args: V) -> set[V]:
    """
    >>> aset()
    set()

    >>> aset(None)
    set()

    >>> aset(0, None)
    {0}
    """
    return _athing(set, *args)


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

    def to_dict(self) -> dict:
        return {
            k: v.to_dict() if isinstance(v, NestedDict) else v
            for k, v in self.items()
        }


class OrderedSet(MutableSet[K]):
    """
    A mutable set that maintains insertion order. Unlike similar implementations
    of the same name floating around on the internet, it is not a sequence.

    >>> s = OrderedSet(['b', 'a', 'c', 'b']); s
    OrderedSet(['b', 'a', 'c'])

    >>> s.discard('a'); s
    OrderedSet(['b', 'c'])

    >>> s.add('a'); s
    OrderedSet(['b', 'c', 'a'])

    Commutativity of set union and intersection

    >>> s1, s2 = OrderedSet([1, 2, 3]), {3, 4}
    >>> s1 | s2, s2 | s1
    (OrderedSet([1, 2, 3, 4]), OrderedSet([1, 2, 3, 4]))

    >>> s1 & s2, s2 & s1
    (OrderedSet([3]), OrderedSet([3]))
    """

    def __init__(self, members: Iterable[K] = (), /) -> None:
        self.inner: dict[K, None] = dict.fromkeys(members)

    def __repr__(self) -> str:
        contents = repr(list(self)) if self else ''
        return f'{type(self).__name__}({contents})'

    def __iter__(self) -> Iterator[K]:
        return iter(self.inner)

    def __len__(self) -> int:
        return len(self.inner)

    def __eq__(self, other: Any) -> bool:
        """
        Symmetry:

        >>> s1, s2 = OrderedSet(), set()
        >>> s1 == s2, s2 == s1
        (True, True)

        >>> s1, s2 = OrderedSet([3, 1, 3]), {1, 3, 1}
        >>> s1 == s2, s2 == s1
        (True, True)

        Transitivity:

        >>> s3 = OrderedSet([1, 3, 1])
        >>> (s1 == s2, s2 == s3, s1 == s3)
        (True, True, True)

        >>> s4 = OrderedSet([1])
        >>> (s2 == s4, s1 == s4)
        (False, False)
        """
        return self.inner.keys() == other

    def __contains__(self, member: K) -> bool:
        """
        >>> 'a' in OrderedSet(['a', 'b'])
        True

        Transitivity of subset relation:

        >>> s1, s2, s3 = OrderedSet([1]), {1, 3}, OrderedSet([1, 3, 4])
        >>> s1 < s2, s2 < s3, s1 < s3
        (True, True, True)

        >>> s4 = OrderedSet([1, 4])
        >>> s2 < s4, s1 < s4
        (False, True)
        """
        return member in self.inner

    def discard(self, member: K) -> None:
        """
        >>> s = OrderedSet([1, 'a', 2])
        >>> s.discard('a'); s
        OrderedSet([1, 2])

        >>> s.discard('a'); s
        OrderedSet([1, 2])
        """
        self.inner.pop(member, None)

    def add(self, member: K) -> None:
        """
        >>> s = OrderedSet(['a', 'b'])
        >>> s.add('a'); s
        OrderedSet(['a', 'b'])

        >>> s.add('c'); s
        OrderedSet(['a', 'b', 'c'])
        """
        self.inner[member] = None

    def update(self, members: Iterable[K] = (), /) -> None:
        """
        >>> s = OrderedSet(['a', 'b'])
        >>> s.update([1, 'a', 'b', 2])
        >>> s
        OrderedSet(['a', 'b', 1, 2])
        """
        self.inner |= dict.fromkeys(members)
