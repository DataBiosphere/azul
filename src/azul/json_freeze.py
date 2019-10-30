from azul.types import (
    AnyJSON,
    AnyMutableJSON,
)
from azul.vendored.frozendict import frozendict


def freeze(x: AnyJSON) -> AnyJSON:
    """
    Return a copy of the argument JSON structure with every `dict` in that structure converted to a `frozendict` and
    every list converted to a tuple.

    Frozen JSON structures are immutable and can be used as keys in other dictionaries.

    >>> k = freeze({"1":[2,3]})
    >>> k_ = k.copy()
    >>> k is k_
    False
    >>> {k: 42}[k_]
    42

    Freeze is idempotent

    >>> thaw(freeze(freeze({"1":[2,3]})))
    {'1': [2, 3]}
    """
    if isinstance(x, (dict, frozendict)):
        return frozendict((k, freeze(v)) for k, v in x.items())
    elif isinstance(x, (list, tuple)):
        return tuple(freeze(v) for v in x)
    elif isinstance(x, (bool, str, int, float)) or x is None:
        return x
    else:
        assert False, f'Cannot handle values of type {type(x)}'


def thaw(x: AnyJSON) -> AnyMutableJSON:
    """
    Return a copy of the argument JSON structure with every `frozendict` in that structure converted to a `dict` and
    every tuple converted to a list.

    >>> d = {"1":[2, 3]}
    >>> d_ = thaw(freeze(d))
    >>> d_ == d, d_ is d
    (True, False)

    thaw() is idempotent

    >>> thaw(thaw(freeze(d)))
    {'1': [2, 3]}
    """
    if isinstance(x, (frozendict, dict)):
        return {k: thaw(v) for k, v in x.items()}
    elif isinstance(x, (tuple, list)):
        return [thaw(v) for v in x]
    elif isinstance(x, (bool, str, int, float)) or x is None:
        return x
    else:
        assert False, f'Cannot handle values of type {type(x)}'


def sort_frozen(x: AnyJSON) -> AnyJSON:
    """
    Attempt to recursively sort a frozen JSON structure. Not all JSON structures are supported. The restrictions are
    noted below. This method is really only useful when comparing Elasticsearch documents. Elasticsearches semantics
    for lists is that the order in which list elements occur doesn't really matter. The "term" query {"foo": "bar"}
    matches a documents with "foo": "bar" and ones with "foo":["baz","bar"].

        >>> sort_frozen(freeze({"2": [{"3": True}, {"4": [5, None, None]}], "1": 1}))
        (('1', 1), ('2', ((('3', True),), (('4', (None, None, 5)),))))

    Tuples in the frozen JSON must only contain values that are either None or of types that are comparable against
    each other. All None values in a tuple are put first in the sorted tuple, as if None were less than any other
    value.

        >>> sort_frozen(freeze([0, ""]))
        Traceback (most recent call last):
        ...
        TypeError: '<' not supported between instances of 'str' and 'int'

    Note that True == 0 and False == 1

        >>> sort_frozen(freeze([1, 0, False]))
        (0, False, 1)

    >>> sort_frozen(freeze([{'x':True}, {'x': None}]))
    ((('x', None),), (('x', True),))
    """
    if isinstance(x, frozendict):
        # Note that each key occurs exactly once, so there will be no ties that have to be broken by comparing the
        # values. The values may of heterogeneous types and therefore can't be compared.
        return tuple(sorted((k, sort_frozen(v)) for k, v in x.items()))
    elif isinstance(x, tuple):
        return tuple(sorted((sort_frozen(v) for v in x), key=TupleKey))
    elif isinstance(x, (bool, str, int, float)) or x is None:
        return x
    else:
        assert False, f'Cannot handle values of type {type(x)}'


class TupleKey(object):
    """
    Tuples are compared element-wise so (None,) < (True,) involves None < True
    which fails. To solve this, we wrap all tuple elements. Note that this
    means recursively wrapping tuple elements that are tuples themselves.

    >>> # noinspection PyTypeChecker
    ... (None,) < (True,)
    Traceback (most recent call last):
    ...
    TypeError: '<' not supported between instances of 'NoneType' and 'bool'

    >>> TupleKey((None,)) < TupleKey((True,))
    True

    From https://docs.python.org/3.6/reference/datamodel.html#object.__hash__

    > A class that overrides __eq__() and does not define __hash__() will have
    > its __hash__() implicitly set to None.

    Just making sure

    >>> {TupleKey((True,)):1}
    Traceback (most recent call last):
    ...
    TypeError: unhashable type: 'TupleKey'
    """
    __slots__ = ['obj']

    def __init__(self, obj):
        if isinstance(obj, tuple):
            obj = tuple(TupleKey(e) for e in obj)
        self.obj = obj

    def __lt__(self, other):
        if self.obj is None:
            return other.obj is not None
        else:
            return other.obj is not None and self.obj < other.obj

    def __gt__(self, other):
        raise NotImplementedError()

    def __eq__(self, other):
        return self.obj == other.obj

    def __le__(self, other):
        raise NotImplementedError()

    def __ge__(self, other):
        raise NotImplementedError()
