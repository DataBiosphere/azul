from collections import (
    defaultdict,
)
import copy
import json
from json import (
    JSONDecodeError,
)
from typing import (
    Mapping,
    ReadOnly,
    Self,
    Sequence,
    TypeGuard,
    TypedDict,
    Union,
    final,
)

import attr
from chalice.app import (
    ForbiddenError,
)
from more_itertools import (
    one,
    only,
)

from azul.lib import (
    R,
)
from azul.lib.types import (
    AnyJSON,
    FlatJSON,
    JSON,
    JSONTypedDict,
    MutableFlatJSON,
    PrimitiveJSON,
    check_type,
    json_element_strings,
    json_mapping,
)
from azul.plugins import (
    FieldName,
    MetadataPlugin,
)

type IsFilterValuesJSON = Union[
    Sequence[str | None],
    Sequence[int | None],
    Sequence[float | None],
    Sequence[bool | None],
    Sequence[FlatJSON | None],
    Sequence[Sequence[int] | None],
    Sequence[Sequence[float] | None],
    Sequence[Sequence[str] | None]
]


# `is` is a reserved keyword so we can't use the class-based syntax for
# TypedDict, but have to use the alternative syntax instead. This also prevents
# us from using JSONTypedDict. I wasn't able to use `final` with the alternative
# syntax, hence the need to subclass again. @final is needed to enable mypy to
# narrow `filter: FilterJSON` using `assert 'in' in filter`.
#
@final
class IsFilterJSON(TypedDict('_IsFilterJSON',
                             {'is': ReadOnly[IsFilterValuesJSON]})):
    pass


@final
class IsNotFilterJSON(JSONTypedDict):
    is_not: ReadOnly[IsFilterValuesJSON]


type RangeFilterValuesJSON = Union[
    Sequence[Sequence[int]],
    Sequence[Sequence[float]],
    Sequence[Sequence[str]]
]


@final
class IntersectsFilterJSON(JSONTypedDict):
    intersects: ReadOnly[RangeFilterValuesJSON]


type ContainsFilterValuesJSON = Union[
    Sequence[int | Sequence[int]],
    Sequence[float | Sequence[float]],
    Sequence[str | Sequence[str]]
]


@final
class ContainsFilterJSON(JSONTypedDict):
    contains: ReadOnly[ContainsFilterValuesJSON]


@final
class WithinFilterJSON(JSONTypedDict):
    within: ReadOnly[RangeFilterValuesJSON]


type FilterJSON = Union[
    IsFilterJSON,
    IsNotFilterJSON,
    IntersectsFilterJSON,
    ContainsFilterJSON,
    WithinFilterJSON
]

type FiltersJSON = Mapping[FieldName, FilterJSON]


def is_filters_json(v: JSON) -> TypeGuard[FiltersJSON]:
    """
    >>> is_filters_json({'x':{'is': ["a", "b"]}})
    True
    >>> is_filters_json({'x':{'is': ["a", 1]}})
    False
    >>> is_filters_json({'x':{'is': [None, {}]}})
    True
    >>> is_filters_json({
    ... 'a': {'is': []},
    ... 'b': {'is_not': [1, 2, None]},
    ... 'c': {'is': [None, {'x': 42, 'y': 45}]},
    ... 'd': {'within': [[1, 2], [3, 4]]},
    ... 'e': {'intersects': [[1, 2], [3, 4]]},
    ... 'f': {'contains': ['a', 'b']},
    ... 'g': {'contains': [[1, 2], [3, 4]]}
    ... })
    True
    """
    return check_type(FiltersJSON, v)


type _FilterJSON = Mapping[
    FieldName,
    Mapping[
        str,
        Sequence[PrimitiveJSON | FlatJSON | Sequence[PrimitiveJSON]]
    ]
]

type _FilterMutableJSON = dict[
    FieldName,
    dict[
        str,
        list[PrimitiveJSON | MutableFlatJSON | list[PrimitiveJSON]]
    ]
]


def _upcast_filters(filters: FiltersJSON) -> _FilterJSON:
    """
    Mypy does not realize that FilterJSON is actually JSON, probably due to the
    union of TypedDicts in its definition. Instead, it widens the filter values
    to just ``object``. Use this function if you need to process filters more
    generically, independent of the specific operators and the different
    constraints they impose on the values they operate on.
    """
    return filters  # type: ignore[return-value]


def _upcast_filters_unsafe(filters) -> _FilterMutableJSON:
    """
    Same as :meth:`upcast_filters` but pretends that the result is mutable.
    Callers must immediately make a deep copy of the returned value.
    """
    return filters  # type: ignore[return-value]


def parse_filters(raw_filters: str | None) -> AnyJSON:
    """
    Deserialize, validate and normalize the given string form of the `filters`
    request parameter. The aim of normalization is to eliminate any
    insignificant differences so that serializing the value returned from calls
    to this method with semantically equivalent and valid arguments yields
    exactly the same JSON string. Two valid arguments are considered
    semantically equivalent if they match the same subset of all possible
    documents.

    >>> parse_filters(None)
    {}

    >>> parse_filters('{}')
    {}

    >>> parse_filters('{]')
    Traceback (most recent call last):
        ...
    AssertionError: R('Filters are not valid JSON')
    """
    if raw_filters is None:
        return {}
    else:
        try:
            return json.loads(raw_filters)
        except JSONDecodeError:
            assert False, R('Filters are not valid JSON')


def validate_filters(filters: AnyJSON) -> FiltersJSON:
    """
    These doctests only have the validation failures, the the happy paths are
    covered in the doctests of py:meth:`normalize_filters`.

    >>> validate_filters({'x': {'within': ['c', ['a', 'b']]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Invalid filter values', 'x')

    >>> validate_filters([])
    Traceback (most recent call last):
        ...
    AssertionError: R('Filters must be an object')

    >>> validate_filters({'': {'is': [42]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Empty field name')

    >>> validate_filters({'x': 42})
    Traceback (most recent call last):
        ...
    AssertionError: R('Filter must be an object', 'x')

    >>> validate_filters({'x': {}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Need exactly one filter per field', 'x')

    >>> validate_filters({'x': {'is': [1], 'contains': [1]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Need exactly one filter per field', 'x')

    >>> validate_filters({'x': {'foo': [2]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Invalid operator', 'x', 'foo')

    >>> validate_filters({'x': {'is': {}}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Values must be an array', 'x')

    >>> validate_filters({'x': {'is': []}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Need at least one value', 'x')

    >>> validate_filters({'x': {'within': []}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Need at least one value', 'x')

    >>> validate_filters({'x': {'is': [1, 1]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Duplicate values', 'x')

    >>> validate_filters({'x': {'is': [None, None]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Duplicate values', 'x')

    >>> validate_filters({'x': {'within': [1]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Invalid filter values', 'x')

    >>> validate_filters({'x': {'is': [42, 4.1]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Invalid filter values', 'x')

    >>> validate_filters({'x': {'within': [[1]]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Range must be list of length 2', 'x')

    >>> validate_filters({'x': {'within': [[2, 1]]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Range is inverted', 'x')

    >>> validate_filters({'x': {'intersects': [[2, 1]]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Range is inverted', 'x')

    >>> validate_filters({'x': {'within': [[1, 2], ['', '']]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Invalid filter values', 'x')

    >>> validate_filters({'x': {'within': [[1, 1.1], [2, 2.2]]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Invalid filter values', 'x')

    >>> validate_filters({'x': {'within': [[False, True]]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Invalid filter values', 'x')

    >>> validate_filters({'x': {'within': [{}]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Invalid filter values', 'x')

    >>> validate_filters({'x': {'within': [[1, 2], [1, 2]]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Duplicate values', 'x')

    >>> validate_filters({'x': {'is': [{}]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Empty object', 'x')

    >>> validate_filters({'x': {'is': [{'': 1}]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Empty property name', 'x')

    >>> validate_filters({'x': {'is': [{'y': 1}, {'z': 2}]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Inconsistent property names', 'x')

    >>> validate_filters({'x': {'is': [{'y': 1, 'z': []}]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Invalid filter values', 'x')

    >>> validate_filters({'x': {'is': [{'y': 1, 'z': 2}, {'y': '', 'z': 3}]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Inconsistent property values', 'x')

    >>> validate_filters({'x': {'is': [{'a': 1, 'b': 2}, {'b': 2, 'a': 1}]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Duplicate objects', 'x')

    >>> validate_filters({'x': {'contains': [1, 'a']}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Invalid filter values', 'x')

    >>> validate_filters({'x':{'is': [[]]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Range must be list of length 2', 'x')

    >>> validate_filters({'x':{'is_not': [[1, 2, 3]]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Range must be list of length 2', 'x')

    >>> validate_filters({'x':{'is': [[1, None]]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Invalid filter values', 'x')

    >>> validate_filters({'x':{'is_not': [[1, 2], ['a', 'b']]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Invalid filter values', 'x')

    >>> validate_filters({'x':{'is': [[2, 1]]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Range is inverted', 'x')

    >>> validate_filters({'x':{'is_not': [['b', 'a']]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Range is inverted', 'x')

    >>> validate_filters({'x':{'is': [[1, 2], [1, 2]]}})
    Traceback (most recent call last):
        ...
    AssertionError: R('Duplicate values', 'x')
    """

    def validate_range(field: str, value: list[int] | list[float] | list[str]):
        assert len(value) == 2, R('Range must be list of length 2', field)
        assert value == sorted(value), R('Range is inverted', field)

    assert type(filters) is dict, R('Filters must be an object')
    for field, filter in filters.items():
        assert len(field) > 0, R('Empty field name')
        assert type(filter) is dict, R('Filter must be an object', field)
        assert len(filter) == 1, R('Need exactly one filter per field', field)
        operator, values = one(filter.items())
        assert type(values) is list, R('Values must be an array', field)
        assert len(values) > 0, R('Need at least one value', field)
        value_types = set(map(type, values))
        if operator in {'is', 'is_not'}:
            identity_filters = IsFilterJSON | IsNotFilterJSON
            assert check_type(identity_filters, filter), R('Invalid filter values', field)
            value_types.discard(type(None))
            value_type = only(value_types)
            if value_type is dict:
                assert operator == 'is', operator
                key_sets = set(map(frozenset, map(dict.keys, values)))
                assert len(key_sets) == 1, R('Inconsistent property names', field)
                keys = one(key_sets)
                assert len(keys) > 0, R('Empty object', field)
                assert '' not in keys, R('Empty property name', field)
                value_types_by_key = defaultdict(set)
                for value in values:
                    for k, v in value.items():
                        if v is not None:
                            value_types_by_key[k].add(type(v))
                num_value_types = set(map(len, value_types_by_key.values()))
                assert num_value_types == {1}, R('Inconsistent property values', field)
                unique_values = set(map(frozenset, map(dict.items, values)))
                assert len(unique_values) == len(values), R('Duplicate objects', field)
            elif value_type is list:
                ranges: set[tuple[int] | tuple[float] | tuple[str] | None] = set()
                for value in values:
                    if value is None:
                        ranges.add(value)
                    else:
                        validate_range(field, value)
                        ranges.add(tuple(value))
                assert len(ranges) == len(values), R('Duplicate values', field)
            else:
                assert len(set(values)) == len(values), R('Duplicate values', field)
        elif operator in {'within', 'intersects', 'contains'}:
            range_filters = WithinFilterJSON | IntersectsFilterJSON | ContainsFilterJSON
            assert check_type(range_filters, filter), R('Invalid filter values', field)
            expected_lengths = (1, 2) if operator == 'contains' else (1,)
            assert len(value_types) in expected_lengths, field
            ranges, primitives = set(), set()
            for value in values:
                if isinstance(value, list):
                    validate_range(field, value)
                    ranges.add(tuple(value))
                else:
                    assert operator == 'contains'
                    primitives.add(value)
            assert len(ranges) + len(primitives) == len(values), R('Duplicate values', field)
        else:
            assert False, R('Invalid operator', field, operator)

    assert is_filters_json(filters)
    return filters


def normalize_filters(filters: FiltersJSON) -> FiltersJSON:
    """
    >>> from azul.lib.functions import compose
    >>> validate_and_normalize = compose(normalize_filters, validate_filters)

    >>> validate_and_normalize({'x': {'intersects': [[3, 4], [1, 2], [1, 1]]}})
    {'x': {'intersects': [[1, 1], [1, 2], [3, 4]]}}

    The is_not operator behaves like is with respect to sorting and validation.

    >>> validate_and_normalize({'x': {'is_not': [3, 1, None, 2]}})
    {'x': {'is_not': [None, 1, 2, 3]}}

    Contains supports scalars, ranges, or a mix of both, all sorted.

    >>> validate_and_normalize({'x': {'contains': [3, 1, 2]}})
    {'x': {'contains': [1, 2, 3]}}

    >>> validate_and_normalize({'x': {'contains': [[3, 4], [1, 2]]}})
    {'x': {'contains': [[1, 2], [3, 4]]}}

    >>> validate_and_normalize({'x': {'contains': [3, [3, 4], 1, [1, 2]]}})
    {'x': {'contains': [1, [1, 2], 3, [3, 4]]}}

    >>> validate_and_normalize({'x': {'is': [None]}})
    {'x': {'is': [None]}}

    Values are sorted.

    >>> validate_and_normalize({'x': {'is': [2, 1, None]}})
    {'x': {'is': [None, 1, 2]}}

    The entries in value dictionaries are sorted by key.

    >>> validate_and_normalize({'x': {'is': [{'b': 2, 'a': 1}]}})
    {'x': {'is': [{'a': 1, 'b': 2}]}}

    Value dictionaries are sorted by their values, in order of the key. If two
    dictionaries have equal values at the first key, the value at the second key
    is used as a tie breaker and so on.

    >>> validate_and_normalize({'x': {'is': [
    ...     {'b': 2, 'a': 1},
    ...     {'a': 0, 'b': 3},
    ...     {'b': None, 'a': 1}
    ... ]}})
    {'x': {'is': [{'a': 0, 'b': 3}, {'a': 1, 'b': None}, {'a': 1, 'b': 2}]}}

    >>> validate_and_normalize({'x':{'is': [[3, 4], None, [1, 2]]}})
    {'x': {'is': [None, [1, 2], [3, 4]]}}

    >>> validate_and_normalize({'x':{'is': [['b', 'c'], ['a', 'b']]}})
    {'x': {'is': [['a', 'b'], ['b', 'c']]}}

    >>> validate_and_normalize({'x':{'is': [['b', 'b'], ['a', 'a']]}})
    {'x': {'is': [['a', 'a'], ['b', 'b']]}}

    Ranges are sorted by start and end value.

    >>> validate_and_normalize({'x': {'within': [[3, 4], [1, 2], [1, 1]]}})
    {'x': {'within': [[1, 1], [1, 2], [3, 4]]}}

    Overall, filters are sorted by field name.

    >>> validate_and_normalize({'y': {'within': [[1, 2]]}, 'x': {'is': [4, 3]}})
    {'x': {'is': [3, 4]}, 'y': {'within': [[1, 2]]}}

    >>> validate_and_normalize({'x': {'intersects': [['a', 'b'], ['', ' ']]}})
    {'x': {'intersects': [['', ' '], ['a', 'b']]}}

    >>> validate_and_normalize({'x': {'contains': [['', ''], '']}})
    {'x': {'contains': ['', ['', '']]}}
    """

    def key(v):
        if v is None:
            return ()
        elif type(v) is dict:
            # The values are primitive so we just need to handle None values
            # and "freeze" the iterable of entries.
            return tuple((k, key(v)) for k, v in sorted(v.items()))
        elif type(v) is list:
            return tuple(v)
        else:
            return v,

    def sort_value(v):
        return dict(sorted(v.items())) if isinstance(v, Mapping) else v

    filters_: _FilterJSON = {
        field: {
            operator: sorted(map(sort_value, values), key=key)
            for operator, values in filter.items()
        }
        for field, filter in sorted(_upcast_filters(filters).items())
    }

    assert is_filters_json(filters_)
    return filters_


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class Filters:
    explicit: FiltersJSON
    source_ids: set[str]

    @classmethod
    def from_json(cls, json: AnyJSON) -> Self:
        """
        Deserialize an instance of this class without reifying it.
        """
        json = json_mapping(json)
        return cls(explicit=validate_filters(json['explicit']),
                   source_ids=set(json_element_strings(json['source_ids'])))

    def to_json(self) -> JSON:
        """
        The inverse of :py:meth:`from_json`.
        """
        return {
            'explicit': _upcast_filters(self.explicit),
            'source_ids': sorted(self.source_ids)
        }

    def update_explicit(self, filters: FiltersJSON) -> Self:
        return attr.evolve(self, explicit={**self.explicit, **filters})

    def reify(self,
              plugin: MetadataPlugin,
              *,
              limit_access: bool = True
              ) -> FiltersJSON:
        """
        Combine the explicit filters passed in by clients with the implicit ones
        representing additional restrictions such as which sources are
        accessible to clients.

        :param plugin: Metadata plugin for the current request's catalog

        :param limit_access: Whether to enforce data access controls by
                             inserting an implicit filter on the source ID facet
        """
        filters = copy.copy(_upcast_filters_unsafe(self.explicit))
        special_fields = plugin.special_fields

        def extract_filter[T](field: str, *, default: set | T) -> set | T:
            filter = filters.pop(field, {})
            # Other operators are not supported on string or boolean fields
            assert filter.keys() <= {'is'}, filter
            try:
                values = filter['is']
            except KeyError:
                return default
            else:
                return set(values)

        source_id_name = special_fields.source_id.name
        explicit_sources = extract_filter(source_id_name, default=None)
        accessible_name = special_fields.accessible.name
        accessible = extract_filter(accessible_name, default={False, True})
        source_operator = 'is'
        sources: set | list | None

        if limit_access:
            if explicit_sources is None:
                sources = self.source_ids if True in accessible else []
            else:
                forbidden_sources = explicit_sources - self.source_ids
                if forbidden_sources:
                    raise ForbiddenError('Cannot filter by inaccessible sources',
                                         forbidden_sources)
                else:
                    sources = explicit_sources if True in accessible else []
        else:
            if accessible == set():
                sources = []
            elif accessible == {False, True}:
                sources = explicit_sources
            elif accessible == {True}:
                if explicit_sources is None:
                    sources = self.source_ids
                else:
                    sources = self.source_ids & explicit_sources
            elif accessible == {False}:
                if explicit_sources is None:
                    sources = self.source_ids
                    source_operator = 'is_not'
                else:
                    sources = explicit_sources - self.source_ids
            else:
                assert False, accessible

        if sources is None:
            assert limit_access is False, limit_access
        else:
            filters[source_id_name] = {source_operator: sorted(sources)}

        if limit_access:
            assert set(filters[source_id_name]['is']) <= self.source_ids

        assert is_filters_json(filters)
        return filters
