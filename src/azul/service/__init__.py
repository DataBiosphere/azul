from collections import (
    defaultdict,
)
from functools import (
    partial,
)
from itertools import (
    chain,
)
import json
import logging
from typing import (
    Protocol,
    Self,
    TypedDict,
    cast,
    get_args,
)

import attr
from chalice import (
    ForbiddenError,
)
from more_itertools import (
    one,
    only,
)

from azul import (
    CatalogName,
    R,
    mutable_furl,
)
from azul.json import (
    copy_json,
)
from azul.plugins import (
    FieldName,
    MetadataPlugin,
)
from azul.types import (
    FlatJSON,
    JSON,
    PrimitiveJSON,
    reify,
)

log = logging.getLogger(__name__)

# We can't express that these are actually pairs, i.e. lists of length 2. We
# could, using tuples, but those are not JSON, even though the `json` module
# supports serializing them by default.
#
type FilterRange = list[int] | list[float] | list[str]
type FilterRangeEnd = int | float | str

# `is` is a reserved keyword so we can't use the class-based syntax for
# TypedDict, but have to use the constructor-based one instead. We don't
# currently represent the mutual exclusivity of the operators. We could, as a
# union of singleton TypeDict subclasses, but PyCharm doesn't support that.
#
FilterJSON = TypedDict(
    'FilterJSON',
    {
        'is': list[PrimitiveJSON | FlatJSON],
        'is_not': list[PrimitiveJSON | FlatJSON],
        'intersects': list[FilterRange],
        'contains': list[FilterRange | FilterRangeEnd],
        'within': list[FilterRange],
    },
    total=False
)

type FiltersJSON = dict[FieldName, FilterJSON]

_filter_operators = FilterJSON.__optional_keys__
_simple_filter_value_types = reify(PrimitiveJSON)
_dict_filter_value_types = reify(get_args(FlatJSON.__value__)[1])
assert _simple_filter_value_types == _dict_filter_value_types
_filter_range_end_types = reify(FilterRangeEnd)


def parse_filters(raw_filters: str | None) -> FiltersJSON:
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

    >>> parse_filters('{"x":{"is":[null]}}')
    {'x': {'is': [None]}}

    Values are sorted.

    >>> parse_filters('{"x":{"is":[2,1,null]}}')
    {'x': {'is': [None, 1, 2]}}

    The entries in value dictionaries are sorted by key.

    >>> parse_filters('{"x":{"is":[{"b":2,"a":1}]}}')
    {'x': {'is': [{'a': 1, 'b': 2}]}}

    Value dictionaries are sorted by their values, in order of the key. If two
    dictionaries have equal values at the first key, the value at the second key
    is used as a tie breaker and so on.

    >>> parse_filters('{"x":{"is":[{"b":2,"a":1},{"a":0,"b":3},{"b":null,"a":1}]}}')
    {'x': {'is': [{'a': 0, 'b': 3}, {'a': 1, 'b': None}, {'a': 1, 'b': 2}]}}

    Ranges are sorted by start and end value.

    >>> parse_filters('{"x":{"within":[[3,4],[1,2],[1,1]]}}')
    {'x': {'within': [[1, 1], [1, 2], [3, 4]]}}

    Overall, filters are sorted by field name.

    >>> parse_filters('{"y":{"within":[[1,2]]},"x":{"is":[4, 3]}}')
    {'x': {'is': [3, 4]}, 'y': {'within': [[1, 2]]}}

    >>> parse_filters('[]')
    Traceback (most recent call last):
        ...
    AssertionError: R('Filters must be an object')

    >>> parse_filters('{"":42}')
    Traceback (most recent call last):
        ...
    AssertionError: R('Empty field name')

    >>> parse_filters('{"x":42}')
    Traceback (most recent call last):
        ...
    AssertionError: R('Filter must be an object', 'x')

    >>> parse_filters('{"x":{}}')
    Traceback (most recent call last):
        ...
    AssertionError: R('Need exactly one filter per field', 'x')

    >>> parse_filters('{"x":{"is":[1],"contains":[1]}}')
    Traceback (most recent call last):
        ...
    AssertionError: R('Need exactly one filter per field', 'x')

    >>> parse_filters('{"x":{"foo":[2]}}')
    Traceback (most recent call last):
        ...
    AssertionError: R('Invalid operator', 'x', 'foo')

    >>> parse_filters('{"x":{"is":[]}}')
    Traceback (most recent call last):
        ...
    AssertionError: R('Need at least one value', 'x')

    >>> parse_filters('{"x":{"is":[1,1]}}')
    Traceback (most recent call last):
        ...
    AssertionError: R('Duplicate values', 'x')

    >>> parse_filters('{"x":{"within":[1]}}')
    Traceback (most recent call last):
        ...
    AssertionError: R('Value does not match operator', 'x', <class 'int'>, 'within')

    >>> parse_filters('{"x":{"is":[42,4.1]}}')
    Traceback (most recent call last):
        ...
    AssertionError: R('Inconsistent value types', 'x')

    >>> parse_filters('{"x":{"within":[[1]]}}')
    Traceback (most recent call last):
        ...
    AssertionError: R('Range must be list of length 2', 'x')

    >>> parse_filters('{"x":{"within":[[2,1]]}}')
    Traceback (most recent call last):
        ...
    AssertionError: R('Range is inverted', 'x')

    >>> parse_filters('{"x":{"within":[[1,2],["",""]]}}')
    Traceback (most recent call last):
        ...
    AssertionError: R('Inconsistent range ends', 'x')

    >>> parse_filters('{"x":{"within":[[1,1.1],[2,2.2]]}}')
    Traceback (most recent call last):
        ...
    AssertionError: R('Inconsistent range ends', 'x')

    >>> parse_filters('{"x":{"within":[[false,true]]}}')
    Traceback (most recent call last):
        ...
    AssertionError: R('Invalid range end', 'x')

    >>> parse_filters('{"x":{"within":[{}]}}')
    Traceback (most recent call last):
        ...
    AssertionError: R('Value does not match operator', 'x', <class 'dict'>, 'within')

    >>> parse_filters('{"x":{"within":[[1,2],[1,2]]}}')
    Traceback (most recent call last):
        ...
    AssertionError: R('Duplicate ranges', 'x')

    >>> parse_filters('{"x":{"is":[{}]}}')
    Traceback (most recent call last):
        ...
    AssertionError: R('Empty object', 'x')

    >>> parse_filters('{"x":{"is":[{"":1}]}}')
    Traceback (most recent call last):
        ...
    AssertionError: R('Empty property name', 'x')

    >>> parse_filters('{"x":{"is":[{"y":1},{"z":2}]}}')
    Traceback (most recent call last):
        ...
    AssertionError: R('Inconsistent property names', 'x')

    >>> parse_filters('{"x":{"is":[{"y":1,"z":[]}]}}')
    Traceback (most recent call last):
        ...
    AssertionError: R('Invalid property value', 'x')

    >>> parse_filters('{"x":{"is":[{"y":1,"z":2},{"y":"","z":3}]}}')
    Traceback (most recent call last):
        ...
    AssertionError: R('Inconsistent property values', 'x')

    >>> parse_filters('{"x":{"is":[{"a":1,"b":2},{"b":2,"a":1}]}}')
    Traceback (most recent call last):
        ...
    AssertionError: R('Duplicate objects', 'x')
    """
    if raw_filters is None:
        return {}
    else:
        filters = json.loads(raw_filters)
        assert type(filters) is dict, R('Filters must be an object')
        for field, filter in filters.items():
            assert len(field) > 0, R('Empty field name')
            assert type(filter) is dict, R('Filter must be an object', field)
            assert len(filter) == 1, R('Need exactly one filter per field', field)
            operator, values = one(filter.items())
            assert operator in _filter_operators, R('Invalid operator', field, operator)
            assert len(values) > 0, R('Need at least one value', field)
            num_value_types = set(map(type, values))
            num_value_types.discard(type(None))
            assert len(num_value_types) < 2, R('Inconsistent value types', field)
            value_type = only(num_value_types)
            mismatch = R('Value does not match operator', field, value_type, operator)
            if value_type is None:
                assert operator in {'is'}, mismatch
            elif value_type in _simple_filter_value_types:
                assert operator in {'is', 'contains'}, mismatch
                assert len(set(values)) == len(values), R('Duplicate values', field)
            elif value_type is list:
                assert operator in {'contains', 'within', 'intersects'}, mismatch
                for range in values:
                    assert len(range) == 2, R('Range must be list of length 2', field)
                    assert range[0] <= range[1], R('Range is inverted', field)
                assert len(set(map(tuple, values))) == len(values), R('Duplicate ranges', field)
                end_types = set(chain.from_iterable(map(partial(map, type), values)))
                assert len(end_types) == 1, R('Inconsistent range ends', field)
                end_type = one(end_types)
                assert end_type in _filter_range_end_types, R('Invalid range end', field)
            elif value_type is dict:
                assert operator == 'is', mismatch
                key_sets = set(map(frozenset, map(dict.keys, values)))
                assert len(key_sets) == 1, R('Inconsistent property names', field)
                keys = one(key_sets)
                assert len(keys) > 0, R('Empty object', field)
                assert '' not in keys, R('Empty property name', field)
                value_types_by_key = defaultdict(set)
                for value in values:
                    for k, v in value.items():
                        assert type(v) in _dict_filter_value_types, R('Invalid property value', field)
                        if v is not None:
                            value_types_by_key[k].add(type(v))
                num_value_types = set(map(len, value_types_by_key.values()))
                assert num_value_types == {1}, R('Inconsistent property values', field)
                # Sort each value dictionary in place by key (and value, but key
                # is already unique). This makes sorting the values and checking
                # their uniqueness easier.
                for value in values:
                    sorted_value = dict(sorted(value.items()))
                    value.clear()
                    value.update(sorted_value)
                unique_values = set(map(tuple, map(dict.items, values)))
                assert len(unique_values) == len(values), R('Duplicate objects', field)
            else:
                assert False, R('Invalid value', field)

            def key(v):
                if v is None:
                    return False, v
                elif type(v) is dict:
                    # The entries in the dict are alteady sorted by key, the
                    # values are primitive so we just need to handle None values
                    # and "freeze" the iterable of entries.
                    return True, tuple((k, key(v)) for k, v in v.items())
                else:
                    return True, v

            values.sort(key=key)

        filters = {k: v for k, v in sorted(filters.items())}

        return cast(FiltersJSON, filters)


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class Filters:
    explicit: FiltersJSON
    source_ids: set[str]

    @classmethod
    def from_json(cls, json: JSON) -> Self:
        """
        Deserialize an instance of this class without reifying it.
        """
        return cls(explicit=json['explicit'],
                   source_ids=set(json['source_ids']))

    def to_json(self) -> JSON:
        """
        The inverse of :py:meth:`from_json`.
        """
        return {
            'explicit': self.explicit,
            'source_ids': sorted(self.source_ids)
        }

    def update(self, filters: FiltersJSON) -> Self:
        return attr.evolve(self, explicit={**self.explicit, **filters})

    def reify_implicit_only(self, plugin: MetadataPlugin) -> FiltersJSON:
        """
        Construct filters for *only* the implicit restriction on accessible
        sources. This is conceptually equivalent to the set difference
        `self.reify(limit_access=True) - self.reify(limit_access=False)`.

        :param plugin: Metadata plugin for the current request's catalog
        """
        source_id_field = plugin.special_fields.source_id.name
        filters = copy_json(self.explicit)
        explicit_sources = self._extract_filter(filters, source_id_field, default=None)
        if explicit_sources is not None:
            self._forbid_explicit_inaccessible_sources(explicit_sources)
        return {
            source_id_field: {
                'is': sorted(self.source_ids),
            }
        }

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
        filters = copy_json(self.explicit)
        special_fields = plugin.special_fields

        explicit_sources = self._extract_filter(filters,
                                                special_fields.source_id.name,
                                                default=None)
        accessible = self._extract_filter(filters,
                                          special_fields.accessible.name,
                                          default={False, True})
        source_relation = 'is'

        if limit_access:
            if explicit_sources is None:
                sources = self.source_ids if True in accessible else []
            else:
                self._forbid_explicit_inaccessible_sources(explicit_sources)
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
                    source_relation = 'is_not'
                else:
                    sources = explicit_sources - self.source_ids
            else:
                assert False, accessible

        if sources is None:
            assert limit_access is False, limit_access
        else:
            filters[special_fields.source_id.name] = {source_relation: sorted(sources)}

        if limit_access:
            assert set(filters[special_fields.source_id.name]['is']) <= self.source_ids

        return filters

    def _extract_filter(self, filters, field: str, *, default: set | None) -> set | None:
        filter = filters.pop(field, {})
        # Other operators are not supported on string or boolean fields
        assert filter.keys() <= {'is'}, filter
        try:
            values = filter['is']
        except KeyError:
            return default
        else:
            return set(values)

    def _forbid_explicit_inaccessible_sources(self, explicit_sources: set[str]):
        forbidden_sources = explicit_sources - self.source_ids
        if forbidden_sources:
            raise ForbiddenError('Cannot filter by inaccessible sources',
                                 forbidden_sources)


class BadArgumentException(Exception):

    def __init__(self, message):
        super().__init__(message)


class FileUrlFunc(Protocol):

    def __call__(self,
                 *,
                 catalog: CatalogName,
                 file_uuid: str,
                 fetch: bool = True,
                 **params: str
                 ) -> mutable_furl: ...
