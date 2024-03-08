from collections.abc import (
    Mapping,
    Sequence,
)
import json
import logging
from typing import (
    Optional,
    Protocol,
    TypedDict,
)

import attr
from chalice import (
    ForbiddenError,
)

from azul import (
    CatalogName,
    mutable_furl,
)
from azul.chalice import (
    AppController,
)
from azul.json import (
    copy_json,
)
from azul.plugins import (
    MetadataPlugin,
)
from azul.types import (
    FlatJSON,
    JSON,
    PrimitiveJSON,
)

# We can't express that these are actually pairs. We could, using tuples, but
# those are not JSON, generally speaking, even though the `json` module supports
# serializing them by default.
FilterRange = Sequence[int] | Sequence[float] | Sequence[str]

# `is` is a reserved keyword so we can't use the class-based syntax for
# TypedDict, but have to use the constructor-based one instead. We don't
# currently represent the mutual exclusivity of the operators. We could, as a
# union of singleton TypeDict subclasses, but PyCharm doesn't support that.
#
FilterOperator = TypedDict(
    'FilterOperator',
    {
        'is': list[PrimitiveJSON | FlatJSON],
        'is_not': list[PrimitiveJSON | FlatJSON],
        'intersects': Sequence[FilterRange],
        'contains': Sequence[FilterRange | int | float | str],
        'within': Sequence[FilterRange],
    },
    total=False
)

FiltersJSON = Mapping[str, FilterOperator]


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class Filters:
    explicit: FiltersJSON
    source_ids: set[str]

    @classmethod
    def from_json(cls, json: JSON) -> 'Filters':
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

    def update(self, filters: FiltersJSON) -> 'Filters':
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
        filters = copy_json(self.explicit)
        special_fields = plugin.special_fields

        def extract_filter(field: str, *, default: set | None) -> set | None:
            filter = filters.pop(field, {})
            # Other operators are not supported on string or boolean fields
            assert filter.keys() <= {'is'}, filter
            try:
                values = filter['is']
            except KeyError:
                return default
            else:
                return set(values)

        explicit_sources = extract_filter(special_fields.source_id, default=None)
        accessible = extract_filter(special_fields.accessible, default={False, True})
        source_relation = 'is'

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
                    source_relation = 'is_not'
                else:
                    sources = explicit_sources - self.source_ids
            else:
                assert False, accessible

        if sources is None:
            assert limit_access is False, limit_access
        else:
            filters[special_fields.source_id] = {source_relation: sorted(sources)}

        if limit_access:
            assert set(filters[special_fields.source_id]['is']) <= self.source_ids

        return filters


log = logging.getLogger(__name__)


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


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class ServiceAppController(AppController):
    file_url_func: FileUrlFunc

    def _parse_filters(self, filters: Optional[str]) -> FiltersJSON:
        """
        Parses a string with Azul filters in JSON syntax. Handles default cases
        where filters are None or '{}'.
        """
        if filters is None:
            return {}
        else:
            return json.loads(filters)
