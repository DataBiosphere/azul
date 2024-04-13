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
        return cls(explicit=json['explicit'],
                   source_ids=set(json['source_ids']))

    def to_json(self) -> JSON:
        return {
            'explicit': self.explicit,
            'source_ids': sorted(self.source_ids)
        }

    def update(self, filters: FiltersJSON) -> 'Filters':
        return attr.evolve(self, explicit={**self.explicit, **filters})

    def reify(self, plugin: MetadataPlugin) -> FiltersJSON:
        filters = copy_json(self.explicit)
        # We can safely ignore the `within`, `contains`, and `intersects`
        # operators since these always return empty results when used with
        # string fields.
        special_fields = plugin.special_fields
        facet_filter = filters.setdefault(special_fields.source_id, {})
        try:
            requested_source_ids = facet_filter['is']
        except KeyError:
            facet_filter['is'] = list(self.source_ids)
        else:
            inaccessible = set(requested_source_ids) - self.source_ids
            if inaccessible:
                raise ForbiddenError(f'Cannot filter by inaccessible sources: {inaccessible!r}')
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
