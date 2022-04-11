import json
import logging
from typing import (
    List,
    Mapping,
    MutableMapping,
    Optional,
    Protocol,
    Sequence,
    Set,
)

import attr
from chalice import (
    ForbiddenError,
)

from azul import (
    CatalogName,
)
from azul.json import (
    copy_json,
)
from azul.plugins import (
    ServiceConfig,
)
from azul.types import (
    JSON,
    LambdaContext,
    PrimitiveJSON,
)

FiltersJSON = Mapping[str, Mapping[str, Sequence[PrimitiveJSON]]]
MutableFiltersJSON = MutableMapping[str, MutableMapping[str, List[PrimitiveJSON]]]


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class Filters:
    explicit: FiltersJSON
    source_ids: Set[str]

    def reify(self,
              service_config: ServiceConfig,
              *,
              explicit_only: bool
              ) -> FiltersJSON:
        if explicit_only:
            return self.explicit
        else:
            filters = copy_json(self.explicit)
            self._add_implicit_source_filter(filters, service_config.source_id_field)
            return filters

    def to_json(self):
        return {
            'explicit': self.explicit,
            'source_ids': sorted(self.source_ids)
        }

    @classmethod
    def from_json(cls, json: JSON) -> 'Filters':
        return cls(explicit=json['explicit'],
                   source_ids=set(json['source_ids']))

    def _add_implicit_source_filter(self,
                                    filters: MutableFiltersJSON,
                                    source_id_facet: str
                                    ) -> MutableFiltersJSON:
        # We can safely ignore the `within`, `contains`, and `intersects`
        # operators since these always return empty results when used with
        # string fields.
        facet_filter = filters.setdefault(source_id_facet, {})
        try:
            requested_source_ids = facet_filter['is']
        except KeyError:
            facet_filter['is'] = list(self.source_ids)
        else:
            inaccessible = set(requested_source_ids) - self.source_ids
            if inaccessible:
                raise ForbiddenError(f'Cannot filter by inaccessible sources: {inaccessible!r}')
        assert set(filters[source_id_facet]['is']) <= self.source_ids
        return filters


class MutableFilters(Filters):
    explicit: MutableFiltersJSON

    def reify(self,
              service_config: ServiceConfig,
              *,
              explicit_only: bool
              ) -> FiltersJSON:
        filters = copy_json(self.explicit)
        if not explicit_only:
            self._add_implicit_source_filter(filters, service_config.source_id_field)
        return filters


logger = logging.getLogger(__name__)


class BadArgumentException(Exception):

    def __init__(self, message):
        super().__init__(message)


class AbstractService:
    pass


class FileUrlFunc(Protocol):

    def __call__(self,
                 *,
                 catalog: CatalogName,
                 file_uuid: str,
                 fetch: bool = True,
                 **params: str) -> str: ...


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class Controller:
    lambda_context: LambdaContext
    file_url_func: FileUrlFunc

    def _parse_filters(self, filters: Optional[str]) -> MutableFiltersJSON:
        """
        Parses a string with Azul filters in JSON syntax. Handles default cases
        where filters are None or '{}'.
        """
        if filters is None:
            return {}
        else:
            return json.loads(filters)
