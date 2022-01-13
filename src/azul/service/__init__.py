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

from azul import (
    CatalogName,
)
from azul.json import (
    copy_json,
)
from azul.types import (
    LambdaContext,
    PrimitiveJSON,
)

FiltersJSON = Mapping[str, Mapping[str, Sequence[PrimitiveJSON]]]
MutableFiltersJSON = MutableMapping[str, MutableMapping[str, List[PrimitiveJSON]]]


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class Filters:
    explicit: FiltersJSON
    source_ids: Set[str]

    def reify(self, *, explicit_only: bool) -> FiltersJSON:
        if explicit_only:
            return self.explicit
        else:
            filters = copy_json(self.explicit)
            self._add_implicit_source_filter(filters)
            return filters

    def _add_implicit_source_filter(self, filters: MutableFiltersJSON) -> MutableFiltersJSON:
        # We can safely ignore the `within`, `contains`, and `intersects`
        # operators since these always return empty results when used with
        # string fields.
        source_ids = self.source_ids
        explicit_source_ids = filters.setdefault('sourceId', {}).get('is')
        if explicit_source_ids is not None:
            source_ids = self.source_ids.intersection(explicit_source_ids)
        filters['sourceId']['is'] = list(source_ids)
        return filters


class MutableFilters(Filters):
    explicit: MutableFiltersJSON

    def reify(self, *, explicit_only: bool) -> MutableFiltersJSON:
        filters = copy_json(self.explicit)
        if not explicit_only:
            self._add_implicit_source_filter(filters)
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
