from typing import (
    Optional,
    Set,
)

from chalice import (
    UnauthorizedError,
)

from azul import (
    CatalogName,
    cached_property,
)
from azul.auth import (
    Authentication,
)
from azul.service import (
    Controller,
    MutableFilters,
)
from azul.service.source_service import (
    SourceService,
)
from azul.types import (
    JSONs,
)


class SourceController(Controller):

    @cached_property
    def _source_service(self) -> SourceService:
        return SourceService()

    def list_sources(self,
                     catalog: CatalogName,
                     authentication: Optional[Authentication]
                     ) -> JSONs:
        try:
            sources = self._source_service.list_sources(catalog, authentication)
            return [
                {'sourceId': source.id, 'sourceSpec': str(source.spec)}
                for source in sources
            ]
        except PermissionError:
            raise UnauthorizedError

    def _list_source_ids(self,
                         catalog: CatalogName,
                         authentication: Optional[Authentication]
                         ) -> Set[str]:
        sources = self.list_sources(catalog, authentication)
        return {source['sourceId'] for source in sources}

    def get_filters(self,
                    catalog: CatalogName,
                    authentication: Authentication,
                    filters: Optional[str] = None
                    ) -> MutableFilters:
        return MutableFilters(explicit=self._parse_filters(filters),
                              source_ids=self._list_source_ids(catalog, authentication))
