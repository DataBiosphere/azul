from typing import (
    Optional,
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
from azul.chalice import (
    ServiceUnavailableError,
)
from azul.service import (
    Filters,
    ServiceAppController,
)
from azul.service.source_service import (
    SourceService,
)
from azul.terra import (
    TerraTimeoutException,
)
from azul.types import (
    JSONs,
)


class SourceController(ServiceAppController):

    @cached_property
    def _source_service(self) -> SourceService:
        return SourceService()

    def list_sources(self,
                     catalog: CatalogName,
                     authentication: Optional[Authentication]
                     ) -> JSONs:
        try:
            sources = self._source_service.list_sources(catalog, authentication)
        except PermissionError:
            raise UnauthorizedError
        except TerraTimeoutException as e:
            raise ServiceUnavailableError(*e.args)
        else:
            return [
                {'sourceId': source.id, 'sourceSpec': str(source.spec)}
                for source in sources
            ]

    def _list_source_ids(self,
                         catalog: CatalogName,
                         authentication: Optional[Authentication]
                         ) -> set[str]:
        sources = self.list_sources(catalog, authentication)
        return {source['sourceId'] for source in sources}

    def get_filters(self,
                    catalog: CatalogName,
                    authentication: Optional[Authentication],
                    filters: Optional[str] = None
                    ) -> Filters:
        return Filters(explicit=self._parse_filters(filters),
                       source_ids=self._list_source_ids(catalog, authentication))
