import logging
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
    BadGatewayError,
    ServiceUnavailableError,
)
from azul.http import (
    LimitedTimeoutException,
)
from azul.service import (
    Filters,
    ServiceAppController,
)
from azul.service.source_service import (
    SourceService,
)
from azul.types import (
    JSONs,
)

log = logging.getLogger(__name__)


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
        except LimitedTimeoutException as e:
            raise ServiceUnavailableError(*e.args)
        else:
            authoritative_source_ids = {source.id for source in sources}
            cached_source_ids = self._list_source_ids(catalog, authentication)
            # For optimized performance, the cache may include source IDs that
            # are accessible but are not configured for indexing. Therefore, we
            # expect the set of actual sources to be a subset of the cached
            # sources.
            diff = authoritative_source_ids - cached_source_ids
            if diff:
                log.debug(diff)
                raise BadGatewayError('Inconsistent response from repository')
            return [
                {'sourceId': source.id, 'sourceSpec': str(source.spec)}
                for source in sources
            ]

    def _list_source_ids(self,
                         catalog: CatalogName,
                         authentication: Optional[Authentication]
                         ) -> set[str]:
        try:
            source_ids = self._source_service.list_source_ids(catalog, authentication)
        except PermissionError:
            raise UnauthorizedError
        except LimitedTimeoutException as e:
            raise ServiceUnavailableError(*e.args)
        else:
            return source_ids

    def get_filters(self,
                    catalog: CatalogName,
                    authentication: Optional[Authentication],
                    filters: Optional[str] = None
                    ) -> Filters:
        return Filters(explicit=self._parse_filters(filters),
                       source_ids=self._list_source_ids(catalog, authentication))
