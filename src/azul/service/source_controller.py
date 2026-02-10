import logging

from chalice import (
    TooManyRequestsError,
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
    AppController,
    BadGatewayError,
    TerraTimeoutError,
)
from azul.http import (
    LimitedTimeoutException,
    TooManyRequestsException,
)
from azul.service.source_service import (
    SourceService,
)
from azul.types import (
    JSONs,
)

log = logging.getLogger(__name__)


class SourceController(AppController):

    @cached_property
    def _source_service(self) -> SourceService:
        return SourceService()

    def list_sources(self,
                     catalog: CatalogName,
                     authentication: Authentication | None
                     ) -> JSONs:
        try:
            sources = self._source_service.list_accessible_sources(catalog, authentication)
        except PermissionError:
            raise UnauthorizedError
        except LimitedTimeoutException as e:
            raise TerraTimeoutError(*e.args)
        except TooManyRequestsException as e:
            raise TooManyRequestsError(*e.args)
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
                         authentication: Authentication | None
                         ) -> set[str]:
        try:
            source_ids = self._source_service.list_accessible_source_ids(catalog,
                                                                         authentication)
        except PermissionError:
            raise UnauthorizedError
        except LimitedTimeoutException as e:
            raise TerraTimeoutError(*e.args)
        except TooManyRequestsException as e:
            raise TooManyRequestsError(*e.args)
        else:
            return source_ids
