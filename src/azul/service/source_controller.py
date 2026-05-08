import logging

from chalice.app import (
    TooManyRequestsError,
    UnauthorizedError,
)

from azul import (
    CatalogName,
)
from azul.auth import (
    Authentication,
)
from azul.chalice import (
    BadGatewayError,
    Controller,
    TemporaryRedirectError,
)
from azul.http import (
    LimitedTimeoutException,
    TooManyRequestsException,
)
from azul.lib import (
    cached_property,
)
from azul.lib.types import (
    JSONs,
)
from azul.service.source_service import (
    SourceService,
)

log = logging.getLogger(__name__)


class SourceController(Controller):

    @cached_property
    def _source_service(self) -> SourceService:
        return SourceService()

    def list_sources(self,
                     catalog: CatalogName,
                     authentication: Authentication | None
                     ) -> JSONs:
        try:
            sources = self._source_service.list_sources(catalog, authentication)
        except PermissionError:
            raise UnauthorizedError
        except LimitedTimeoutException as e:
            raise TemporaryRedirectError(*e.args)
        except TooManyRequestsException as e:
            raise TooManyRequestsError(*e.args)
        else:
            authoritative_source_ids = {source.ref.id for source in sources}
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
                {
                    'sourceId': source.ref.id,
                    'sourceSpec': source.ref.spec.to_json(),
                    'sourceConfig': source.config.to_json()
                }
                for source in sources
            ]

    def _list_source_ids(self,
                         catalog: CatalogName,
                         authentication: Authentication | None
                         ) -> set[str]:
        try:
            source_ids = self._source_service.list_source_ids(catalog, authentication)
        except PermissionError:
            raise UnauthorizedError
        except LimitedTimeoutException as e:
            raise TemporaryRedirectError(*e.args)
        except TooManyRequestsException as e:
            raise TooManyRequestsError(*e.args)
        else:
            return source_ids
