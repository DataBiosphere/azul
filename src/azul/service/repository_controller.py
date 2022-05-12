from collections.abc import (
    Mapping,
    Sequence,
)
import json
import logging
import time
from typing import (
    Optional,
    cast,
)

from chalice import (
    BadRequestError,
    NotFoundError,
)

from azul import (
    CatalogName,
    cache,
    cached_property,
    config,
    reject,
)
from azul.auth import (
    Authentication,
)
from azul.indexer.document import (
    FieldType,
)
from azul.plugins import (
    RepositoryPlugin,
)
from azul.service import (
    BadArgumentException,
)
from azul.service.elasticsearch_service import (
    IndexNotFoundError,
    Pagination,
)
from azul.service.repository_service import (
    EntityNotFoundError,
    RepositoryService,
)
from azul.service.source_controller import (
    SourceController,
)
from azul.types import (
    JSON,
)
from azul.uuids import (
    InvalidUUIDError,
)

log = logging.getLogger(__name__)


class RepositoryController(SourceController):

    @cached_property
    def service(self) -> RepositoryService:
        return RepositoryService()

    @classmethod
    @cache
    def repository_plugin(cls, catalog: CatalogName) -> RepositoryPlugin:
        return RepositoryPlugin.load(catalog).create(catalog)

    def search(self,
               *,
               catalog: CatalogName,
               entity_type: str,
               item_id: Optional[str],
               filters: Optional[str],
               pagination: Pagination,
               authentication: Authentication
               ) -> JSON:
        filters = self.get_filters(catalog, authentication, filters)
        try:
            response = self.service.search(catalog=catalog,
                                           entity_type=entity_type,
                                           file_url_func=self.file_url_func,
                                           item_id=item_id,
                                           filters=filters,
                                           pagination=pagination)
        except (BadArgumentException, InvalidUUIDError) as e:
            raise BadRequestError(e)
        except (EntityNotFoundError, IndexNotFoundError) as e:
            raise NotFoundError(e)
        return cast(JSON, response)

    def summary(self,
                *,
                catalog: CatalogName,
                filters: str,
                authentication: Authentication
                ) -> JSON:
        filters = self.get_filters(catalog, authentication, filters)
        try:
            response = self.service.summary(catalog, filters)
        except BadArgumentException as e:
            raise BadRequestError(e)
        return cast(JSON, response)

    def _parse_range_request_header(self,
                                    range_specifier: str
                                    ) -> Sequence[tuple[Optional[int], Optional[int]]]:
        """
        >>> # noinspection PyTypeChecker
        >>> rc = RepositoryController(lambda_context=None, file_url_func=None)
        >>> rc._parse_range_request_header('bytes=100-200,300-400')
        [(100, 200), (300, 400)]

        >>> rc._parse_range_request_header('bytes=-100')
        [(None, 100)]

        >>> rc._parse_range_request_header('bytes=100-')
        [(100, None)]

        >>> rc._parse_range_request_header('foo=100')
        []

        >>> rc._parse_range_request_header('')
        Traceback (most recent call last):
        ...
        chalice.app.BadRequestError: Invalid range specifier ''

        >>> rc._parse_range_request_header('100-200')
        Traceback (most recent call last):
        ...
        chalice.app.BadRequestError: Invalid range specifier '100-200'

        >>> rc._parse_range_request_header('bytes=')
        Traceback (most recent call last):
        ...
        chalice.app.BadRequestError: Invalid range specifier 'bytes='

        >>> rc._parse_range_request_header('bytes=100')
        Traceback (most recent call last):
        ...
        chalice.app.BadRequestError: Invalid range specifier 'bytes=100'

        >>> rc._parse_range_request_header('bytes=-')
        Traceback (most recent call last):
        ...
        chalice.app.BadRequestError: Invalid range specifier 'bytes=-'

        >>> rc._parse_range_request_header('bytes=--')
        Traceback (most recent call last):
        ...
        chalice.app.BadRequestError: Invalid range specifier 'bytes=--'
        """

        def to_int_or_none(value: str) -> Optional[int]:
            return None if value == '' else int(value)

        parsed_ranges = []
        try:
            unit, ranges = range_specifier.split('=')
            if unit == 'bytes':
                for range_spec in ranges.split(','):
                    start, end = range_spec.split('-')
                    reject(start == '' and end == '', 'Empty range')
                    parsed_ranges.append((to_int_or_none(start), to_int_or_none(end)))
            else:
                reject(unit == '', 'Empty range unit')
        except Exception as e:
            raise BadRequestError(f'Invalid range specifier {range_specifier!r}') from e
        return parsed_ranges

    def download_file(self,
                      catalog: CatalogName,
                      fetch: bool,
                      file_uuid: str,
                      query_params: Mapping[str, str],
                      headers: Mapping[str, str],
                      authentication: Optional[Authentication]
                      ):
        file_version = query_params.get('version')
        replica = query_params.get('replica')
        file_name = query_params.get('fileName')
        drs_path = query_params.get('drsPath')
        wait = query_params.get('wait')
        request_index = int(query_params.get('requestIndex', '0'))
        token = query_params.get('token')

        plugin = self.repository_plugin(catalog)
        download_cls = plugin.file_download_class()

        if request_index == 0:
            file = self.service.get_data_file(catalog=catalog,
                                              file_uuid=file_uuid,
                                              file_version=file_version,
                                              filters=self.get_filters(catalog, authentication, None))
            if file is None:
                raise NotFoundError(f'Unable to find file {file_uuid!r}, '
                                    f'version {file_version!r} in catalog {catalog!r}')
            file_version = file['version']
            drs_path = file['drs_path']
            file_size = file['size']
            if file_name is None:
                file_name = file['name']
        else:
            file_size = None
            assert file_version is not None
            assert file_name is not None

        # Due to https://github.com/curl/curl/issues/6740 causing curl to error
        # when trying to resume a previously completed file download, we check
        # for a range request starting at the end of the file and instead of
        # a returning a 416 (Range Not Satisfiable) as specified in RFC7233
        # https://tools.ietf.org/html/rfc7233#section-4.4 we return a 206
        # (Partial Content) with an empty body.
        try:
            range_specifier = headers['range']
        except KeyError:
            pass
        else:
            requested_range = self._parse_range_request_header(range_specifier)
            if requested_range == [(file_size, None)]:
                return {
                    'Status': 206,
                    'Content-Length': 0
                }

        download = download_cls(file_uuid=file_uuid,
                                file_name=file_name,
                                file_version=file_version,
                                drs_path=drs_path,
                                replica=replica,
                                token=token)

        download.update(plugin, authentication)
        if download.retry_after is not None:
            retry_after = min(download.retry_after, int(1.3 ** request_index))
            query_params = {
                'version': download.file_version,
                'fileName': download.file_name,
                'requestIndex': request_index + 1
            }
            if download.drs_path is not None:
                query_params['drsPath'] = download.drs_path
            if download.token is not None:
                query_params['token'] = download.token
            if download.replica is not None:
                query_params['replica'] = download.replica
            if wait is not None:
                if wait == '0':
                    pass
                elif wait == '1':
                    # Sleep in the lambda but ensure that we wake up before it
                    # runs out of execution time (and before API Gateway times
                    # out) so we get a chance to return a response to the client
                    remaining_time = self.lambda_context.get_remaining_time_in_millis() / 1000
                    server_side_sleep = min(float(retry_after),
                                            remaining_time - config.api_gateway_timeout_padding - 3)
                    time.sleep(server_side_sleep)
                    retry_after = round(retry_after - server_side_sleep)
                else:
                    assert False, wait
                query_params['wait'] = wait
            return {
                'Status': 301,
                **({'Retry-After': retry_after} if retry_after else {}),
                'Location': str(self.file_url_func(catalog=catalog,
                                                   file_uuid=file_uuid,
                                                   fetch=fetch,
                                                   **query_params))
            }
        elif download.location is not None:
            log_data = {
                'file_name': file_name,
                'file_uuid': file_uuid,
                'file_version': file_version,
                'catalog': catalog,
                'fetch': fetch,
                **{
                    k: headers.get(k)
                    for k in ('range', 'host', 'user-agent', 'x-forwarded-for')
                }
            }
            log.info('Download of file %s', json.dumps(log_data))
            return {
                'Status': 302,
                'Location': str(download.location)
            }
        else:
            assert download.drs_path is None, download
            raise NotFoundError(f'File {file_uuid!r} with version {file_version!r} '
                                f'was found in catalog {catalog!r}, however no download is currently available')

    @cache
    def field_types(self, catalog: CatalogName) -> Mapping[str, FieldType]:
        """
        Returns the field type for each supported sort and filter field, using
        the name of the field as provided by clients.
        """
        result = {}
        for field, path in self.service.metadata_plugin(catalog).field_mapping.items():
            field_type = self.service.field_type(catalog, path)
            if isinstance(field_type, FieldType):
                result[field] = field_type
        return result
