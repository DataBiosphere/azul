from collections.abc import (
    Mapping,
    Sequence,
)
import json
import logging
import time
from typing import (
    Any,
    Callable,
    cast,
)

import attr
import attrs
from chalice import (
    BadRequestError,
    NotFoundError,
    TooManyRequestsError,
)

from azul import (
    CatalogName,
    R,
    cache,
    cached_property,
    config,
)
from azul.auth import (
    Authentication,
)
from azul.chalice import (
    ServiceUnavailableError,
)
from azul.collections import (
    adict,
)
from azul.http import (
    LimitedTimeoutException,
    TooManyRequestsException,
)
from azul.indexer.field import (
    FieldType,
    pass_thru_bool,
)
from azul.indexer.mirror_service import (
    BaseMirrorService,
    MirrorFileDownload,
)
from azul.plugins import (
    File,
    RepositoryPlugin,
)
from azul.service import (
    BadArgumentException,
)
from azul.service.app_controller import (
    Mandatory,
    validate_catalog,
    validate_params,
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
    is_optional,
)
from azul.uuids import (
    InvalidUUIDError,
)

log = logging.getLogger(__name__)


class RepositoryController(SourceController):

    @cached_property
    def service(self) -> RepositoryService:
        return RepositoryService()

    @cached_property
    def mirror_service(self) -> BaseMirrorService:
        return BaseMirrorService()

    @cache
    def repository_plugin(self, catalog: CatalogName) -> RepositoryPlugin:
        return RepositoryPlugin.load(catalog).create(catalog)

    def search(self,
               *,
               catalog: CatalogName,
               entity_type: str,
               item_id: str | None,
               filters: str | None,
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
                                    ) -> Sequence[tuple[int | None, int | None]]:
        """
        >>> # noinspection PyTypeChecker
        >>> rc = RepositoryController(app=None, file_url_func=None)
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

        def to_int_or_none(value: str) -> int | None:
            return None if value == '' else int(value)

        parsed_ranges = []
        try:
            unit, ranges = range_specifier.split('=')
            if unit == 'bytes':
                for range_spec in ranges.split(','):
                    start, end = range_spec.split('-')
                    assert start != '' or end != '', R('Empty range')
                    parsed_ranges.append((to_int_or_none(start), to_int_or_none(end)))
            else:
                assert unit != '', R('Empty range unit')
        except Exception as e:
            raise BadRequestError(f'Invalid range specifier {range_specifier!r}') from e
        return parsed_ranges

    def download_file(self,
                      catalog: CatalogName,
                      fetch: bool,
                      file_uuid: str,
                      query_params: Mapping[str, str],
                      headers: Mapping[str, str],
                      authentication: Authentication | None
                      ):

        # Check the catalog in a separate step so that the plugins can be loaded
        # safely, since doing so requires a valid catalog. We need the metadata
        # plugin to know which file parameters to expect, and the repository
        # plugin to validate the file version.
        validate_params(query_params,
                        catalog=validate_catalog,
                        requestIndex=int,
                        allow_extra_params=True)

        request_index = int(query_params.get('requestIndex', '0'))

        validate_params(query_params,
                        catalog=str,
                        requestIndex=int,
                        wait=self._validate_wait,
                        replica=self._validate_replica,
                        token=str,
                        **self._file_param_validators(catalog, request_index))

        file_version = query_params.get('version')
        replica = query_params.get('replica')
        file_name = query_params.get('fileName')
        drs_uri = query_params.get('drsUri')
        wait = query_params.get('wait')
        token = query_params.get('token')

        if request_index == 0:
            file = self.service.get_data_file(catalog=catalog,
                                              file_uuid=file_uuid,
                                              file_version=file_version,
                                              filters=self.get_filters(catalog, authentication, None))
            if file is None:
                raise NotFoundError(f'Unable to find file {file_uuid!r}, '
                                    f'version {file_version!r} in catalog {catalog!r}')
            file = attr.evolve(file, **adict(name=file_name, drs_uri=drs_uri))
        else:
            file = self._file_from_request(catalog, file_uuid, query_params)

        try:
            range_specifier = headers['range']
        except KeyError:
            pass
        else:
            requested_range = self._parse_range_request_header(range_specifier)
            if requested_range == [(file.size, None)]:
                # Due to https://github.com/curl/curl/issues/10521 which causes
                # curl below 8.5.0 to fail when getting a 416 response for an
                # attempt to resume a previously completed file download,
                # instead, we return a 206 along with a `Content-Range` header,
                # which has been confirmed to work for all curl versions tested
                # (7.71.1 through 8.12.1).
                return {
                    'Status': 206,
                    'Content-Length': 0,
                    'Content-Range': f'bytes */{file.size}'
                }

        plugin = self.repository_plugin(catalog)

        is_mirrored = (config.enable_mirroring
                       and self.mirror_service.is_mirrored(catalog, file))
        if is_mirrored:
            download = MirrorFileDownload(
                file=file,
                location=self.mirror_service.get_mirror_url(catalog, file),
                replica=replica,
                token=token
            )
        else:
            download_cls = plugin.file_download_class()
            download = download_cls(file=file, replica=replica, token=token)

        try:
            download.update(plugin, authentication)
        except LimitedTimeoutException as e:
            raise ServiceUnavailableError(*e.args)
        except TooManyRequestsException as e:
            raise TooManyRequestsError(*e.args)
        if download.retry_after is not None:
            retry_after = min(download.retry_after, int(1.3 ** request_index))
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
            query_params = self._file_to_request(download.file) | adict(
                token=download.token,
                replica=download.replica,
                requestIndex=request_index + 1,
                wait=wait
            )
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
                **file.to_json(),
                'catalog': catalog,
                'fetch': fetch,
                **{
                    k: headers.get(k)
                    for k in ('range', 'host', 'user-agent', 'x-forwarded-for')
                }
            }
            log.info('Download of %s file %s',
                     'mirrored' if is_mirrored else 'repository',
                     json.dumps(log_data))
            return {
                'Status': 302,
                'Location': download.location
            }
        else:
            assert download.file.drs_uri is None, download
            raise NotFoundError(f'File {file_uuid!r} with version {file_version!r} '
                                f'was found in catalog {catalog!r}, however no download is currently available')

    @cache
    def field_types(self, catalog: CatalogName) -> Mapping[str, FieldType]:
        """
        Returns the field type for each supported sort and filter field, using
        the name of the field as provided by clients.
        """
        result = {}
        plugin = self.service.metadata_plugin(catalog)
        for field, path in plugin.field_mapping.items():
            field_type = self.service.field_type(catalog, path)
            if isinstance(field_type, FieldType):
                result[field] = field_type
        # This field is a synthetic element of the response and will never be
        # null. Including it here helps to streamline request validation.
        accessible = plugin.special_fields.accessible
        assert accessible not in result, result
        result[accessible] = pass_thru_bool
        return result

    def _validate_wait(self, wait: str | None):
        if wait not in ('0', '1', None):
            raise ValueError

    def _validate_replica(self, replica: str):
        if replica not in ('aws', 'gcp'):
            raise ValueError

    def _file_param_validators(self,
                               catalog: CatalogName,
                               request_index: int
                               ) -> dict[str, Callable[[Any], Any]]:
        all_file_validators = dict(
            version=self.repository_plugin(catalog).validate_version,
            fileName=str,
            drsUri=str,
            sha256=str,
            md5=str
        )
        result = {}
        for a in attrs.fields(self._file_class(catalog)):
            try:
                param_name = self._file_params_by_field[a.name]
            except KeyError:
                assert a.name == 'uuid' or is_optional(a.type), a
            else:
                validator = all_file_validators[param_name]
                if request_index > 0 and not is_optional(a.type):
                    validator = Mandatory(validator)
                result[param_name] = validator
        return result

    def _file_from_request(self,
                           catalog: CatalogName,
                           uuid: str,
                           params: Mapping[str, str]
                           ) -> File:
        file_class = self._file_class(catalog)
        fields = {}
        for a in attrs.fields(file_class):
            if a.name == 'uuid':
                value = uuid
            else:
                try:
                    # A KeyError here means we do not support passing the field as a query parameter
                    param_name = self._file_params_by_field[a.name]
                    # A KeyError here means we do support it, but no parameter was provided
                    value = params[param_name]
                except KeyError:
                    assert is_optional(a.type), a
                    value = None
            fields[a.name] = value
        return file_class.from_json(fields)

    def _file_to_request(self, file: File) -> dict[str, str]:
        params = {}
        for a in attrs.fields(type(file)):
            if a.name != 'uuid':
                value = getattr(file, a.name)
                param_name = self._file_params_by_field.get(a.name)
                if param_name is None or not isinstance(value, str):
                    assert is_optional(a.type), (a.name, file)
                else:
                    params[param_name] = value
        return params

    _file_params_by_field = {
        'version': 'version',
        'name': 'fileName',
        'drs_uri': 'drsUri',
        'sha256': 'sha256',
        'md5': 'md5'
    }

    def _file_class(self, catalog: CatalogName) -> type[File]:
        return self.service.metadata_plugin(catalog).file_class
