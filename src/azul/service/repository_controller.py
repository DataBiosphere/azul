from collections.abc import (
    Mapping,
    Sequence,
)
import json
import logging
import time
from typing import (
    Any,
)

import attr
import attrs
from chalice.app import (
    BadRequestError,
    NotFoundError,
    Response,
    TooManyRequestsError,
    UnauthorizedError,
)
from more_itertools import (
    one,
)

from azul import (
    CatalogName,
    config,
)
from azul.auth import (
    Authentication,
)
from azul.chalice import (
    TemporaryRedirectError,
)
from azul.drs import (
    DRSStatusException,
)
from azul.http import (
    LimitedTimeoutException,
    TooManyRequestsException,
)
from azul.indexer.mirror_service import (
    MirrorFileDownload,
    MirrorService,
)
from azul.indexer.repository_service import (
    RepositoryService,
)
from azul.lib import (
    R,
    cached_property,
)
from azul.lib.collections import (
    adict,
)
from azul.lib.types import (
    MutableJSON,
    is_optional,
    json_int,
)
from azul.openapi import (
    format_description as fd,
    params,
    responses,
    schema,
)
from azul.plugins import (
    File,
    RepositoryPlugin,
)
from azul.service.controller import (
    Mandatory,
    ServiceController,
    Validator,
    validate_params,
)
from azul.service.index_service import (
    IndexService,
)

log = logging.getLogger(__name__)


class RepositoryController(ServiceController):

    @cached_property
    def _repository_service(self) -> RepositoryService:
        return RepositoryService()

    @cached_property
    def _index_service(self) -> IndexService:
        return IndexService()

    def _mirror_service(self, catalog: CatalogName) -> MirrorService:
        return self._index_service.mirror_service(catalog)

    def _repository_plugin(self, catalog: CatalogName) -> RepositoryPlugin:
        return self._repository_service.repository_plugin(catalog)

    @property
    def _repository_files_spec(self):
        return {
            'tags': ['Repository'],
            'parameters': [
                self._catalog_param_spec,
                *self._file_fqid_parameters_spec,
                params.query(
                    'fileName',
                    schema.optional(str),
                    description=fd('''
                        The desired name of the file. The given value will be included
                        in the Content-Disposition header of the response. If absent, a
                        best effort to determine the file name from metadata will be
                        made. If that fails, the UUID of the file will be used instead.
                    ''')
                ),
                params.query(
                    'wait',
                    schema.optional(int),
                    description=fd('''
                        If 0, the client is responsible for honoring the waiting period
                        specified in the Retry-After response header. If 1, the server
                        will delay the response in order to consume as much of that
                        waiting period as possible. This parameter should only be set to
                        1 by clients who can't honor the `Retry-After` header,
                        preventing them from quickly exhausting the maximum number of
                        redirects. If the server cannot wait the full amount, any amount
                        of wait time left will still be returned in the Retry-After
                        header of the response.
                    ''')
                ),
                params.query(
                    'replica',
                    schema.optional(str),
                    description=fd('''
                        If the underlying repository offers multiple replicas of the
                        requested file, use the specified replica. Otherwise, this
                        parameter is ignored. If absent, the only replica — for
                        repositories that don't support replication — or the default
                        replica — for those that do — will be used.
                    ''')
                ),
                params.query(
                    'requestIndex',
                    schema.optional(int),
                    description='Do not use. Reserved for internal purposes.'
                ),
                params.query(
                    'drsUri',
                    schema.optional(str),
                    description='Do not use. Reserved for internal purposes.'
                ),
                params.query('token',
                             schema.optional(str),
                             description='Reserved. Do not pass explicitly.')
            ]
        }

    def handlers(self) -> dict[str, Any]:
        @self.app.route(
            path=self._file_path(fetch=False, file_uuid='{file_uuid}'),
            methods=['GET'],
            interactive=False,
            cors=True,
            spec={
                **self._repository_files_spec,
                'summary': 'Redirect to a URL for downloading a given data file from the '
                           'underlying repository',
                'description': fd('''
                    This endpoint is not suitable for interactive use via the Swagger
                    UI. Please use the [/fetch endpoint][1] instead.

                    [1]: #operations-Repository-get_fetch_repository_files__file_uuid_
                '''),
                'responses': {
                    '301': {
                        'description': fd('''
                            A URL to the given file is still being prepared. Retry by
                            waiting the number of seconds specified in the `Retry-After`
                            header of the response and the requesting the URL specified
                            in the `Location` header.
                        '''),
                        'headers': {
                            'Location': responses.header(str, description=fd('''
                                A URL pointing back at this endpoint, potentially with
                                different or additional request parameters.
                            ''')),
                            'Retry-After': responses.header(int, description=fd('''
                                Recommended number of seconds to wait before requesting
                                the URL specified in the `Location` header. The response
                                may carry this header even if server-side waiting was
                                requested via `wait=1`.
                            '''))
                        }
                    },
                    '302': {
                        'description': fd('''
                            The file can be downloaded from the URL returned in the
                            `Location` header.
                        '''),
                        'headers': {
                            'Location': responses.header(str, description=fd('''
                                    A URL that will yield the actual content of the file.
                            ''')),
                            'Content-Disposition': responses.header(str, description=fd('''
                                Set to a value that makes user agents download the file
                                instead of rendering it, suggesting a meaningful name
                                for the downloaded file stored on the user's file
                                system. The suggested file name is taken  from the
                                `fileName` request parameter or, if absent, from
                                metadata describing the file. It generally does not
                                correlate with the path component of the URL returned in
                                the `Location` header.
                            '''))
                        }
                    }
                }
            }
        )
        def get_repository_files(file_uuid: str) -> Response:
            result = self.download_file(file_uuid, fetch=False)
            status_code = json_int(result.pop('Status'))
            return Response(body='',
                            headers={k: str(v) for k, v in result.items()},
                            status_code=status_code)

        @self.app.route(
            path=self._file_path(fetch=True, file_uuid='{file_uuid}'),
            methods=['GET'],
            cors=True,
            spec={
                **self._repository_files_spec,
                'summary': 'Request a URL for downloading a given data file',
                'responses': {
                    '200': {
                        'description': fd(f'''
                            Emulates the response code and headers of
                            {one(getattr(get_repository_files, 'path'))} while bypassing
                            the default user agent behavior. Note that the status
                            code of a successful response will be 200 while the
                            `Status` field of its body will be 302.

                            The response described here is intended to be processed by
                            client-side Javascript such that the emulated headers can be
                            handled in Javascript rather than relying on the native
                            implementation by the web browser.
                        '''),
                        **responses.json_content(
                            schema.object(
                                Status=int,
                                Location=str
                            )
                        )
                    }
                }
            }
        )
        def get_fetch_repository_files(file_uuid: str) -> Response:
            body = self.download_file(file_uuid, fetch=True)
            return Response(body=json.dumps(body), status_code=200)

        @self.app.route(
            '/repository/sources',
            methods=['GET'],
            cors=True,
            spec={
                'summary': 'List available data sources',
                'tags': ['Repository'],
                'parameters': [self._catalog_param_spec],
                'responses': {
                    '200': {
                        'description': fd('''
                            List the sources the currently authenticated user is
                            authorized to access in the underlying data repository.
                        '''),
                        **responses.json_content(
                            schema.object(sources=schema.array(
                                schema.object(
                                    sourceId=str,
                                    sourceSpec=str
                                )
                            ))
                        )
                    }
                }
            }
        )
        def get_repository_sources() -> Response:
            request = self.current_request
            query_params = self._query_params(request)
            validate_params(query_params,
                            catalog=self._validate_catalog)
            authentication = self._authentication(request)
            sources = self.list_sources(self.app.catalog,
                                        authentication)
            return Response(body={'sources': sources}, status_code=200)

        return locals()

    def download_file(self, file_uuid: str, fetch: bool) -> MutableJSON:
        request = self.current_request
        query_params = self._query_params(request)
        headers = request.headers

        # FIXME: Prevent duplicate filenames from files in different subgraphs by
        #        prepending the subgraph UUID to each filename when downloaded
        #        https://github.com/DataBiosphere/azul/issues/2682

        catalog = self.app.catalog
        authentication = self._authentication(request)
        return self._download_file(catalog=catalog,
                                   fetch=fetch,
                                   file_uuid=file_uuid,
                                   query_params=query_params,
                                   headers=headers,
                                   authentication=authentication)

    def _download_file(self,
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
                        catalog=self._validate_catalog,
                        requestIndex=int,
                        allow_extra_params=True)

        request_index = int(query_params.get('requestIndex', '0'))

        validate_params(query_params,
                        catalog=str,
                        requestIndex=int,
                        wait=self._validate_wait,
                        replica=self._validate_replica,
                        token=str,
                        allow_extra_params=False,
                        **self._file_param_validators(catalog, request_index))

        file_version = query_params.get('version')
        replica = query_params.get('replica')
        file_name = query_params.get('fileName')
        drs_uri = query_params.get('drsUri')
        wait = query_params.get('wait')
        token = query_params.get('token')

        if request_index == 0:
            filters = self._prepare_filters(catalog, authentication, None)
            file = self._index_service.get_data_file(catalog=catalog,
                                                     file_uuid=file_uuid,
                                                     file_version=file_version,
                                                     filters=filters)
            if file is None:
                raise NotFoundError(f'Unable to find file {file_uuid!r}, '
                                    f'version {file_version!r} in catalog {catalog!r}')
            if file_name is not None:
                file = attr.evolve(file, name=file_name)
            if drs_uri is not None:
                file = attr.evolve(file, drs_uri=drs_uri)
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

        plugin = self._repository_plugin(catalog)

        mirror_url = None
        if config.enable_mirroring:
            mirror_service = self._mirror_service(catalog)
            if mirror_service.info_exists(file):
                mirror_url = mirror_service.mirror_url(file)

        if mirror_url is None:
            download_cls = plugin.file_download_class()
            download = download_cls(plugin=plugin, file=file, replica=replica, token=token)
        else:
            # The file's content type would be None on subsequent requests since
            # it isn't propagated via a query parameter. `MirrorFileDownload`
            # will always be ready immediately.
            assert request_index == 0, request_index
            download = MirrorFileDownload(
                plugin=plugin,
                file=file,
                location=mirror_url,
                replica=replica,
                token=token
            )
            assert download.retry_after is None, download

        try:
            download.update(authentication)
        except LimitedTimeoutException as e:
            raise TemporaryRedirectError(*e.args)
        except TooManyRequestsException as e:
            raise TooManyRequestsError(*e.args)
        except DRSStatusException as e:
            msg, status, data = e.args
            if status == UnauthorizedError.STATUS_CODE:
                raise UnauthorizedError(msg)
            else:
                raise
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
            query_params = adict(self._file_to_request(download.file),
                                 token=download.token,
                                 replica=download.replica,
                                 requestIndex=str(request_index + 1),
                                 wait=wait)
            return {
                'Status': 301,
                **({'Retry-After': retry_after} if retry_after else {}),
                'Location': str(self._file_url(catalog=catalog,
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
                     'repository' if mirror_url is None else 'mirrored',
                     json.dumps(log_data))
            return {
                'Status': 302,
                'Location': download.location
            }
        else:
            assert download.file.drs_uri is None, download
            raise NotFoundError(f'File {file_uuid!r} with version {file_version!r} '
                                f'was found in catalog {catalog!r}, however no download is currently available')

    def _parse_range_request_header(self,
                                    range_specifier: str
                                    ) -> Sequence[tuple[int | None, int | None]]:
        """
        >>> # noinspection PyTypeChecker
        >>> dc = RepositoryController(app=None)
        >>> dc._parse_range_request_header('bytes=100-200,300-400')
        [(100, 200), (300, 400)]

        >>> dc._parse_range_request_header('bytes=-100')
        [(None, 100)]

        >>> dc._parse_range_request_header('bytes=100-')
        [(100, None)]

        >>> dc._parse_range_request_header('foo=100')
        []

        >>> dc._parse_range_request_header('')
        Traceback (most recent call last):
        ...
        chalice.app.BadRequestError: Invalid range specifier ''

        >>> dc._parse_range_request_header('100-200')
        Traceback (most recent call last):
        ...
        chalice.app.BadRequestError: Invalid range specifier '100-200'

        >>> dc._parse_range_request_header('bytes=')
        Traceback (most recent call last):
        ...
        chalice.app.BadRequestError: Invalid range specifier 'bytes='

        >>> dc._parse_range_request_header('bytes=100')
        Traceback (most recent call last):
        ...
        chalice.app.BadRequestError: Invalid range specifier 'bytes=100'

        >>> dc._parse_range_request_header('bytes=-')
        Traceback (most recent call last):
        ...
        chalice.app.BadRequestError: Invalid range specifier 'bytes=-'

        >>> dc._parse_range_request_header('bytes=--')
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

    def _validate_wait(self, wait: str | None):
        if wait not in ('0', '1', None):
            raise ValueError

    def _validate_replica(self, replica: str):
        if replica not in ('aws', 'gcp'):
            raise ValueError

    def _file_param_validators(self,
                               catalog: CatalogName,
                               request_index: int
                               ) -> dict[str, Validator]:
        all_file_validators: Mapping[str, Validator] = dict(
            version=self._repository_plugin(catalog).validate_version,
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
        return self._index_service.metadata_plugin(catalog).file_class
