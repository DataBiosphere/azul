from collections.abc import (
    Mapping,
    Sequence,
)
import json
import logging
from typing import (
    Any,
)

import attr
from chalice.app import (
    BadRequestError,
    ForbiddenError,
    NotFoundError,
    Request,
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
    PersonalAccessTokenAuthentication,
)
from azul.chalice import (
    TemporaryRedirectError,
)
from azul.drs import (
    DRSRequesterPaysRequired,
    DRSStatusException,
)
from azul.http import (
    LimitedTimeoutException,
    TooManyRequestsException,
)
from azul.indexer.mirror_service import (
    MirrorService,
)
from azul.indexer.repository_service import (
    RepositoryService,
)
from azul.lib import (
    R,
    cached_property,
)
from azul.lib.strings import (
    format_and_dedent as fd,
)
from azul.lib.types import (
    MutableJSON,
    json_int,
)
from azul.openapi import (
    params,
    responses,
    schema,
)
from azul.plugins import (
    RepositoryPlugin,
)
from azul.service.controller import (
    ServiceController,
    validate_params,
)
from azul.service.index_service import (
    IndexService,
)
from azul.service.user_service import (
    InvalidPersonalAccessTokenError,
    UserService,
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
        return MirrorService.for_catalog(catalog)

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
                    'replica',
                    schema.optional(str),
                    description=fd('''
                        If the underlying repository offers multiple replicas of the
                        requested file, use the specified replica. Otherwise, this
                        parameter is ignored. If absent, the only replica — for
                        repositories that don't support replication — or the default
                        replica — for those that do — will be used.
                    ''')
                )
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

    def _authentication(self, request: Request) -> Authentication | None:
        authentication = super()._authentication(request)
        if isinstance(authentication, PersonalAccessTokenAuthentication):
            try:
                authentication = self._user_service.exchange_token(authentication)
            except InvalidPersonalAccessTokenError:
                raise UnauthorizedError('Invalid token')
        return authentication

    @cached_property
    def _user_service(self) -> UserService:
        return UserService()

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
        # safely, since doing so requires a valid catalog. We need the
        # repository plugin to validate the file version.
        validate_params(query_params,
                        catalog=self._validate_catalog,
                        allow_extra_params=True)

        plugin = self._repository_plugin(catalog)

        validate_params(query_params,
                        catalog=str,
                        replica=self._validate_replica,
                        version=plugin.validate_version,
                        fileName=str,
                        allow_extra_params=False)

        file_version = query_params.get('version')
        replica = query_params.get('replica')
        file_name = query_params.get('fileName')

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

        mirror_url = None
        if config.enable_mirroring:
            mirror_url = self._mirror_service(catalog).mirror_url(file)

        if mirror_url is not None:
            location = mirror_url
        else:
            try:
                location = plugin.file_download_url(file, authentication, replica)
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
            except DRSRequesterPaysRequired as e:
                msg, status, data = e.args
                raise ForbiddenError(msg)

        if location is not None:
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
                'Location': location
            }
        else:
            assert file.drs_uri is None, file
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

    def _validate_replica(self, replica: str):
        if replica not in ('aws', 'gcp'):
            raise ValueError
