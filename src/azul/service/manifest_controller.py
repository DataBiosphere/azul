from collections.abc import (
    Mapping,
)
from typing import (
    Any,
    Sequence,
    TypedDict,
    Union,
    cast,
    get_type_hints,
)

from chalice.app import (
    BadRequestError,
    ChaliceViewError,
    Response,
)
from furl import (
    furl,
)

from azul import (
    config,
)
from azul.auth import (
    Authentication,
)
from azul.chalice import (
    GoneError,
)
from azul.filters import (
    Filters,
)
from azul.lib import (
    cached_property,
    mutable_furl,
)
from azul.lib.strings import (
    format_and_dedent as fd,
)
from azul.lib.types import (
    AnyJSON,
    FlatJSON,
    JSON,
    LambdaContext,
    is_of_type,
    json_int,
    json_mapping,
    not_none,
    optional,
)
from azul.openapi import (
    params,
    responses,
    schema,
)
from azul.plugins import (
    ManifestFormat,
)
from azul.service.async_manifest_service import (
    AsyncManifestService,
    GenerationFailed,
    GenerationFinished,
    InvalidTokenError,
    NoSuchGeneration,
    Token,
)
from azul.service.controller import (
    validate_params,
)
from azul.service.manifest_service import (
    CachedManifestNotFound,
    CurlManifestGenerator,
    InvalidManifestKey,
    InvalidManifestKeySignature,
    Manifest,
    ManifestKey,
    ManifestPartition,
    ManifestService,
    SignedManifestKey,
)
from azul.service.query_controller import (
    QueryController,
)

manifest_state_key = 'manifest'


class ManifestGenerationState(TypedDict, total=False):
    manifest_key: JSON
    filters: JSON
    partition: JSON | None
    manifest: JSON | None


assert manifest_state_key in get_type_hints(ManifestGenerationState)


class ManifestController(QueryController):

    @cached_property
    def _async_service(self) -> AsyncManifestService:
        return AsyncManifestService()

    @cached_property
    def _service(self) -> ManifestService:
        return ManifestService(file_url_func=self._file_url)

    @property
    def _formats(self) -> Sequence[ManifestFormat]:
        return self._metadata_plugin.manifest_formats

    def _describe_format_param(self) -> str:
        descriptions_by_format = {
            ManifestFormat.compact: fd('''
                (the default) for a compact, tab-separated manifest.
            '''),
            ManifestFormat.terra_pfb: fd('''
                for a manifest in the [PFB format][2]. This format is mainly
                used for exporting data to Terra.
            '''),
            ManifestFormat.curl: fd('''
                for a [curl configuration file][3] manifest. This manifest can
                be used with the curl program to download all the files listed
                in the manifest.
            '''),
            ManifestFormat.verbatim_jsonl: fd('''
                for a verbatim manifest in [JSONL][4] format. Each line contains
                an unaltered metadata entity from the underlying repository.
            '''),
            ManifestFormat.verbatim_pfb: fd('''
                for a verbatim manifest in the [PFB format][2]. This format is
                mainly used for exporting data to Terra.
            ''')
        }
        supported_format_descriptions = [
            f'- `{format.value}` {descriptions_by_format[format]}'
            for format in self._formats
        ]
        links = [
            '[1]: https://software.broadinstitute.org/firecloud/documentation/article?id=10954',
            '[2]: https://github.com/uc-cdis/pypfb',
            '[3]: https://curl.haxx.se/docs/manpage.html#-K',
            '[4]: https://jsonlines.org/'
        ]
        return '\n\n'.join([
            'The desired format of the output.',
            *supported_format_descriptions,
            *links
        ])

    def _route(self, *, fetch: bool, initiate: bool):
        path = self._manifest_path(fetch=fetch, token=None if initiate else '{token}')
        return self.app.route(
            # The path parameter could be a token *or* an object key, but we don't
            # want to complicate the API with this detail
            path=path,
            # The initial PUT request is idempotent.
            methods=['PUT' if initiate else 'GET'],
            interactive=fetch,
            cors=True,
            path_spec=None if initiate else {
                'parameters': [
                    params.path('token', str, description=fd('''
                            An opaque string representing the manifest preparation job
                        '''))
                ]
            },
            spec={
                'tags': ['Manifests'],
                'summary':
                    (
                        'Initiate the preparation of a manifest'
                        if initiate else
                        'Determine status of a manifest preparation job'
                    ) + (
                        ' via XHR' if fetch else ''
                    ),
                'description': fd('''
                        Create a manifest preparation job, returning either

                        - a 301 redirect to the URL of the status of that job or

                        - a 302 redirect to the URL of an already prepared manifest.

                        This endpoint is not suitable for interactive use via the
                        Swagger UI. Please use [PUT /fetch/manifest/files][1] instead.

                        [1]: #operations-Manifests-put_fetch_manifest_files
                    ''') + self._parameter_hoisting_note('PUT', '/manifest/files', 'PUT')
                if initiate and not fetch else
                fd('''
                        Check on the status of an ongoing manifest preparation job,
                        returning either

                        - a 301 redirect to this endpoint if the manifest job is still
                          running

                        - a 302 redirect to the URL of the completed manifest.

                        This endpoint is not suitable for interactive use via the
                        Swagger UI. Please use [GET /fetch/manifest/files/{token}][1]
                        instead.

                        [1]: #operations-Manifests-get_fetch_manifest_files__token_
                    ''') if not initiate and not fetch else fd('''
                        Create a manifest preparation job, returning a 200 status
                        response whose JSON body emulates the HTTP headers that would be
                        found in a response to an equivalent request to the [PUT
                        /manifest/files][1] endpoint.

                        Whenever client-side JavaScript code is used in a web
                        application to request the preparation of a manifest from Azul,
                        this endpoint should be used instead of [PUT
                        /manifest/files][1]. This way, the client can use XHR to make
                        the request, retaining full control over the handling of
                        redirects and enabling the client to bypass certain limitations
                        on the native handling of redirects in web browsers. For
                        example, most browsers ignore the `Retry-After` header in
                        redirect responses, causing them to prematurely exhaust the
                        upper limit on the number of consecutive redirects, before the
                        manifest generation job is done.

                        [1]: #operations-Manifests-put_manifest_files
                    ''') + self._parameter_hoisting_note('PUT', '/fetch/manifest/files', 'PUT')
                if initiate and fetch else
                fd('''
                        Check on the status of an ongoing manifest preparation job,
                        returning a 200 status response whose JSON body emulates the
                        HTTP headers that would be found in a response to an equivalent
                        request to the [GET /manifest/files/{token}][1] endpoint.

                        Whenever client-side JavaScript code is used in a web
                        application to request the preparation of a manifest from Azul,
                        this endpoint should be used instead of [GET
                        /manifest/files/{token}][1]. This way, the client can use XHR to
                        make the request, retaining full control over the handling of
                        redirects and enabling the client to bypass certain limitations
                        on the native handling of redirects in web browsers. For
                        example, most browsers ignore the `Retry-After` header in
                        redirect responses, causing them to prematurely exhaust the
                        upper limit on the number of consecutive redirects, before the
                        manifest generation job is done.

                        [1]: #operations-Manifests-get_manifest_files__token_
                    '''),
                'parameters': [
                    self._catalog_param_spec,
                    self._filters_param_spec,
                    params.query(
                        'format',
                        schema.optional(
                            schema.enum(
                                *[
                                    format.value
                                    for format in self._formats
                                ],
                                form=str
                            )
                        ),
                        description=self._describe_format_param()
                    )
                ] if initiate else [],
                'responses': {
                    '301': {
                        'description': fd(f'''
                                A redirect indicating that the manifest preparation job
                                {'has started' if initiate else 'is running'}. Wait for
                                the recommended number of seconds (see `Retry-After`
                                header) and then follow the redirect to check the status
                                of {'that job' if initiate else 'the job again'}.
                            '''),
                        'headers': {
                            'Location': {
                                'description': fd('''
                                    The URL of the manifest preparation job at
                                    the [`GET /manifest/files/{token}`][2] endpoint.

                                    [2]: #operations-Manifests-get_fetch_manifest_files__token_
                                ''') if initiate else fd('''
                                    The URL of this endpoint
                                '''),
                                'schema': {'type': 'string', 'format': 'url'}
                            },
                            'Retry-After': {
                                'description': fd('''
                                    The recommended number of seconds to wait before
                                    requesting the URL specified in the `Location`
                                    header
                                '''),
                                'schema': {'type': 'string'}
                            }
                        }
                    },
                    '302': {
                        'description': fd(f'''
                                A redirect indicating that the manifest preparation job
                                is {'already' if initiate else 'now'} done. Immediately
                                follow the redirect to obtain the manifest contents.

                                The response body contains, for a number of commonly
                                used shells, a command line suitable for downloading the
                                manifest.
                            '''),
                        'headers': {
                            'Location': {
                                'description': fd(''' The URL of the manifest.
                                Clients should not make any assumptions about
                                any parts of the returned domain, except that
                                the scheme will be `https`.
                                '''),
                                'schema': {'type': 'string', 'format': 'url'}
                            }
                        }
                    },
                    **({} if initiate else {
                        '410': {
                            'description': fd('''
                                    The manifest preparation job has expired. Request a
                                    new preparation using the `PUT /manifest/files`
                                    endpoint.
                                ''')
                        }
                    })
                } if not fetch else {
                    '200': {
                        'description': fd('''
                                When handling this response, clients should wait the
                                number of seconds given in the `Retry-After` property of
                                the response body and then make another XHR request to
                                the URL specified in the `Location` property.

                                For a detailed description of these properties see the
                                documentation for the respective response headers
                                documented under ''') + (fd('''
                            [PUT /manifest/files][1].

                            [1]: #operations-Manifests-put_manifest_files
                            ''') if initiate else fd('''
                            [GET /manifest/files/{token}][1].

                            [1]: #operations-Manifests-get_manifest_files__token_
                            ''')) + fd('''

                            Note: For a 200 status code response whose body has the
                            `Status` property set to 302, the `Location` property
                            may reference the [GET /manifest/files/{token}][2]
                            endpoint and that endpoint may return yet another
                            redirect, this time a genuine (not emulated) 302 status
                            redirect to the actual location of the manifest.

                            [2]: #operations-Manifests-get_manifest_files__token_

                            Note: A 200 status response with a `Status` property of
                            302 in its body additionally contains a `CommandLine`
                            property that lists, for a number of commonly used
                            shells, a command line suitable for downloading the
                            manifest.
                        '''),
                        **responses.json_content(
                            schema.object(
                                additionalProperties=False,
                                Status=int,
                                Location={'type': 'string', 'format': 'url'},
                                **{'Retry-After': schema.optional(int)},
                                CommandLine=schema.optional(schema.object(
                                    additionalProperties=False,
                                    **{
                                        key: str
                                        for key in CurlManifestGenerator.command_lines(url=furl(''),
                                                                                       file_name='',
                                                                                       authentication=None)
                                    }
                                ))
                            )
                        ),
                    }
                }

            }
        )

    def _manifest_path(self, *, fetch: bool, token: str | None) -> tuple[str, ...]:
        path: tuple[str, ...] = ('manifest', 'files')
        if fetch:
            path = ('fetch', *path)
        if token is not None:
            path = (*path, token)
        return path

    def handlers(self) -> dict[str, Any]:
        @self._route(fetch=False, initiate=True)
        def put_manifest_files():
            return self.download(fetch=False)

        @self._route(fetch=False, initiate=False)
        def get_manifest_files_token(token: str):
            return self.download(fetch=False, token_or_key=token)

        @self._route(fetch=True, initiate=True)
        def put_fetch_manifest_files():
            return self.download(fetch=True)

        @self._route(fetch=True, initiate=False)
        def get_fetch_manifest_files_token(token: str):
            return self.download(fetch=True, token_or_key=token)

        @self.app.lambda_function(name=config.manifest_sfn)
        def generate_manifest(event: AnyJSON, _context: LambdaContext):
            assert isinstance(event, Mapping)
            assert all(isinstance(k, str) for k in event.keys())
            return self.generate(event)

        return locals()

    def download(self, fetch: bool, token_or_key: str | None = None):
        request = self.current_request
        query_params = self._hoist_parameters(request)
        if token_or_key is None:
            query_params.setdefault('filters', '{}')
            # We list the `catalog` validator first so that the catalog is validated
            # before any other potentially catalog-dependent validators are invoked
            validate_params(query_params,
                            catalog=self._validate_catalog,
                            format=self._validate_manifest_format,
                            filters=self._validate_filters)
            # Now that the catalog is valid, we can provide the default format that
            # depends on it
            default_format = self._formats[0].value
            query_params.setdefault('format', default_format)
        else:
            validate_params(query_params)
        authentication = self._authentication(request)
        return self._download(query_params=query_params,
                              token_or_key=token_or_key,
                              fetch=fetch,
                              authentication=authentication)

    def _validate_manifest_format(self, format: str):
        supported_formats = {f.value for f in self._formats}
        try:
            ManifestFormat(format)
        except ValueError:
            raise BadRequestError(f'Unknown manifest format `{format}`. '
                                  f'Must be one of {supported_formats}')
        else:
            if format not in supported_formats:
                raise BadRequestError(f'Manifest format `{format}` is not supported for '
                                      f'catalog {self.app.catalog}. Must be one of {supported_formats}')

    def _download(self,
                  *,
                  token_or_key: str | None,
                  query_params: Mapping[str, str],
                  fetch: bool,
                  authentication: Authentication | None):
        manifest: Manifest | None
        manifest_key: SignedManifestKey | ManifestKey | None
        token, manifest_key = self._unpack_token_or_key(token_or_key)

        if token is None:
            if manifest_key is None:
                # Neither a token representing an ongoing execution was given,
                # nor the key of an already cached manifest. There could still
                # be a cached manifest, so we'll need to look it up.
                format = ManifestFormat(query_params['format'])
                catalog = query_params.get('catalog', config.default_catalog)
                filters = query_params.get('filters')
                filters = self._prepare_filters(catalog, authentication, filters)
                try:
                    manifest = self._service.get_cached_manifest(format=format,
                                                                 catalog=catalog,
                                                                 filters=filters)
                except CachedManifestNotFound as e:
                    # A cache miss, but the exception tells us the cache key
                    manifest, manifest_key = None, e.manifest_key
                    # Prepare the execution that will generate the manifest
                    token = self._start_execution(filters=filters,
                                                  manifest_key=manifest_key)
                else:
                    # A cache hit
                    manifest_key = manifest.manifest_key
            else:
                # The client passed the key of a cached manifest, originating
                # from the final 302 response to a fetch request for a curl
                # manifest (see below).
                if fetch:
                    raise BadRequestError('The fetch endpoint does not support a manifest key')
                if authentication is not None:
                    raise BadRequestError('Must omit authentication when passing a manifest key')
                try:
                    manifest_key = self._service.verify_manifest_key(manifest_key)
                    manifest = self._service.get_cached_manifest_with_key(manifest_key)
                except CachedManifestNotFound:
                    # We could start another execution but that would require
                    # the client to follow more redirects. We've already sent
                    # the final 302 so we shouldn't do that.
                    raise GoneError('The manifest has expired, please request a new one')
                except InvalidManifestKeySignature:
                    raise BadRequestError('Invalid token')
        else:
            # A token for an execution was given
            assert manifest_key is None, manifest_key
            try:
                token_or_result = self._async_service.inspect_generation(token)
            except NoSuchGeneration:
                raise BadRequestError('Invalid token')
            except GenerationFailed as e:
                raise ChaliceViewError('Failed to generate manifest', e.status, e.output)
            if isinstance(token_or_result, Token):
                # Execution is still ongoing, we got an updated token
                token, manifest, manifest_key = token_or_result, None, None
            elif isinstance(token_or_result, dict):
                # The execution is done, the resulting manifest should be ready
                result = token_or_result
                manifest = Manifest.from_json(json_mapping(result['output']['manifest']))
                manifest_key = manifest.manifest_key
                try:
                    manifest = self._service.get_cached_manifest_with_key(manifest_key)
                except CachedManifestNotFound as e:
                    assert manifest_key == e.manifest_key
                    # There are two possible causes for the missing manifest: it
                    # may have expired, in which case the supplied token must be
                    # really stale, or the manifest was deleted immediately
                    # after it was created. We haven't sent a 302 redirect yet,
                    # so we'll just restart the generation by starting another
                    # execution for it.
                    manifest = None
                    filters = Filters.from_json(result['input']['filters'])
                    token = self._start_execution(filters=filters,
                                                  manifest_key=manifest_key,
                                                  previous_token=token)
                else:
                    assert manifest_key == manifest.manifest_key
            else:
                assert False, token_or_result

        body: dict[str, int | str | FlatJSON]
        url: furl

        if manifest is None:
            assert token is not None
            url = self._manifest_url(fetch=fetch, token_or_key=token.encode())
            body = {
                'Status': 301,
                'Location': str(url),
                'Retry-After': token.retry_after
            }
        else:
            assert manifest.manifest_key == manifest_key

            # The manifest is ultimately downloaded via a signed URL that points
            # to the storage bucket. This URL expires after one hour. The signed
            # URL is a client-side secret and a short expiration reduces the
            # impact of it being accidentially shared with unauthorized parties.
            #
            # A short expiration, however, is unsuitable for cURL downloads that
            # may need to be retried over a longer time period. To allow for
            # cURL manifests to remain valid for longer than one hour, we
            # instead return a 301 redirect to the non-fetch `/manifest/files`
            # endpoint with the object key of the cached manifest specified as a
            # query parameter. This object key is also a client secret; it is
            # mutually exclusive with OAuth tokens and allows for the cached
            # manifest to be downloaded without authentication for as long as
            # the cached manifest persists in S3. This increases the risk of the
            # secret being shared, but is necessary to preserve the utility of
            # the cURL download feature in general.
            #
            # Note that on AnVIL, we are prohibited from exposing manifest URLs
            # that remain valid for longer than one hour, if those manifests
            # contain managed-access data or metadata. Fortunately, there's
            # currently no need to enforce this here because the AnVIL plugin's
            # cURL-format manifest never includes managed-access files.
            #
            if fetch and manifest.format is ManifestFormat.curl:
                manifest_key = self._service.sign_manifest_key(manifest_key)
                url = self._manifest_url(fetch=False, token_or_key=manifest_key.encode())
            else:
                url = furl(self._service.get_manifest_url(manifest))
            body = {
                'Status': 302,
                'Location': str(url),
                'CommandLine': self._service.command_lines(manifest, url, authentication)
            }

        # Note: Response objects returned without a 'Content-Type' header will
        # be given one of type 'application/json' as default by Chalice.
        #
        # https://aws.github.io/chalice/tutorials/basicrestapi.html#customizing-the-http-response

        if fetch:
            return Response(body=body)
        else:
            status = json_int(body.pop('Status'))
            command_line = optional(json_mapping, body.pop('CommandLine', None))
            headers: dict[str, str | list[str]] = {k: str(v) for k, v in body.items()}
            if command_line is None:
                new_body = ''
            else:
                headers['Content-Type'] = 'text/plain'
                new_body = ''.join(
                    f'\nDownload the manifest in {shell} with `curl` using:\n\n{cmd}\n'
                    for shell, cmd in command_line.items()
                )
            return Response(body=new_body, status_code=status, headers=headers)

    def _manifest_url(self,
                      *,
                      fetch: bool,
                      token_or_key: str | None = None,
                      **params: str
                      ) -> mutable_furl:
        path = self._manifest_path(fetch=fetch, token=token_or_key)
        url = self.app.base_url.add(path=path)
        return url.set(args=params)

    type TokenOrKey = Union[
        tuple[None, None],
        tuple[Token, None],
        tuple[None, SignedManifestKey]
    ]

    def _unpack_token_or_key(self, token_or_key: str | None) -> TokenOrKey:
        if token_or_key is None:
            return None, None
        else:
            try:
                return Token.decode(token_or_key), None
            except InvalidTokenError:
                try:
                    return None, SignedManifestKey.decode(token_or_key)
                except InvalidManifestKey:
                    # The OpenAPI spec doesn't distinguish key and token
                    raise BadRequestError('Invalid token')

    def _start_execution(self,
                         filters: Filters,
                         manifest_key: ManifestKey,
                         previous_token: Token | None = None,
                         ) -> Token:
        partition = ManifestPartition.first()
        state: ManifestGenerationState = {
            'filters': filters.to_json(),
            'manifest_key': manifest_key.to_json(),
            'partition': partition.to_json()
        }
        # Manifest keys for catalogs with long names would be too long to be
        # used directly as state machine execution names.
        #
        generation_id = manifest_key.uuid

        # ManifestGenerationState is also JSON but there is no way to express
        # that since TypedDict rejects a co-parent class.
        #
        input = cast(JSON, state)
        iteration = 0 if previous_token is None else previous_token.iteration + 1

        # Depending on the configured manifest expiration, and assuming that the
        # manifest isn't deleted prematurely, there is an upper bound on the
        # number of times a manifest can expire before the execution that
        # generated it does. This also means that we can limit how many times we
        # expect the manifest to be re-generated by a new iteration. In
        # practice, we don't ever expect to see that many iterations because the
        # manifest key is invalidated every time new code is deployed, which
        # typically happens at least once a week.
        #
        sfn_execution_expiration = 90  # Fixed by AWS
        max_iteration = sfn_execution_expiration // config.manifest_expiration
        if iteration > max_iteration:
            raise ChaliceViewError('Too many executions of this manifest generation')
        try:
            return self._async_service.start_generation(generation_id, input, iteration)
        except GenerationFinished as e:
            # Returning a token will result in a redirect. If the client follows
            # that redirect, and if the manifest still doesn't exist, we'll end
            # up right back in this method and can then try starting the next
            # iteration.
            return e.token

    def generate(self, state: JSON) -> ManifestGenerationState:
        assert is_of_type(state, ManifestGenerationState)
        partition = ManifestPartition.from_json(not_none(state['partition']))
        manifest_key = ManifestKey.from_json(state['manifest_key'])
        result = self._service.get_manifest(format=manifest_key.format,
                                            catalog=manifest_key.catalog,
                                            filters=Filters.from_json(state['filters']),
                                            partition=partition,
                                            manifest_key=manifest_key)
        if isinstance(result, ManifestPartition):
            assert not result.is_last, result
            return {
                **state,
                'partition': result.to_json()
            }
        elif isinstance(result, Manifest):
            return {
                # The presence of this key terminates the step function loop
                'manifest': result.to_json()
            }
        else:
            assert False, type(result)
