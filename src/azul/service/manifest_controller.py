from collections.abc import (
    Mapping,
)
from math import (
    ceil,
)
from typing import (
    TypedDict,
    cast,
    get_type_hints,
)

import attrs
from chalice import (
    BadRequestError,
    ChaliceViewError,
    Response,
)
from furl import (
    furl,
)

from azul import (
    cached_property,
    config,
)
from azul.auth import (
    Authentication,
)
from azul.chalice import (
    GoneError,
)
from azul.plugins import (
    ManifestFormat,
)
from azul.service import (
    Filters,
)
from azul.service.async_manifest_service import (
    AsyncManifestService,
    GenerationFailed,
    GenerationFinished,
    InvalidTokenError,
    NoSuchGeneration,
    Token,
)
from azul.service.manifest_service import (
    CachedManifestNotFound,
    InvalidManifestKey,
    InvalidManifestKeySignature,
    Manifest,
    ManifestKey,
    ManifestPartition,
    ManifestService,
    ManifestUrlFunc,
    SignedManifestKey,
)
from azul.service.source_controller import (
    SourceController,
)
from azul.service.storage_service import (
    StorageService,
)
from azul.types import (
    FlatJSON,
    JSON,
)

manifest_state_key = 'manifest'


class ManifestGenerationState(TypedDict, total=False):
    manifest_key: JSON
    filters: JSON
    partition: JSON | None
    manifest: JSON | None


assert manifest_state_key in get_type_hints(ManifestGenerationState)


@attrs.frozen(kw_only=True)
class ManifestController(SourceController):
    manifest_url_func: ManifestUrlFunc

    @cached_property
    def async_service(self) -> AsyncManifestService:
        return AsyncManifestService()

    @cached_property
    def service(self) -> ManifestService:
        return ManifestService(StorageService(), self.file_url_func)

    def get_manifest(self, state: JSON) -> ManifestGenerationState:
        # We trust StepFunctions to pass
        state: ManifestGenerationState
        partition = ManifestPartition.from_json(state['partition'])
        manifest_key = ManifestKey.from_json(state['manifest_key'])
        result = self.service.get_manifest(format=manifest_key.format,
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

    def _unpack_token_or_key(self,
                             token_or_key: str
                             ) -> tuple[Token | None, SignedManifestKey | None]:
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
        generation_id = manifest_key.uuid
        # ManifestGenerationState is also JSON but there is no way to express
        # that since TypedDict rejects a co-parent class.
        input = cast(JSON, state)
        next_iteration = 0 if previous_token is None else previous_token.iteration + 1
        for i in range(10):
            try:
                return self.async_service.start_generation(generation_id,
                                                           input,
                                                           iteration=next_iteration + i)
            except GenerationFinished:
                pass
        raise ChaliceViewError('Too many executions of this manifest generation')

    def get_manifest_async(self,
                           *,
                           token_or_key: str,
                           query_params: Mapping[str, str],
                           fetch: bool,
                           authentication: Authentication | None):

        token, manifest_key = self._unpack_token_or_key(token_or_key)

        if token is None:
            if manifest_key is None:
                # Neither a token representing an ongoing execution was given,
                # nor the key of an already cached manifest. There could still
                # be a cached manifest, so we'll need to look it up.
                format = ManifestFormat(query_params['format'])
                catalog = query_params.get('catalog', config.default_catalog)
                filters = self.get_filters(catalog, authentication, query_params.get('filters'))
                try:
                    manifest = self.service.get_cached_manifest(format=format,
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
                    manifest_key = self.service.verify_manifest_key(manifest_key)
                    manifest = self.service.get_cached_manifest_with_key(manifest_key)
                except CachedManifestNotFound:
                    # We could start another execution but that would require
                    # the client to follow more redirects. We've already sent
                    # the final 302 so we shouldn't that.
                    raise GoneError('The manifest has expired, please request a new one')
                except InvalidManifestKeySignature:
                    raise BadRequestError('Invalid token')
        else:
            # A token for an ongoing execution was given
            assert manifest_key is None, manifest_key
            try:
                token_or_result = self.async_service.inspect_generation(token)
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
                manifest = Manifest.from_json(result['output']['manifest'])
                manifest_key = manifest.manifest_key
                try:
                    manifest = self.service.get_cached_manifest_with_key(manifest_key)
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
        wait = query_params.get('wait')

        if manifest is None:
            assert token is not None
            url = self.manifest_url_func(fetch=fetch,
                                         token_or_key=token.encode(),
                                         **({} if wait is None else {'wait': wait}))
            body = {
                'Status': 301,
                'Location': str(url),
                'Retry-After': token.retry_after
            }
        else:
            assert manifest.manifest_key == manifest_key
            # The manifest is ultimately downloaded via a signed URL that points
            # to the storage bucket. This signed URL expires after one hour,
            # which is desirable because it is a client and its short lifespan
            # reduces the risk of it being shared. However, this also makes it
            # unsuitable for cURL downloads that may need to be retried over
            # longer timespans (https://github.com/DataBiosphere/azul/issues/2875)
            # To allow for cURL manifests to remain valid for longer than 1
            # hour, we instead return a 301 redirect to the non-fetch
            # `/manifest/files` endpoint with the object key of the cached
            # manifest specified as a query parameter. This object key is also a
            # client secret; it is mutually exclusive with OAuth tokens and
            # allows for the cached manifest to be downloaded without
            # authentication for as long as the cached manifest persists in S3.
            # This increases the risk of the secret being shared, but is
            # necessary to preserve the functionality of the cURL download.
            if fetch and manifest.format is ManifestFormat.curl:
                # For AnVIL, we are prohibited from exposing a manifest URL that
                # remains valid for longer than 1 hour. Currently, the AnVIL
                # plugin does not support cURL-format manifests.
                assert not config.is_anvil_enabled(manifest_key.catalog)
                manifest_key = self.service.sign_manifest_key(manifest_key)
                url = self.manifest_url_func(fetch=False, token_or_key=manifest_key.encode())
            else:
                url = furl(self.service.get_manifest_url(manifest))
            body = {
                'Status': 302,
                'Location': str(url),
                'CommandLine': self.service.command_lines(manifest, url, authentication)
            }

        if wait is not None:
            if wait == '0':
                pass
            elif wait == '1':
                retry_after = body.get('Retry-After')
                if retry_after is not None:
                    time_slept = self.server_side_sleep(float(retry_after))
                    body['Retry-After'] = ceil(retry_after - time_slept)
            else:
                assert False, wait

        # Note: Response objects returned without a 'Content-Type' header will
        # be given one of type 'application/json' as default by Chalice.
        # https://aws.github.io/chalice/tutorials/basicrestapi.html#customizing-the-http-response

        if fetch:
            return Response(body=body)
        else:
            status = body.pop('Status')
            command_line: FlatJSON = body.pop('CommandLine', None)
            headers = {k: str(v) for k, v in body.items()}
            if command_line is None:
                new_body = ''
            else:
                headers['Content-Type'] = 'text/plain'
                new_body = ''.join(
                    f'\nDownload the manifest in {shell} with `curl` using:\n\n{cmd}\n'
                    for shell, cmd in command_line.items()
                )
            return Response(body=new_body, status_code=status, headers=headers)
