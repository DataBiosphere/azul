from collections.abc import (
    Mapping,
)
from typing import (
    Optional,
    TypedDict,
    cast,
    get_type_hints,
)

import attr
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
    JSON,
)

manifest_state_key = 'manifest'


class ManifestGenerationState(TypedDict, total=False):
    manifest_key: JSON
    filters: JSON
    partition: Optional[JSON]
    manifest: Optional[JSON]


assert manifest_state_key in get_type_hints(ManifestGenerationState)


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
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

    def get_manifest_async(self,
                           *,
                           token_or_key: str,
                           query_params: Mapping[str, str],
                           fetch: bool,
                           authentication: Optional[Authentication]):
        if token_or_key is None:
            token, manifest_key = None, None
        else:
            try:
                token, manifest_key = Token.decode(token_or_key), None
            except InvalidTokenError:
                try:
                    token, manifest_key = None, SignedManifestKey.decode(token_or_key)
                except InvalidManifestKey:
                    # The OpenAPI spec doesn't distinguish key and token
                    raise BadRequestError('Invalid token')

        if token is None:
            if manifest_key is None:
                format = ManifestFormat(query_params['format'])
                catalog = query_params.get('catalog', config.default_catalog)
                filters = self.get_filters(catalog, authentication, query_params.get('filters'))
                try:
                    manifest = self.service.get_cached_manifest(format=format,
                                                                catalog=catalog,
                                                                filters=filters)
                except CachedManifestNotFound as e:
                    manifest, manifest_key = None, e.manifest_key
                    partition = ManifestPartition.first()
                    state: ManifestGenerationState = {
                        'filters': filters.to_json(),
                        'manifest_key': manifest_key.to_json(),
                        'partition': partition.to_json()
                    }
                    # ManifestGenerationState is also JSON but there is no way
                    # to express that since TypedDict rejects a co-parent class.
                    token = self.async_service.start_generation(cast(JSON, state))
                else:
                    manifest_key = manifest.manifest_key
            else:
                if fetch:
                    raise BadRequestError('The fetch endpoint does not support a manifest key')
                if authentication is not None:
                    raise BadRequestError('Must omit authentication when passing a manifest key')
                try:
                    manifest_key = self.service.verify_manifest_key(manifest_key)
                    manifest = self.service.get_cached_manifest_with_key(manifest_key)
                except CachedManifestNotFound:
                    raise GoneError('The requested manifest has expired, please request a new one')
                except InvalidManifestKeySignature:
                    raise BadRequestError('Invalid token')
        else:
            try:
                token_or_state = self.async_service.inspect_generation(token)
            except NoSuchGeneration:
                raise BadRequestError('Invalid token')
            except GenerationFailed as e:
                raise ChaliceViewError('Failed to generate manifest', e.status, e.output)
            if isinstance(token_or_state, Token):
                token, manifest, manifest_key = token_or_state, None, None
            elif isinstance(token_or_state, dict):
                state = token_or_state
                manifest = Manifest.from_json(state['manifest'])
                manifest_key = manifest.manifest_key
            else:
                assert False, token_or_state

        if manifest is None:
            url = self.manifest_url_func(fetch=fetch, token_or_key=token.encode())
            body = {
                'Status': 301,
                'Location': str(url),
                'Retry-After': token.wait_time,
                'CommandLine': self.service.command_lines(manifest, url, authentication)
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
                url = furl(manifest.location)
            body = {
                'Status': 302,
                'Location': str(url),
                'CommandLine': self.service.command_lines(manifest, url, authentication)
            }

        if fetch:
            return Response(body=body)
        else:
            headers = {k: str(body[k]) for k in body.keys() & {'Location', 'Retry-After'}}
            msg = ''.join(
                f'\nDownload the manifest in {shell} with `curl` using:\n\n{cmd}\n'
                for shell, cmd in body['CommandLine'].items()
            )
            return Response(body=msg, status_code=body['Status'], headers=headers)
