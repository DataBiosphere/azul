import json
from typing import (
    Mapping,
    Optional,
)

import attr
from chalice import (
    BadRequestError,
    Response,
)
from furl import (
    furl,
)

from azul import (
    CatalogName,
    cached_property,
    config,
)
from azul.auth import (
    Authentication,
)
from azul.chalice import (
    GoneError,
)
from azul.service import (
    Filters,
)
from azul.service.async_manifest_service import (
    AsyncManifestService,
    InvalidTokenError,
    Token,
)
from azul.service.manifest_service import (
    CachedManifestNotFound,
    CachedManifestSourcesChanged,
    Manifest,
    ManifestFormat,
    ManifestPartition,
    ManifestService,
    ManifestUrlFunc,
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


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class ManifestController(SourceController):
    step_function_lambda_name: str
    manifest_url_func: ManifestUrlFunc

    @cached_property
    def async_service(self) -> AsyncManifestService:
        name = config.state_machine_name(self.step_function_lambda_name)
        async_service = AsyncManifestService(name)
        return async_service

    @cached_property
    def service(self) -> ManifestService:
        return ManifestService(StorageService(), self.file_url_func)

    partition_state_key = 'partition'

    manifest_state_key = 'manifest'

    def get_manifest(self, state: JSON) -> JSON:
        partition = ManifestPartition.from_json(state[self.partition_state_key])
        auth = state.get('authentication')
        result = self.service.get_manifest(format_=ManifestFormat(state['format_']),
                                           catalog=state['catalog'],
                                           filters=Filters.from_json(state['filters']),
                                           partition=partition,
                                           authentication=None if auth is None else Authentication.from_json(auth),
                                           object_key=state['object_key'])
        if isinstance(result, ManifestPartition):
            assert not result.is_last, result
            return {
                **state,
                self.partition_state_key: result.to_json()
            }
        elif isinstance(result, Manifest):
            return {
                # The presence of this key terminates the step function loop
                self.manifest_state_key: result.to_json()
            }
        else:
            assert False, type(result)

    # FIXME: Accept furl instance parameter instead of URL string
    #        https://github.com/DataBiosphere/azul/issues/3398
    def get_manifest_async(self,
                           *,
                           self_url: str,
                           catalog: CatalogName,
                           query_params: Mapping[str, str],
                           fetch: bool,
                           authentication: Optional[Authentication]):

        token = query_params.get('token')
        if token is None:
            format_ = ManifestFormat(query_params['format'])
            try:
                object_key = query_params['objectKey']
            except KeyError:
                filters = self.get_filters(catalog, authentication, query_params.get('filters'))
                object_key, manifest = self.service.get_cached_manifest(format_=format_,
                                                                        catalog=catalog,
                                                                        filters=filters,
                                                                        authentication=authentication)
                if manifest is None:
                    assert object_key is not None
                    partition = ManifestPartition.first()
                    state = {
                        'format_': format_.value,
                        'catalog': catalog,
                        'filters': filters.to_json(),
                        'authentication': None if authentication is None else authentication.to_json(),
                        'object_key': object_key,
                        self.partition_state_key: partition.to_json()
                    }
                    token = self.async_service.start_generation(state)
            else:
                # FIXME: Add support for long-lived API tokens
                #        https://github.com/DataBiosphere/azul/issues/3328
                if authentication is None:
                    filters = self.get_filters(catalog, authentication, query_params.get('filters'))
                else:
                    raise BadRequestError("Must omit authentication when passing 'objectKey'")
                try:
                    manifest = self.service.get_cached_manifest_with_object_key(
                        format_=format_,
                        catalog=catalog,
                        filters=filters,
                        object_key=object_key,
                        authentication=authentication
                    )
                except CachedManifestNotFound:
                    raise GoneError('The requested manifest has expired, '
                                    'please request a new one')
                except CachedManifestSourcesChanged:
                    raise GoneError('The requested manifest has become invalid '
                                    'due to an authorization change, please '
                                    'request a new one')
                else:
                    assert manifest is not None
        else:
            try:
                token = Token.decode(token)
                token_or_state = self.async_service.inspect_generation(token)
            except InvalidTokenError as e:
                raise BadRequestError(e.args) from e
            else:
                if isinstance(token_or_state, Token):
                    token, manifest = token_or_state, None
                elif isinstance(token_or_state, JSON.__origin__):
                    manifest = Manifest.from_json(token_or_state[self.manifest_state_key])
                else:
                    assert False, token_or_state

        if manifest is None:
            location = furl(self_url, args={'token': token.encode()})
            body = {
                'Status': 301,
                'Location': str(location),
                'Retry-After': token.wait_time,
                'CommandLine': self.service.command_lines(manifest,
                                                          str(location),
                                                          authentication)
            }
        else:
            if fetch:
                filters = manifest.filters.reify(self.service.service_config(catalog),
                                                 explicit_only=True)
                url = self.manifest_url_func(fetch=False,
                                             catalog=manifest.catalog,
                                             format_=manifest.format_,
                                             filters=json.dumps(filters),
                                             objectKey=manifest.object_key)
            else:
                url = manifest.location
            body = {
                'Status': 302,
                'Location': url,
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
