from typing import (
    Mapping,
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
from azul.service import (
    Controller,
)
from azul.service.async_manifest_service import (
    AsyncManifestService,
    InvalidTokenError,
    Token,
)
from azul.service.manifest_service import (
    Manifest,
    ManifestFormat,
    ManifestService,
)
from azul.service.storage_service import (
    StorageService,
)
from azul.types import (
    JSON,
)


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class ManifestController(Controller):
    step_function_lambda_name: str

    @cached_property
    def async_service(self) -> AsyncManifestService:
        name = config.state_machine_name(self.step_function_lambda_name)
        async_service = AsyncManifestService(name)
        return async_service

    @cached_property
    def service(self) -> ManifestService:
        return ManifestService(StorageService())

    def get_manifest(self, input: JSON) -> JSON:
        manifest = self.service.get_manifest(format_=ManifestFormat(input['format_']),
                                             catalog=input['catalog'],
                                             filters=input['filters'],
                                             object_key=input['object_key'])
        return manifest.to_json()

    def get_manifest_async(self,
                           *,
                           self_url: str,
                           catalog: CatalogName,
                           query_params: Mapping[str, str],
                           fetch: bool):

        token = query_params.get('token')
        if token is None:
            service = self.service
            format_ = ManifestFormat(query_params['format'])
            filters = service.parse_filters(query_params['filters'])
            object_key, manifest = service.get_cached_manifest(format_=format_,
                                                               catalog=catalog,
                                                               filters=filters)
            if manifest is None:
                assert object_key is not None
                input = dict(format_=format_.value,
                             catalog=catalog,
                             filters=filters,
                             object_key=object_key)
                token = self.async_service.start_generation(input)
        else:
            try:
                token = Token.decode(token)
                token_or_manifest = self.async_service.inspect_generation(token)
            except InvalidTokenError as e:
                raise BadRequestError(e.args) from e
            else:
                if isinstance(token_or_manifest, Token):
                    token, manifest = token_or_manifest, None
                elif isinstance(token_or_manifest, Manifest):
                    token, manifest = None, token_or_manifest
                else:
                    assert False, token_or_manifest

        if manifest is None:
            location = furl(self_url, args={'token': token.encode()})
            body = {
                'Status': 301,
                'Location': location.url,
                'Retry-After': token.wait_time
            }
        else:
            body = {
                'Status': 302,
                'Location': manifest.location
            }
            try:
                command_line = manifest.properties['command_line']
            except KeyError:
                pass
            else:
                body['CommandLine'] = command_line

        if fetch:
            return Response(body=body)
        else:
            headers = {k: str(body[k]) for k in body.keys() & {'Location', 'Retry-After'}}
            return Response(body='', status_code=body['Status'], headers=headers)
