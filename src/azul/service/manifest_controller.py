from typing import (
    Mapping,
)

import attr
from botocore.exceptions import (
    ClientError,
)
from chalice import (
    BadRequestError,
    Response,
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
)
from azul.service.manifest_service import (
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

    def get_manifest(self, event: JSON) -> JSON:
        manifest = self.service.get_manifest(format_=ManifestFormat(event['format']),
                                             catalog=event['catalog'],
                                             filters=event['filters'],
                                             object_key=event['object_key'])
        return manifest.to_json()

    def get_manifest_async(self,
                           *,
                           self_url: str,
                           catalog: CatalogName,
                           query_params: Mapping[str, str],
                           fetch: bool):
        wait_time, manifest = self.__file_manifest(self_url, catalog, query_params)
        if fetch:
            body = {
                'Status': 301 if wait_time else 302,
                'Location': manifest.location,
            }
            try:
                command_line = manifest.properties['command_line']
            except KeyError:
                pass
            else:
                body['CommandLine'] = command_line
            if wait_time:  # Only return Retry-After if manifest is not ready
                body['Retry-After'] = wait_time
            return Response(body=body)
        else:
            return Response(body='',
                            headers={
                                'Retry-After': str(wait_time),
                                'Location': manifest.location
                            },
                            status_code=301 if wait_time else 302)

    def __file_manifest(self, self_url: str, catalog: CatalogName, query_params):
        format_ = ManifestFormat(query_params['format'])
        service = self.service
        filters = service.parse_filters(query_params['filters'])

        object_key = None
        token = query_params.get('token')
        if token is None:
            object_key, manifest = service.get_cached_manifest(format_=format_,
                                                               catalog=catalog,
                                                               filters=filters)
            if manifest is not None:
                return 0, manifest
        try:
            return self.async_service.start_or_inspect_manifest_generation(
                self_url,
                format_=format_,
                catalog=catalog,
                filters=filters,
                token=token,
                object_key=object_key)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ExecutionDoesNotExist':
                raise BadRequestError('Invalid token given')
            raise
        except ValueError as e:
            raise BadRequestError(e.args)
