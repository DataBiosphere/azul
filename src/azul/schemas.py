import json
import re
from typing import (
    Any,
)

from chalice.app import (
    BadRequestError,
)

from azul.chalice import (
    Controller,
)
from azul.lib import (
    mutable_furl,
)
from azul.lib.strings import (
    format_and_dedent as fd,
)
from azul.lib.types import (
    JSON,
)
from azul.openapi import (
    params,
    responses,
    schema,
)


class SchemaController(Controller):
    """
    A controller for serving JSON schemas relating to an Azul facility
    """
    _schema_route = '/schemas/{facility}/{name}/{version_and_extension}'

    version_and_extension_re = re.compile(r'v([1-9][0-9]*)\.json')

    def _parse_version(self, version_and_extension: str):
        match = self.version_and_extension_re.match(version_and_extension)
        if match:
            return match.group(1)
        else:
            raise BadRequestError('Invalid version and extension', version_and_extension)

    def _format_version(self, version: int) -> str:
        return f'v{version}.json'

    def schema_url(self, facility: str, name: str, version: int) -> mutable_furl:
        path = self._schema_route.format(facility=facility,
                                         name=name,
                                         version_and_extension=self._format_version(version))
        return self.app.base_url.set(path=path)

    def handlers(self) -> dict[str, Any]:
        """
        Chalice routes and application handlers to be injected into the global
        scope of a Chalice application module.
        """

        @self.app.route(
            self._schema_route,
            methods=['GET'],
            cors=True,
            spec={
                'summary': 'Retrieve JSON schemas',
                'tags': ['Auxiliary'],
                'parameters': [
                    params.path('facility', str, example='mirror'),
                    params.path('name', str, example='info'),
                    params.path('version_and_extension',
                                schema.pattern(self.version_and_extension_re.pattern),
                                example='v2.json'),
                ],
                'description': fd(
                    '''
                    [JSON Schemas](https://json-schema.org/docs) for various Azul facilities.
                    '''
                ),
                'responses': {
                    '200': {
                        'description': 'Contents of the schema',
                        **responses.json_content(
                            schema.object(
                                properties={
                                    '$schema': schema.format('url'),
                                    '$id': schema.format('url'),
                                    'type': schema.schema(JSON)
                                },
                                additionalProperties=True,
                                example=self.get_schema('mirror', 'info', 2)
                            )
                        )
                    }
                }
            }
        )
        def get_schema(facility: str, name: str, version_and_extension: str) -> JSON:
            version = self._parse_version(version_and_extension)
            return self.get_schema(facility, name, version)

        return locals()

    def get_schema(self, facility: str, name: str, version: int) -> JSON:
        path = 'schemas', facility, name, self._format_version(version)
        schema = json.loads(self.app.load_static_resource(*path))
        schema['$id'] = str(self.schema_url(facility, name, version))
        return schema
