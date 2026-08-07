from functools import (
    lru_cache,
)
import json
import logging

from jsonschema import (
    FormatChecker,
    ValidationError,
)
from jsonschema.validators import (
    Draft202012Validator,
)
from referencing import (
    Registry,
    Resource,
)

from azul.http import (
    HasCachedHttpClient,
    raise_on_status,
)
from azul.lib import (
    R,
    cached_property,
)
from azul.lib.types import (
    JSON,
)

log = logging.getLogger(__name__)


class SchemaValidator(HasCachedHttpClient):

    def validate_json(self, file_json: JSON, file_name: str):
        try:
            schema = self._download_json_file(file_json['describedBy'])
        except json.decoder.JSONDecodeError:
            schema_url = file_json['describedBy']
            assert False, R(
                'Failed to parse schema JSON', file_name, schema_url)
        self.validator = self.validator.evolve(schema=schema)
        try:
            self.validator.validate(file_json)
        except ValidationError as e:
            assert False, R(*e.args, file_name)

    @lru_cache(maxsize=None)
    def _download_json_file(self, file_url: str) -> JSON:
        response = self._http_client.request('GET', file_url, redirect=False)
        raise_on_status(response)
        return response.json()

    def _retrieve_resource(self, resource_url: str) -> Resource:
        file_json = self._download_json_file(resource_url)
        return Resource.from_contents(file_json)

    @cached_property
    def validator(self) -> Draft202012Validator:
        registry = Registry(retrieve=self._retrieve_resource)
        return Draft202012Validator(schema={},
                                    registry=registry,
                                    format_checker=FormatChecker())
