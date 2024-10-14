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
import requests

from azul import (
    RequirementError,
    cached_property,
)
from azul.types import (
    JSON,
)

log = logging.getLogger(__name__)


class SchemaValidator:

    def validate_json(self, file_json: JSON, file_name: str):
        try:
            schema = self._download_json_file(file_json['describedBy'])
        except json.decoder.JSONDecodeError as e:
            schema_url = file_json['describedBy']
            raise RequirementError('Failed to parse schema JSON',
                                   file_name, schema_url) from e
        self.validator.evolve(schema=schema)
        try:
            self.validator.validate(file_json)
        except ValidationError as e:
            raise RequirementError(*e.args, file_name) from e

    @lru_cache(maxsize=None)
    def _download_json_file(self, file_url: str) -> JSON:
        response = requests.get(file_url, allow_redirects=False)
        response.raise_for_status()
        return response.json()

    def _retrieve_resource(self, resource_url: str) -> Resource:
        file_json = self._download_json_file(resource_url)
        return Resource.from_contents(file_json)

    @cached_property
    def validator(self) -> Draft202012Validator:
        registry = Registry(retrieve=self._retrieve_resource)
        return Draft202012Validator(schema={}, registry=registry, format_checker=FormatChecker())
