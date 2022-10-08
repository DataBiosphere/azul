from functools import (
    lru_cache,
)
import json
import logging

from jsonschema import (
    FormatChecker,
    ValidationError,
    validate,
)
import requests

from humancellatlas.data.metadata.api import (
    JSON,
)
from humancellatlas.data.metadata.helpers.exception import (
    RequirementError,
)

logger = logging.getLogger(__name__)


class SchemaValidator:

    def validate_json(self, file_json: JSON, file_name: str):
        try:
            schema = self._download_schema(file_json['describedBy'])
        except json.decoder.JSONDecodeError as e:
            schema_url = file_json['describedBy']
            raise RequirementError('Failed to parse schema JSON',
                                   file_name, schema_url) from e
        try:
            validate(file_json, schema, format_checker=FormatChecker())
        except ValidationError as e:
            raise RequirementError(*e.args, file_name) from e

    @lru_cache(maxsize=None)
    def _download_schema(self, schema_url: str) -> JSON:
        response = requests.get(schema_url, allow_redirects=False)
        response.raise_for_status()
        return response.json()
