import json
import logging
from typing import (
    Callable,
    Mapping,
    Optional,
)

import jsonschema
from more_itertools import (
    one,
)

from azul import (
    RequirementError,
    reject,
)
from azul.openapi.schema import (
    type_lookup,
)
from azul.plugins.metadata.hca.transform import (
    value_and_unit,
)
from azul.types import (
    JSON,
)
from azul.uuids import (
    InvalidUUIDError,
    InvalidUUIDVersionError,
    validate_uuid,
)

logger = logging.getLogger(__name__)


class ValidationError(Exception):

    def __init__(self,
                 message,
                 name,
                 value,
                 schema: Optional[JSON] = None):
        self.message = message
        self.name = name
        self.value = value
        if schema is not None:
            self.schema = schema


class SpecValidator:

    def __init__(self,
                 *,
                 name: str,
                 required: bool,
                 default: str = None,
                 schema: Mapping[str, str] = None,
                 content: Mapping[str, str] = None,
                 pattern: Optional[str] = None,
                 validator: Callable,
                 **kwargs):
        """
        :param kwargs: Since 'in' is a Python reserved word we can only extract it
         using kwargs. All other kwargs are ignored.
        """
        self.in_ = kwargs['in']
        self.json_validators = dict(jsonschema.Draft4Validator.VALIDATORS)
        self.name = name
        self.required = required
        self.pattern = pattern
        self.validator = validator
        self.default = default
        self.schema = schema
        self.content = content

    def validate(self, param_value: str):
        raise NotImplementedError()


class SchemaSpecValidator(SpecValidator):

    def __init__(self,
                 *,
                 name: str,
                 required: bool,
                 pattern: Optional[str] = None,
                 schema: Mapping,
                 **kwargs):
        reject('content' in kwargs)
        super().__init__(name=name,
                         required=required,
                         default=schema.get('default'),
                         schema=schema,
                         pattern=pattern,
                         validator=type_lookup(schema['type']),
                         **kwargs)

    def validate(self, param_value: str):
        """
        Validates if a given request parameter meets the specification.
        :param param_value: The value of the parameter to be validated
        """
        try:
            self.validator(param_value)
        except (TypeError, ValueError):
            raise ValidationError('Invalid parameter', self.name, param_value)
        schema_type = self.schema['type']
        if schema_type == 'string':
            if self.schema.get('format') == 'uuid':
                # noinspection PyUnusedLocal
                def uuid_validator(validator, value, instance, schema):
                    try:
                        validate_uuid(instance)
                    except (InvalidUUIDError, InvalidUUIDVersionError) as e:
                        yield jsonschema.exceptions.ValidationError(e.args[0])

                self.json_validators['format'] = uuid_validator
        # jsonschema validator does not work with strings of integers, must
        # cast them.
        elif schema_type == 'integer':
            param_value = int(param_value)
        elif schema_type == 'array':
            param_value = param_value.split(',')
        json_validator = jsonschema.validators.create(meta_schema=jsonschema.Draft4Validator.META_SCHEMA,
                                                      validators=self.json_validators)
        schema_validator = json_validator(self.schema)
        try:
            schema_validator.validate(param_value)
        except jsonschema.exceptions.ValidationError as e:
            if e.validator == 'pattern':
                message = 'Invalid characters within parameter'
            elif e.validator == 'format' and e.validator_value == 'uuid':
                message = e.message
            else:
                message = 'Invalid parameter'
            raise ValidationError(message=message,
                                  name=self.name,
                                  value=param_value,
                                  schema=e.schema)


class ContentSpecValidator(SpecValidator):

    def __init__(self,
                 *,
                 name: str,
                 required: bool,
                 pattern: Optional[str] = None,
                 content: Mapping,
                 **kwargs):
        reject('schema' in kwargs)
        super().__init__(name=name,
                         required=required,
                         default=content['application/json']['schema']['default'],
                         content=content,
                         pattern=pattern,
                         validator=type_lookup(content['application/json']['schema']['type']),
                         **kwargs)

    def validate(self, param_value: str):
        """
        Validates if a given request parameter meets the specification.
        :param param_value: The value of the parameter to be validated
        """
        schema = self.content['application/json']['schema']
        try:
            filter_json = json.loads(param_value)
        except json.decoder.JSONDecodeError:
            raise ValidationError('Invalid JSON', self.name, param_value)
        if type(filter_json) is not dict:
            raise ValidationError('Must be a JSON object', self.name, param_value)
        else:
            for facet, filter_ in filter_json.items():
                try:
                    relation, value = one(filter_.items())
                except ValueError:
                    raise ValidationError(message=f"The 'filters' parameter entry for {facet!r} may "
                                                  f"only specify a single relation",
                                          name=self.name,
                                          value=filter_json)
                except AttributeError:
                    raise ValidationError(message=f"The 'filters' parameter value for {facet!r} must be "
                                                  f"a JSON object",
                                          name=self.name,
                                          value=filter_json)
                try:
                    jsonschema.validate({facet: filter_}, schema)
                except jsonschema.exceptions.ValidationError as e:
                    schema_info = {'type': schema['type'], 'properties': schema['properties']}
                    if (
                        len(e.absolute_schema_path) == 1
                        and 'additionalProperties' in e.absolute_schema_path
                    ):
                        message = f'Unknown facet {facet!r}'
                    elif 'oneOf' and 'required' in e.absolute_schema_path:
                        message = f'Unknown relation in the {self.name!r} parameter entry for {facet!r}'
                    elif 'minItems' or 'maxItems' in e.absolute_schema_path:
                        message = (f'The value of the {relation!r} relation in the {self.name!r} parameter entry '
                                   f'for {facet!r} is invalid')
                    else:
                        message = 'Invalid filter'
                        schema_info = None
                    raise ValidationError(message=message,
                                          name=self.name,
                                          value=param_value,
                                          schema=schema_info)
                # FIXME: Remove this special case
                #        https://github.com/DataBiosphere/azul/issues/2254
                if facet == 'organismAge':
                    relation, values = one(filter_.items())
                    for value in values:
                        try:
                            value_and_unit.to_index(value)
                        except RequirementError as e:
                            raise ValidationError(message=e,
                                                  name=self.name,
                                                  value=param_value,
                                                  schema=schema)
