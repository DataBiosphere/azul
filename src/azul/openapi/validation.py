from abc import (
    ABC,
    abstractmethod,
)
from copy import (
    copy,
)
import json
import logging
from typing import (
    Any,
    Dict,
    Optional,
    TypeVar,
)

import attr
import jsonschema
from more_itertools import (
    one,
)

from azul import (
    RequirementError,
)
from azul.openapi.schema import (
    python_type_for,
)
from azul.types import (
    JSON,
)

logger = logging.getLogger(__name__)


@attr.s(auto_attribs=True, kw_only=False, auto_exc=True)
class ValidationError(Exception):
    message: str
    name: str
    value: Any
    content: Optional[JSON] = None
    schema: Optional[JSON] = None

    def invalid_parameter_filter(self, _, value):
        return True if value is not None else False


V = TypeVar('V', bound='SpecValidator')


@attr.s(auto_attribs=True, kw_only=True)
class SpecValidator(ABC):
    """
    A base class for OpenAPI specification validators. See
    https://swagger.io/specification/#parameter-object for details about
    parameter specification.
    """
    name: str
    required: bool
    in_: str
    description: Optional[str] = None
    deprecated: bool = False
    default: Optional[str] = None
    schema: Optional[JSON] = None

    @abstractmethod
    def validate(self, param_value: str):
        """
        Validates if a parameter value meets the specification. Raises a
        ValidationError if the parameter value is invalid.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_missing_spec(self) -> Dict[str, JSON]:
        """
        :return: Dictionary with parameter specifications
        """
        raise NotImplementedError()

    @classmethod
    def from_spec(cls, spec: JSON) -> V:
        """
        Returns the applicable validator class for a parameter
        """
        # noinspection PyTypeChecker
        kwargs: Dict[str, JSON] = copy(spec)
        kwargs['in_'] = kwargs.pop('in')
        if 'content' in kwargs:
            assert 'schema' not in kwargs
            return ContentSpecValidator(**kwargs)
        elif 'schema' in kwargs:
            return SchemaSpecValidator(**kwargs)
        else:
            assert False


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class SchemaSpecValidator(SpecValidator):
    """
    SchemaSpecValidators are used to validate parameters that have
    a `schema` property.
    """

    schema: JSON

    def __attrs_post_init__(self):
        object.__setattr__(self, 'default', self.schema.get('default'))

    def get_python_validator(self, schema):
        """
        Returns the applicable python_type for the OpenAPI type defined
        within the schema.
        """
        if schema['type'] == 'array':
            return lambda param_value: list(param_value.split(','))
        else:
            return python_type_for(schema['type'])

    def get_missing_spec(self) -> Dict[str, JSON]:
        return {
            'name': self.name,
            'in': self.in_,
            'required': self.required,
            'schema': self.schema
        }

    def validate(self, param_value: str):
        validator = self.get_python_validator(self.schema)
        try:
            param_value = validator(param_value)
        except (TypeError, ValueError):
            raise ValidationError('Invalid parameter', self.name, param_value)
        try:
            jsonschema.validate(param_value, self.schema)
        except jsonschema.exceptions.ValidationError as e:
            if e.validator == 'pattern':
                message = 'Invalid characters within parameter'
            else:
                message = 'Invalid parameter'
            raise ValidationError(message=message,
                                  name=self.name,
                                  value=param_value,
                                  schema=e.schema)


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class ContentSpecValidator(SpecValidator):
    """
    ContentSpecValidators are used to validate parameters that have a
    `content` property.
    """

    content: JSON

    def __attrs_post_init__(self):
        object.__setattr__(self,
                           'default',
                           self.content['application/json']['schema']['default'])
        object.__setattr__(self,
                           'schema',
                           self.content['application/json']['schema'])

    def get_missing_spec(self) -> Dict[str, JSON]:
        return {
            'name': self.name,
            'in': self.in_,
            'required': self.required,
            'content': self.content
        }

    def validate(self, param_value: str):
        try:
            filter_json = json.loads(param_value)
        except json.decoder.JSONDecodeError:
            raise ValidationError(message='Invalid JSON',
                                  name=self.name,
                                  value=param_value,
                                  content=self.content)
        if type(filter_json) is not dict:
            raise ValidationError(message='Must be a JSON object',
                                  name=self.name,
                                  value=param_value,
                                  content=self.content)
        else:
            for facet, filter_ in filter_json.items():
                try:
                    relation, value = one(filter_.items())
                except ValueError:
                    raise ValidationError(message=f"The 'filters' parameter entry for"
                                                  f" {facet!r} may only specify a single relation",
                                          name=self.name,
                                          value=param_value,
                                          content=self.content)
                except AttributeError:
                    raise ValidationError(message=f"The 'filters' parameter value for"
                                                  f" {facet!r} must be a JSON object",
                                          name=self.name,
                                          value=param_value,
                                          content=self.content)
                try:
                    jsonschema.validate({facet: filter_}, self.schema)
                except jsonschema.exceptions.ValidationError as e:
                    if (
                        len(e.absolute_schema_path) == 1
                        and 'additionalProperties' in e.absolute_schema_path
                    ):
                        message = f'Unknown facet {facet!r}'
                    elif 'oneOf' and 'required' in e.absolute_schema_path:
                        message = f'Unknown relation in the {self.name!r} parameter entry for {facet!r}'
                    elif 'minItems' or 'maxItems' in e.absolute_schema_path:
                        message = (f'The value of the {relation!r} relation in the {self.name!r}'
                                   f' parameter entry for {facet!r} is invalid')
                    else:
                        message = 'Invalid filter'
                    raise ValidationError(message=message,
                                          name=self.name,
                                          value=param_value,
                                          content=self.content)
                # FIXME: Remove this special case
                #        https://github.com/DataBiosphere/azul/issues/2254
                if facet == 'organismAge':
                    relation, values = one(filter_.items())
                    from azul.plugins.metadata.hca.transform import (
                        value_and_unit,
                    )
                    for value in values:
                        try:
                            value_and_unit.to_index(value)
                        except RequirementError as e:
                            raise ValidationError(message=str(e),
                                                  name=self.name,
                                                  value=param_value,
                                                  content=self.content)
