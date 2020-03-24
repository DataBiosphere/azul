from typing import (
    Union
)

from azul.openapi import (
    format_description_key,
    schema,
    responses,
)
from azul.openapi.schema import (
    TYPE,
)
from azul.types import (
    JSON,
    AnyJSON,
)


def json_type(name: str, schema_: JSON):
    """
    Create a new type to represent a specific JSON schema.
    Useful for describing openapi parameters that use the `content` property
    to describe a JSON schema.
    :param name: The __name__ attribute of the new type.
    :param schema_: The openapi schema to be typified.
    """
    return type(name, (JSON.__origin__,), {'schema': schema_})


def path(name: str, type_: TYPE, **kwargs: AnyJSON) -> JSON:
    """
    Returns an OpenAPI `parameters` specification of a URL path parameter.
    Note that path parameters cannot be optional.

    >>> from azul.doctests import assert_json
    >>> assert_json(path('foo', int))
    {
        "name": "foo",
        "in": "path",
        "required": true,
        "schema": {
            "type": "integer",
            "format": "int64"
        }
    }
    """
    return _make_param(name, in_='path', type_=type_, **kwargs)


def query(name: str, type_: Union[TYPE, schema.optional], **kwargs: AnyJSON) -> JSON:
    """
    Returns an OpenAPI `parameters` specification of a URL query parameter.

    >>> from azul.doctests import assert_json
    >>> assert_json(query('foo', schema.optional(int)))
    {
        "name": "foo",
        "in": "query",
        "required": false,
        "schema": {
            "type": "integer",
            "format": "int64"
        }
    }
    """
    return _make_param(name, in_='query', type_=type_, **kwargs)


def _make_param(name: str, in_: str, type_: Union[TYPE, schema.optional], **kwargs: AnyJSON) -> JSON:
    is_optional = isinstance(type_, schema.optional)
    if is_optional:
        type_ = type_.type_
    type_property = (responses.wrap_json_schema_content(type_.schema)
                     if isinstance(type_, type) and issubclass(type_, JSON.__origin__) else
                     {'schema': schema.make_type(type_)})
    format_description_key(kwargs)
    return {
        'name': name,
        'in': in_,
        'required': not is_optional,
        **type_property,
        **kwargs
    }
