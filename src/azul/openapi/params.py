from typing import (
    Union
)

from azul.openapi import (
    schema,
    unwrap_description,
)
from azul.openapi.schema import (
    TYPE,
)
from azul.types import (
    JSON,
    PrimitiveJSON,
)


def path(name: str, type_: TYPE, **kwargs: PrimitiveJSON) -> JSON:
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


def query(name: str, type_: Union[TYPE, schema.optional], **kwargs: PrimitiveJSON) -> JSON:
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


def _make_param(name: str, in_: str, type_: Union[TYPE, schema.optional], **kwargs: PrimitiveJSON) -> JSON:
    is_optional = isinstance(type_, schema.optional)
    if is_optional:
        type_ = type_.type_
    unwrap_description(kwargs)
    return {
        'name': name,
        'in': in_,
        'required': not is_optional,
        'schema': schema.make_type(type_),
        **kwargs
    }
