from typing import (
    Union
)

from azul.openapi import (
    format_description_key,
    schema,
)
from azul.openapi.schema import (
    TYPE,
)
from azul.types import (
    AnyJSON,
    JSON,
)


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
    format_description_key(kwargs)
    schema_or_content = schema.make_type(type_)
    return {
        'name': name,
        'in': in_,
        'required': not is_optional,
        # https://swagger.io/docs/specification/describing-parameters/#schema-vs-content
        'content' if 'application/json' in schema_or_content else 'schema': schema_or_content,
        **kwargs
    }
