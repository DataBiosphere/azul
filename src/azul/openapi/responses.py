from azul.openapi import (
    application_json,
    format_description_key,
    schema,
)
from azul.openapi.schema import (
    TYPE,
)
from azul.types import (
    AnyJSON,
    JSON,
    PrimitiveJSON,
)


def json_content(schema: JSON, **kwargs: AnyJSON) -> JSON:
    """
    Useful for specifying response bodies and request parameters that are JSON.
    """
    return {
        'content': application_json(schema, **kwargs),
    }


def header(type_: TYPE, **kwargs: PrimitiveJSON) -> JSON:
    """
    Returns the schema and description for a response header.

    >>> from azul.doctests import assert_json
    >>> assert_json(header(float, description='futz'))
    {
        "schema": {
            "type": "number",
            "format": "double"
        },
        "description": "futz"
    }
    """
    format_description_key(kwargs)
    return {
        'schema': schema.make_type(type_),
        **kwargs
    }
