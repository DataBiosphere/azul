from azul.openapi import (
    format_description_key,
    schema,
)
from azul.openapi.schema import (
    TYPE,
)
from azul.types import (
    JSON,
    PrimitiveJSON,
)


def wrap_json_schema_content(json):
    return {'content': {'application/json': {'schema': json}}}


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
