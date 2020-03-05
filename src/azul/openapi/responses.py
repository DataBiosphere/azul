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
    unwrap_description(kwargs)
    return {
        'schema': schema.make_type(type_),
        **kwargs
    }
