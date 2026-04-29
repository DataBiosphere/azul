from typing import (
    Any,
)

from azul.lib.strings import (
    format_and_dedent,
)
from azul.lib.types import (
    AnyJSON,
    JSON,
)


def format_description_key(kwargs: dict[str, Any]) -> None:
    """
    Clean up the `description` key's value in `kwargs` (if it exists)

    >>> from azul.lib.doctests import assert_json
    >>> kwargs = {"foo": "bar", "description": '''
    ...                                        Multi-lined,
    ...                                        indented,
    ...                                        triple-quoted string
    ...                                        '''}
    >>> format_description_key(kwargs)
    >>> assert_json(kwargs)
    {
        "foo": "bar",
        "description": "\\nMulti-lined,\\nindented,\\ntriple-quoted string\\n"
    }

    >>> kwargs = {"foo": "bar"}
    >>> format_description_key(kwargs)
    >>> assert_json(kwargs)
    {
        "foo": "bar"
    }
    """
    try:
        unwrapped = format_and_dedent(kwargs['description'])
    except KeyError:
        pass
    else:
        kwargs['description'] = unwrapped


def application_json(schema: JSON, **kwargs: AnyJSON) -> JSON:
    return {
        'application/json': {
            'schema': schema,
            **kwargs
        }
    }
