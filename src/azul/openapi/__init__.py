from textwrap import dedent
from typing import (
    Any,
    MutableMapping,
)

from azul.types import (
    AnyJSON,
    JSON,
)


def format_description(string: str, *args, **kwargs) -> str:
    """
    Remove common leading whitespace from every line in text.
    Useful for processing triple-quote strings.

    If *args and **kwargs are supplied, they will serve as arguments for
    formatting the dedented string.

    :param string: The string to unwrap

    >>> format_description(" c'est \\n une chaine \\n de plusieurs lignes. ")
    "c'est \\nune chaine \\nde plusieurs lignes. "

    >>> format_description('''
    ...     Multi-lined,
    ...     indented,
    ...     triple-quoted string.
    ... ''')
    '\\nMulti-lined,\\nindented,\\ntriple-quoted string.\\n'

    >>> format_description('''
    ...     Programming is like {verb_ending_in_ing} a {adjective} {noun}.
    ...     Compare {!s} to {}.
    ... ''', 'apple', 'orange', verb_ending_in_ing='peeling', adjective='imaginary', noun='innertube')
    '\\nProgramming is like peeling a imaginary innertube.\\nCompare apple to orange.\\n'
    """
    dedented = dedent(string)
    return dedented.format(*args, **kwargs) if args or kwargs else dedented


def format_description_key(kwargs: MutableMapping[str, Any]) -> None:
    """
    Clean up the `description` key's value in `kwargs` (if it exists)

    >>> from azul.doctests import assert_json
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
        unwrapped = format_description(kwargs['description'])
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
