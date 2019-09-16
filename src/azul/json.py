from io import StringIO
import json

from azul.types import AnyJSON


def json_head(n: int, o: AnyJSON) -> str:
    """
    Return the first n characters of a serialized JSON structure.

    >>> json_head(0, {})
    ''
    >>> json_head(1, {})
    '{'
    >>> json_head(2, {})
    '{}'
    >>> json_head(3, {})
    '{}'
    >>> json_head(0, "x")
    ''
    >>> json_head(1, "x")
    '"'
    >>> json_head(2, "x")
    '"x'
    >>> json_head(3, "x")
    '"x"'
    >>> json_head(4, "x")
    '"x"'
    """
    buf = StringIO()
    for chunk in json.JSONEncoder().iterencode(o):
        buf.write(chunk)
        if buf.tell() > n:
            break
    return buf.getvalue()[:n]
