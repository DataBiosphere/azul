from typing import (
    Optional,
    TypeVar,
)


def to_camel_case(text: str):
    camel_cased = ''.join(part.title() for part in text.split('_'))
    return camel_cased[0].lower() + camel_cased[1:]


def departition(before, sep, after):
    """
    >>> departition(None, '.', 'after')
    'after'

    >>> departition('before', '.', None)
    'before'

    >>> departition('before', '.', 'after')
    'before.after'
    """
    if before is None:
        return after
    elif after is None:
        return before
    else:
        return before + sep + after


def pluralize(word: str, count: int) -> str:
    """
    Appends 's' or 'es' to `word` following common patterns in English spelling
    if `count` indicates that the word should be pluralized.

    >>> pluralize('foo', 1)
    'foo'

    >>> pluralize('bar', 2)
    'bars'

    >>> pluralize('baz', 2)
    'bazes'

    >>> pluralize('woman', 2)
    'womans'
    """
    result = word
    if count != 1:
        if word[-1] in 'sxz' or word[-2:] in ['sh', 'ch']:
            result += 'es'
        else:
            result += 's'
    return result


def splitter(sep: Optional[str] = None, maxsplit: int = -1):
    """
    Main use case:

    >>> list(map(splitter('/'),['a/b', 'c/d']))
    [['a', 'b'], ['c', 'd']]

    >>> splitter()(' ')
    []

    >>> splitter(maxsplit=1)('a b c')
    ['a', 'b c']

    >>> splitter(None, 1)('a b c')
    ['a', 'b c']
    """
    return lambda s: s.split(sep, maxsplit)


STRING = TypeVar('STRING', str, bytes)


def trunc_ellipses(s: STRING, /, max_len: int) -> STRING:
    """
    Truncates a string (bytes array) to the specified length, appending an
    ellipses character (sequence of three dots) to indicate truncation, if the
    argument is longer. Otherwise, returns the argument unchanged. The return
    value, including the ellipses is never longer than the specified number of
    characters (bytes).

    >>> trunc_ellipses('shorter than limit', 50)
    'shorter than limit'

    >>> trunc_ellipses('longer than limit', 5)
    'long…'

    >>> trunc_ellipses('impossible limit', 0)
    Traceback (most recent call last):
    ...
    ValueError: ('max_len argument to small to accomodate ellipsis', 0, 1)

    Edge cases with strings and byte arrays:

    >>> trunc_ellipses('', 0)
    ''

    >>> trunc_ellipses('01', 1)
    '…'

    >>> trunc_ellipses(b'', 0)
    b''

    >>> trunc_ellipses(b'0', 1)
    b'0'

    >>> trunc_ellipses(b'01', 1)
    Traceback (most recent call last):
    ...
    ValueError: ('max_len argument to small to accomodate ellipsis', 1, 3)

    >>> trunc_ellipses(b'012', 3)
    b'012'

    >>> trunc_ellipses(b'0123', 3)
    b'...'

    >>> # noinspection PyTypeChecker
    >>> trunc_ellipses(0, 0)
    Traceback (most recent call last):
    ...
    TypeError: ('First argument must be str or bytes', <class 'int'>)

    >>> # noinspection PyTypeChecker
    >>> trunc_ellipses('', 0.0)
    Traceback (most recent call last):
    ...
    TypeError: ('max_len argument must be int', <class 'float'>)
    """
    if isinstance(s, str):
        ellipses = '…'
    elif isinstance(s, bytes):
        ellipses = b'...'
    else:
        raise TypeError('First argument must be str or bytes', type(s))
    if not isinstance(max_len, int):
        raise TypeError('max_len argument must be int', type(max_len))
    if len(s) > max_len:
        if max_len < len(ellipses):
            raise ValueError('max_len argument to small to accomodate ellipsis',
                             max_len, len(ellipses))
        s = s[:max_len - len(ellipses)] + ellipses
    assert len(s) <= max_len, (len(s), max_len)
    return s
