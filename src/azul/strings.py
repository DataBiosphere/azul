from typing import (
    Iterable,
    Optional,
    Sequence,
    TypeVar,
)

from more_itertools import (
    minmax,
)


def to_camel_case(text: str) -> str:
    camel_cased = ''.join(part.title() for part in text.split('_'))
    return camel_cased[0].lower() + camel_cased[1:]


def departition(before: Optional[str], sep: str, after: Optional[str]) -> str:
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


def pluralize(word: str, count: int = 0) -> str:
    """
    Appends 's' or 'es' to `word` following common patterns in English spelling
    if `count` indicates that the word should be pluralized.

    >>> pluralize('foo')
    'foos'

    >>> pluralize('foo', 0)
    'foos'

    >>> pluralize('foo', 1)
    'foo'

    >>> pluralize('bar', 2)
    'bars'

    >>> pluralize('baz', 2)
    'bazes'

    >>> pluralize('huh')
    'huhs'

    >>> pluralize('hush', 2)
    'hushes'

    >>> pluralize('worry', 2)
    'worries'

    >>> pluralize('woman', 2)
    'womans'
    """
    if count == 1:
        return word
    elif word[-1] in 'sxz' or word[-2:] in ['sh', 'ch']:
        return word + 'es'
    elif word[-1] == 'y':
        return word[:-1] + 'ies'
    else:
        return word + 's'


def join_grammatically(strings: Sequence[str],
                       *,
                       joiner: str = ', ',
                       last_joiner: str = ' and '
                       ) -> str:
    """
    >>> join_grammatically([])
    ''

    >>> join_grammatically(['a'])
    'a'

    >>> join_grammatically(['a','b'])
    'a and b'

    >>> join_grammatically(['a', 'b', 'c'])
    'a, b and c'

    >>> join_grammatically(['a', 'b', 'c'], last_joiner=' or ')
    'a, b or c'
    """
    head, tail = strings[:-2], strings[-2:]
    return joiner.join([*head, last_joiner.join(tail)])


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
    value, including the ellipses, is never longer than the specified number of
    characters (bytes).

    >>> trunc_ellipses('shorter than limit', 50)
    'shorter than limit'

    >>> trunc_ellipses('longer than limit', 5)
    'long…'

    >>> trunc_ellipses('impossible limit', 0)
    Traceback (most recent call last):
    ...
    ValueError: ('max_len argument too small to accommodate ellipsis', 0, 1)

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
    ValueError: ('max_len argument too small to accommodate ellipsis', 1, 3)

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
            raise ValueError('max_len argument too small to accommodate ellipsis',
                             max_len, len(ellipses))
        s = s[:max_len - len(ellipses)] + ellipses
    assert len(s) <= max_len, (len(s), max_len)
    return s


def longest_common_prefix(strings: Iterable[str]) -> Optional[str]:
    """
    >>> lcs = longest_common_prefix
    >>> lcs([])
    >>> lcs([''])
    ''
    >>> lcs(['','a'])
    ''
    >>> lcs(['a', 'b'])
    ''
    >>> lcs(['aa', 'a'])
    'a'
    >>> lcs(['abc', 'ab', 'a'])
    'a'

    Input is traversed exactly once, so an iterator can be passed as well.

    >>> lcs(iter(['abc', 'ab', 'a']))
    'a'
    """
    s1, s2 = minmax(strings, default=(None, None))
    if s1 is None:
        return None
    for i, c in enumerate(s1):
        if s2[i] != c:
            return s1[:i]
    return s1
