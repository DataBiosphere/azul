import re
from textwrap import (
    dedent,
)
from typing import (
    Iterable,
    Sequence,
    overload,
)

from more_itertools import (
    minmax,
)

from azul.lib import (
    R,
)


def format_and_dedent(string: str, **kwargs) -> str:
    """
    Remove common leading whitespace from every line in text. Useful for
    processing triple-quote strings.

    If keyword arguments are supplied, they will serve as arguments for
    formatting the dedented string using str.format().

    :param string: The string to unwrap

    >>> format_and_dedent(" c'est \\n une chaine \\n de plusieurs lignes. ")
    "c'est \\nune chaine \\nde plusieurs lignes. "

    >>> format_and_dedent('''
    ...     Multi-lined,
    ...     indented,
    ...     triple-quoted string.
    ... ''')
    '\\nMulti-lined,\\nindented,\\ntriple-quoted string.\\n'

    >>> format_and_dedent('{foo}{bar!r}', foo=123, bar={})
    '123{}'
    """
    dedented = dedent(string)
    return dedented.format(**kwargs) if kwargs else dedented


def to_camel_case(text: str) -> str:
    camel_cased = ''.join(part.title() for part in text.split('_'))
    return camel_cased[0].lower() + camel_cased[1:]


@overload
def departition(before: str, sep: str, after: str | None) -> str: ...


@overload
def departition(before: str | None, sep: str, after: str) -> str: ...


def departition(before: str | None, sep: str, after: str | None) -> str:
    """
    >>> departition(None, '.', 'after')
    'after'

    >>> departition('before', '.', None)
    'before'

    >>> departition('before', '.', 'after')
    'before.after'
    """
    if before is None:
        assert after is not None
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


def splitter(sep: str | None = None, maxsplit: int = -1):
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


def trunc_ellipses[T:(bytes, str, bytearray)](s: T, /, max_len: int) -> T:
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

    >>> trunc_ellipses(bytearray(b'012345'), 5)
    bytearray(b'01...')

    >>> # noinspection PyTypeChecker
    >>> trunc_ellipses(0, 0)
    Traceback (most recent call last):
    ...
    TypeError: ('First argument must be str, bytes or bytearray', <class 'int'>)

    >>> # noinspection PyTypeChecker
    >>> trunc_ellipses('', 0.0)
    Traceback (most recent call last):
    ...
    TypeError: ('max_len argument must be int', <class 'float'>)
    """
    if isinstance(s, str):
        ellipses = '…'
    elif isinstance(s, (bytes, bytearray)):
        ellipses = b'...'
    else:
        raise TypeError('First argument must be str, bytes or bytearray',
                        type(s))
    if not isinstance(max_len, int):
        raise TypeError('max_len argument must be int', type(max_len))
    if len(s) > max_len:
        if max_len < len(ellipses):
            raise ValueError('max_len argument too small to accommodate ellipsis',
                             max_len, len(ellipses))
        s = s[:max_len - len(ellipses)] + ellipses
    assert len(s) <= max_len, (len(s), max_len)
    return s


def longest_common_prefix(strings: Iterable[str]) -> str | None:
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
    assert s2 is not None
    for i, c in enumerate(s1):
        if s2[i] != c:
            return s1[:i]
    return s1


def join_lines(*lines: str) -> str:
    """
    Join the arguments with a newline character.

    >>> join_lines()
    ''

    >>> join_lines('a')
    'a'

    >>> join_lines('a', 'b')
    'a\\nb'
    """
    return '\n'.join(lines)


def join_words(*words: str) -> str:
    """
    Join the arguments with a space character.

    >>> join_words()
    ''

    >>> join_words('a')
    'a'

    >>> join_words('a', 'b')
    'a b'
    """
    return ' '.join(words)


def delimit(s: str, delimiter: str) -> str:
    """
    Prepend and append a delimiter to a string after ensuring that the former
    does not occur in the latter.

    >>> delimit('foo', "'")
    "'foo'"

    >>> delimit("foo's", "'")
    Traceback (most recent call last):
    ...
    AssertionError: R("'", 'must not occur in', "foo's")
    """
    assert delimiter not in s, R(delimiter, 'must not occur in', s)
    return delimiter + s + delimiter


def parenthesize(s: str, parens: str = "()"):
    """
    >>> parenthesize('foo')
    '(foo)'

    >>> parenthesize('(foo)')
    '((foo))'

    >>> parenthesize('foo)')
    Traceback (most recent call last):
    ...
    AssertionError: R('Extra closing construct in input')

    >>> parenthesize('(foo')
    Traceback (most recent call last):
    ...
    AssertionError: R('Missing closing construct in input')

    >>> parenthesize('foo)', '{}')
    '{foo)}'

    >>> parenthesize('foo', '{)')
    '{foo)'

    >>> parenthesize(123, '()')
    Traceback (most recent call last):
    ...
    AssertionError: R('First argument must be string')

    >>> parenthesize('foo', 123)
    Traceback (most recent call last):
    ...
    AssertionError: R('Second argument must be string')

    >>> parenthesize('foo', '(')
    Traceback (most recent call last):
    ...
    AssertionError: R('Second argument must be two characters', '(')

    >>> parenthesize('foo', '||')
    Traceback (most recent call last):
    ...
    AssertionError: R('Second argument must be two different characters', '||')
    """
    assert isinstance(s, str), R('First argument must be string')
    assert isinstance(parens, str), R('Second argument must be string')
    assert len(parens) == 2, R('Second argument must be two characters', parens)
    open, close = iter(parens)
    assert open != close, R("Second argument must be two different characters", parens)

    i = 0
    for c in s:
        if c == open:
            i += 1
        elif c == close:
            i -= 1
        assert i >= 0, R('Extra closing construct in input')
    assert i == 0, R('Missing closing construct in input')
    return open + s + close


def back_quote(*words: str) -> str:
    """
    Join the arguments with a space character and enclose the result in back
    quotes. The arguments must not contain back quotes.

    >>> back_quote()
    '``'

    >>> back_quote('foo', 'bar')
    '`foo bar`'

    >>> back_quote('foo`s')
    Traceback (most recent call last):
    ...
    AssertionError: R('`', 'must not occur in', 'foo`s')
    """
    return delimit(join_words(*words), '`')


def single_quote(*words: str) -> str:
    """
    Join the arguments with a space character and enclose the result in single
    quotes. The arguments must not contain single quotes.

    >>> single_quote()
    "''"

    >>> single_quote('foo', 'bar')
    "'foo bar'"

    >>> single_quote("foo", "bar's")
    Traceback (most recent call last):
    ...
    AssertionError: R("'", 'must not occur in', "foo bar's")
    """
    return delimit(join_words(*words), "'")


def double_quote(*words: str) -> str:
    """
    Join the arguments with a space character and enclose the result in double
    quotes. The arguments must not contain double quotes.

    >>> double_quote()
    '""'

    >>> double_quote('foo', 'bar')
    '"foo bar"'

    >>> double_quote('foo', 'b"a"r')
    Traceback (most recent call last):
    ...
    AssertionError: R('"', 'must not occur in', 'foo b"a"r')
    """
    return delimit(join_words(*words), '"')


_base64url = r'[A-Za-z0-9_-]'

_secret_re = re.compile('|'.join([
    rf'(?i:bearer )?ey[IJ]{_base64url}+\.({_base64url}+)\.({_base64url}+)',
    rf'(?i:bearer )?ya29\.({_base64url}+)',
]))


def redact(s: str, *, fullmatch: bool = False) -> str:
    """
    Find and redact secrets in the given string. Every captured group in a
    match of ``_secret_re`` is redacted; the rest of the match and the
    surrounding text are preserved.

    >>> redact('token=ya29.some_access_token!')
    'token=ya29.sREDACTED!'

    >>> redact('token=eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signature123!')
    'token=eyJhbGciOiJSUzI1NiJ9.eREDACTED0.sREDACTED!'

    >>> redact('no secrets here')
    'no secrets here'

    >>> redact('Authorization: Bearer ya29.some_access_token')
    'Authorization: Bearer ya29.sREDACTED'

    With ``fullmatch=True``, only strings that are entirely a secret are
    redacted:

    >>> redact('token=ya29.some_access_token!', fullmatch=True)
    'token=ya29.some_access_token!'

    >>> redact('ya29.some_access_token', fullmatch=True)
    'ya29.sREDACTED'

    >>> redact('Bearer ya29.some_access_token', fullmatch=True)
    'Bearer ya29.sREDACTED'
    """
    if fullmatch:
        m = _secret_re.fullmatch(s)
        if m is None:
            return s
        else:
            return _redact_groups(m)
    else:
        return _secret_re.sub(_redact_groups, s)


def _redact_groups(m: re.Match) -> str:
    result = m.group(0)
    start = m.start()
    for i in reversed(range(1, len(m.groups()) + 1)):
        if m.group(i) is not None:
            g_start = m.start(i) - start
            g_end = m.end(i) - start
            result = result[:g_start] + _redact(m.group(i)) + result[g_end:]
    return result


def redactable_access_token(s: str) -> bool:
    return s.startswith('ya29.')


def redactable_jwt(s: str) -> bool:
    return s[:3] in ('eyI', 'eyJ')


def _redact(secret: str, *, num_show: int = 3, mask='REDACTED'):
    """
    Replace the center of the given string with the given mask, leaving at most
    ``num_show`` characters unredacted at the beginning and end, and hiding at
    least 90% of the string.

    >>> d = '0123456789'

    >>> _redact('')
    'REDACTED'

    >>> _redact(d[:-1]), _redact(d)
    ('REDACTED', '0REDACTED')

    >>> _redact((d * 2)[:-1]), _redact(d * 2)
    ('0REDACTED', '0REDACTED9')

    >>> _redact((d * 6)[:-1]), _redact(d * 6)
    ('012REDACTED78', '012REDACTED789')

    >>> _redact(d * 10)
    '012REDACTED789'

    >>> _redact((d * 4)[:-1], num_show=2), _redact(d * 4, num_show=2)
    ('01REDACTED8', '01REDACTED89')

    >>> _redact((d * 2)[:-1], num_show=1), _redact(d * 2, num_show=1)
    ('0REDACTED', '0REDACTED9')

    >>> _redact(d[:-1], num_show=0), _redact(d, num_show=0)
    ('REDACTED', 'REDACTED')
    """
    n = len(secret)
    hide = (1 + n) * 9 // 10
    reveal = min(n - hide, 2 * num_show)
    back = reveal // 2
    front = reveal - back
    return secret[:front] + mask + secret[n - back:]
