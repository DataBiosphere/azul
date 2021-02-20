from typing import (
    Optional,
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


def trunc_ellipses(string: str, max_len: int) -> str:
    """
    Truncates a string to a specified length, appending a single unicode
    ellipses character to indicate truncated content.
    The returned string, including the ellipses, is never more than `max_len`
    characters long.

    >>> trunc_ellipses('shorter than limit', 50)
    'shorter than limit'

    >>> trunc_ellipses('longer than limit', 5)
    'long…'

    >>> trunc_ellipses('impossible limit', 0)
    Traceback (most recent call last):
    ...
    ValueError: Output cannot be empty

    """
    if max_len < 1:
        raise ValueError('Output cannot be empty')
    if len(string) > max_len:
        string = string[:max_len - 1] + '…'
    assert len(string) <= max_len
    return string
