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


def unwrap(string: str):
    """
    Remove newlines and leading/trailing whitespace, and consolidate
    newline-adjacent whitespace.
    Useful for processing triple-quote strings.

    >>> unwrap(' ce nest pas \\n une chaine \\n de plusieurs lignes. ')
    'ce nest pas une chaine de plusieurs lignes.'

    >>> unwrap('''
    ...        Multi-lined,
    ...        indented,
    ...        triple-quoted string.
    ...        ''')
    'Multi-lined, indented, triple-quoted string.'
    """
    return ' '.join(filter(None, (line.strip() for line in string.split('\n'))))
