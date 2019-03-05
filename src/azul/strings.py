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
