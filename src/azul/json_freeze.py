from azul.types import JSON
from azul.vendored.frozendict import frozendict


def freeze(x: JSON):
    """
    Return a copy of the argument JSON structure with every `dict` in that structure converted to a `frozendict`.

    Frozen dictionaries can be compared with `<`, `>` and `==`. They can also be used as keys in other dictionaries.
    """
    if isinstance(x, dict):
        return frozendict((k, freeze(v)) for k, v in x.items())
    elif isinstance(x, list):
        return [freeze(v) for v in x]
    elif isinstance(x, (bool, str, int, float)) or x is None:
        return x
    else:
        assert False, f'Cannot handle values of type {type(x)}'


def thaw(x: JSON):
    """
    Return a copy of the argument JSON structure with every `dict` in that structure converted to a `frozendict`.
    """
    if isinstance(x, frozendict):
        return {k: thaw(v) for k, v in x.items()}
    elif isinstance(x, list):
        return [thaw(v) for v in x]
    elif isinstance(x, (bool, str, int, float)) or x is None:
        return x
    else:
        assert False, f'Cannot handle values of type {type(x)}'
