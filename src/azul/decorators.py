from functools import lru_cache


def memoized_property(f):
    return property(lru_cache(maxsize=None)(f))
