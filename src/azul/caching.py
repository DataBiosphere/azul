from functools import (
    lru_cache,
    wraps,
)
from threading import (
    get_ident,
)


def lru_cache_per_thread(maxsize=128, typed=False):
    """
    Same as :func:`functools.lru_cache` but caches separately per thread. The
    cache is shared by all threads but calls to the wrapped function from
    different threads are cached separately even if the arguments of those calls
    are equal. The maxsize argument applies globally.

    >>> from itertools import count
    >>> from time import sleep
    >>> from concurrent.futures.thread import ThreadPoolExecutor

    A cached function that returns its argument and the number of prior
    invocations:

    >>> i = count()
    >>> @lru_cache_per_thread
    ... def f(n):
    ...     return n, next(i)

    If the function is called a second time with the same argument, the cached
    return value from the first, uncached invocation is returned, the function
    body isn't actually executed …

    >>> list(map(f, [0, 1, 0, 1]))
    [(0, 0), (1, 1), (0, 0), (1, 1)]

    … and the counter only reflects the uncached invocations:

    >>> next(i)
    2

    The same sequence of invocations returns a new set of return values,
    unaffected by whatever was cached for the main thread:

    >>> with ThreadPoolExecutor(max_workers=1) as tpe:
    ...     list(tpe.map(f, [0, 1, 0, 1]))
    [(0, 3), (1, 4), (0, 3), (1, 4)]

    >>> next(i)
    5

    Same function, but at most one cache line. Each call displaces the cached
    result of the previous one.

    >>> i = count()
    >>> @lru_cache_per_thread(maxsize=1)
    ... def g(n):
    ...     return n, next(i)
    >>> list(map(g, [0, 1, 0, 1]))
    [(0, 0), (1, 1), (0, 2), (1, 3)]

    >>> next(i)
    4
    """

    def decorator(f):
        @lru_cache(maxsize=maxsize, typed=typed)
        def caller(tid_, *args, **kwargs):
            return f(*args, **kwargs)

        @wraps(f)
        def wrapper(*args, **kwargs):
            return caller(get_ident(), *args, **kwargs)

        return wrapper

    if callable(maxsize):
        # Unparenthesized usage as in
        # @lru_cache_per_thread
        # def g(): ...
        f, maxsize = maxsize, 128
        return decorator(f)
    else:
        # "Normal" usage as in
        # @lru_cache_per_thread(…)
        # def g(): …
        return decorator
