from contextlib import (
    contextmanager,
)
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


class CachedProperty(object):
    """
    Similar to :class:`property`, except that the getter is only called once.
    This is commonly used to implement lazily initialized attributes.

    Inspired by boltons' cachedproperty:

    https://github.com/mahmoud/boltons/blob/20.2.0/boltons/cacheutils.py#L592

    This implementation is different from boltons' in that it renames the
    ``func`` attribute to ``fget``, and adds the ``fdel`` and ``fset`` methods
    for clearing and priming the cache respectively. These methods were named
    to match those of similar functionality in `:class:`property`.

    Note that despite the presence of `fset` and `fdel`, this class is still a
    non-data descriptor (__set__ and __delete__ are absent). Only for non-data
    descriptors does ``c.__dict__['p']`` take precedence over `C.p.__get__(c)`
    which is the central principle this class relies on.

    https://docs.python.org/3/reference/datamodel.html#invoking-descriptors

    >>> class C:
    ...     x = iter(range(10))
    ...     @CachedProperty
    ...     def p(self):
    ...         return next(self.x)

    >>> c = C()

    The first property access invokes the getter and caches the returned value:

    >>> c.p
    0

    The second access does not invoke the getter but returns the cached value:

    >>> c.p
    0

    Clear the cache:

    >>> C.p.fdel(c)

    The first property access invokes the getter again, the second one does not:

    >>> c.p, c.p
    (1, 1)

    Prime the cache with a different value:

    >>> C.p.fset(c,42)

    Property access returns that value and the getter is not invoked:

    >>> c.p, c.p
    (42, 42)

    Clear the cache again, removing the primed value:

    >>> C.p.fdel(c)

    The getter is invoked on the 1st but not the 2nd access after that:

    >>> c.p, c.p
    (2, 2)

    Enter the `stash` context manager with a cached value of 2, then reset cache

    >>> with C.p.stash(c): c.p
    3

    After exiting the context manager the originally cached value is restored

    >>> c.p
    2

    Enter the context manager without a cached value

    >>> C.p.fdel(c)
    >>> with C.p.stash(c): c.p
    4

    After exiting context manager, cache is restored to being clear

    >>> c.p
    5
    """

    def __init__(self, fget):
        self.__doc__ = getattr(fget, '__doc__')
        self.__isabstractmethod__ = getattr(fget, '__isabstractmethod__', False)
        self.fget = fget

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        else:
            value = obj.__dict__[self.fget.__name__] = self.fget(obj)
            return value

    def __repr__(self):
        name = self.__class__.__name__
        return '<%s func=%s>' % (name, self.fget)

    def fset(self, obj, value):
        obj.__dict__[self.fget.__name__] = value

    def fdel(self, obj):
        obj.__dict__.pop(self.fget.__name__, None)

    @contextmanager
    def stash(self, obj):
        missing = object()
        val = obj.__dict__.get(self.fget.__name__, missing)
        self.fdel(obj)
        try:
            yield
        finally:
            if val is missing:
                self.fdel(obj)
            else:
                self.fset(obj, val)
