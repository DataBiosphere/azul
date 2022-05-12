from collections.abc import (
    Iterable,
    Iterator,
)
from functools import (
    partial,
)
from itertools import (
    islice,
)
import random as _random
from typing import (
    Callable,
    TypeVar,
)

from azul import (
    require,
)

T = TypeVar('T')


# noinspection PyPep8Naming
class generable(Iterable[T]):
    """
    Convert a generator into a true iterable, i.e. an iterable that is not an
    iterator i.e., whose ``__iter__`` does not return ``self`` and that does not
    have ``__next__``.

    A generator function:

    >>> def f(n):
    ...     for i in range(n):
    ...         yield i

    It returns an iterator that can only be consumed once:

    >>> g = f(3)
    >>> list(g)
    [0, 1, 2]
    >>> list(g)
    []

    Wrapping the generator function with ``generable`` produces a true iterable
    that can be consumed multiple times:

    >>> g = generable(f, 3)
    >>> list(g)
    [0, 1, 2]
    >>> list(g)
    [0, 1, 2]
    """

    def __init__(self, generator: Callable[..., Iterator[T]], *args, **kwargs):
        self._generator = partial(generator, *args, **kwargs)

    def __iter__(self) -> Iterator[T]:
        return self._generator()


def reservoir_sample(k: int,
                     it: Iterable[T],
                     *,
                     random: _random.Random = _random
                     ) -> list[T]:
    """
    Return a random choice of a given size from an iterable.

    https://stackoverflow.com/a/35671225/4171119

    >>> r = _random.Random(42)

    >>> reservoir_sample(5, '', random=r)
    []

    >>> reservoir_sample(5, 'abcd', random=r)
    ['c', 'b', 'd', 'a']

    >>> reservoir_sample(0, 'abcd', random=r)
    []

    >>> reservoir_sample(5, 'abcdefghijklmnopqrstuvwxyz', random=r)
    ['x', 'l', 'a', 'n', 'b']
    """
    if k == 0:
        return []
    require(k > 0, 'Sample size must not be negative', k, exception=ValueError)
    it = iter(it)
    sample = list(islice(it, k))
    random.shuffle(sample)
    for i, item in enumerate(it, start=k + 1):
        j = random.randrange(i)
        if j < k:
            sample[j] = item
    return sample
