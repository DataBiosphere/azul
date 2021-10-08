from itertools import (
    islice,
)
import random as _random
from typing import (
    Iterable,
    List,
    TypeVar,
)

from azul import (
    require,
)

T = TypeVar('T')


def reservoir_sample(k: int,
                     it: Iterable[T],
                     *,
                     random: _random.Random = _random
                     ) -> List[T]:
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
