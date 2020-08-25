from abc import (
    ABCMeta,
    abstractmethod,
)
import email.utils
import time
from typing import (
    Optional,
)

from azul import (
    require,
)
from azul.types import (
    LambdaContext,
)


class RemainingTime(metaclass=ABCMeta):
    """
    A monotonically decreasing, non-negative estimate of time remaining in a particular context
    """

    @abstractmethod
    def get(self) -> float:
        """
        Returns the estimated remaining time in seconds
        """
        raise NotImplementedError


class RemainingLambdaContextTime(RemainingTime):
    """
    The estimated running time in an AWS Lambda context
    """

    def __init__(self, context: LambdaContext) -> None:
        super().__init__()
        self._context = context

    def get(self) -> float:
        return self._context.get_remaining_time_in_millis() / 1000


class RemainingTimeUntil(RemainingTime):
    """
    The remaining wall clock time up to a given absolute deadline in terms of time.time()
    """

    def __init__(self, deadline: float) -> None:
        super().__init__()
        self._deadline = deadline

    def get(self) -> float:
        return max(0.0, self._deadline - time.time())


class SpecificRemainingTime(RemainingTimeUntil):
    """
    A specific relative amount of wall clock time in seconds
    """

    def __init__(self, amount: float) -> None:
        require(amount >= 0, "Inital remaining time must be non-negative")
        super().__init__(time.time() + amount)


class AdjustedRemainingTime(RemainingTime):
    """
    Some other estimate of remaining time, adjusted by a fixed offset. Use a negative offset to reduce the remaining
    time or a positive offset to increase it.
    """

    def __init__(self, offset: float, actual: RemainingTime) -> None:
        super().__init__()
        self._offset = offset
        self._actual = actual

    def get(self) -> float:
        return max(0.0, self._actual.get() + self._offset)


def parse_http_date(http_date: str, base_time: Optional[float] = None) -> float:
    """
    Convert an HTTP date string to a Python timestamp (UNIX time).

    :param http_date: a string matching https://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.3.1

    :param base_time: the timestamp for converting a relative HTTP date into Python timestamp, if None, the current
                      time will be used.

    >>> parse_http_date('123', 0.4)
    123.4
    >>> t = 1541313273.0
    >>> parse_http_date('Sun, 04 Nov 2018 06:34:33 GMT') == t
    True
    >>> parse_http_date('Sun, 04 Nov 2018 06:34:33 PST') == t + 8 * 60 * 60
    True
    """
    if base_time is None:
        base_time = time.time()
    try:
        http_date = int(http_date)
    except ValueError:
        http_date = email.utils.parsedate_to_datetime(http_date)
        return http_date.timestamp()
    else:
        return base_time + float(http_date)
