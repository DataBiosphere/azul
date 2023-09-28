from abc import (
    ABCMeta,
    abstractmethod,
)
from datetime import (
    datetime,
)
import email.utils
import re
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
    A monotonically decreasing, non-negative estimate of time remaining in a
    particular context
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
    The remaining wall clock time up to a given absolute deadline in terms of
    time.time()
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
        require(amount >= 0, 'Initial remaining time must be non-negative')
        super().__init__(time.time() + amount)


class AdjustedRemainingTime(RemainingTime):
    """
    Some other estimate of remaining time, adjusted by a fixed offset. Use a
    negative offset to reduce the remaining time or a positive offset to
    increase it.
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

    :param base_time: the timestamp for converting a relative HTTP date into
                      Python timestamp, if None, the current time will be used.

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


dcp2_datetime_format = '%Y-%m-%dT%H:%M:%S.%f%z'


def format_dcp2_datetime(d: datetime) -> str:
    """
    Convert a tz-aware (UTC) datetime into a '2020-01-01T00:00:00.000000Z'
    formatted string.

    >>> from datetime import timezone
    >>> format_dcp2_datetime(datetime(2020, 12, 31, 23, 59, 59, 1, tzinfo=timezone.utc))
    '2020-12-31T23:59:59.000001Z'

    >>> format_dcp2_datetime(datetime(9999, 1, 1, tzinfo=timezone.utc))
    '9999-01-01T00:00:00.000000Z'

    >>> format_dcp2_datetime(datetime(1, 1, 1, tzinfo=timezone.utc))
    '0001-01-01T00:00:00.000000Z'

    >>> format_dcp2_datetime(datetime(2020, 1, 1))
    Traceback (most recent call last):
    ...
    azul.RequirementError: 2020-01-01 00:00:00
    """
    require(str(d.tzinfo) == 'UTC', d)
    date_string = datetime.strftime(d, dcp2_datetime_format)
    # Work around https://bugs.python.org/issue13305
    date_string = ('0000' + date_string)[-31:]
    assert date_string.endswith('+0000'), date_string
    return date_string[:-5] + 'Z'


def parse_dcp2_datetime(s: str) -> datetime:
    """
    Convert a '2020-01-01T00:00:00.000000Z' formatted string into a tz-aware
    (UTC) datetime.

    >>> parse_dcp2_datetime('2020-01-01T00:00:00.000000Z')
    datetime.datetime(2020, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)

    >>> parse_dcp2_datetime('0001-01-01T00:00:00.000000Z')
    datetime.datetime(1, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)

    >>> parse_dcp2_datetime('2020-01-01T00:00:00.000000')
    Traceback (most recent call last):
    ...
    ValueError: time data '2020-01-01T00:00:00.000000' does not match format '%Y-%m-%dT%H:%M:%S.%f%z'
    """
    return datetime.strptime(s, dcp2_datetime_format)


def parse_dcp2_version(s: str) -> datetime:
    """
    Convert a dcp2 `version` string into a tz-aware (UTC) datetime.

    https://github.com/HumanCellAtlas/dcp2/blob/main/docs/dcp2_system_design.rst#312object-naming

    >>> parse_dcp2_version('2020-01-01T00:00:00.123456Z')
    datetime.datetime(2020, 1, 1, 0, 0, 0, 123456, tzinfo=datetime.timezone.utc)

    >>> parse_dcp2_version('2020-01-01t00:00:00.123456Z')
    Traceback (most recent call last):
    ...
    ValueError: Invalid version value '2020-01-01t00:00:00.123456Z'

    >>> parse_dcp2_version('2020-1-01T00:00:00.123456Z')
    Traceback (most recent call last):
    ...
    ValueError: Invalid version value '2020-1-01T00:00:00.123456Z'

    >>> parse_dcp2_version('2020-01-01T00:00:00.12345Z')
    Traceback (most recent call last):
    ...
    ValueError: Invalid version value '2020-01-01T00:00:00.12345Z'
    """
    pattern = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z'
    if re.fullmatch(pattern, s):
        return parse_dcp2_datetime(s)
    else:
        raise ValueError(f'Invalid version value {s!r}')
