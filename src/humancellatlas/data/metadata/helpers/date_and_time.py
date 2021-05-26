from datetime import (
    datetime,
    timezone,
)
from typing import (
    Optional,
)


def datetime_from_string(s: str) -> Optional[datetime]:
    """
    Convert a date string into a tz-aware (UTC) datetime. The timezone can be
    specified as an offset in '±HHMM' format, or using the timezone symbol 'Z'
    for '+0000'.

    >>> datetime_from_string('2021-05-05T21:24:26Z')
    datetime.datetime(2021, 5, 5, 21, 24, 26, tzinfo=datetime.timezone.utc)

    >>> datetime_from_string('2021-05-05T21:24:26.174274Z')
    datetime.datetime(2021, 5, 5, 21, 24, 26, 174274, tzinfo=datetime.timezone.utc)

    >>> datetime_from_string('2021-05-05T21:24:26.174274+0000')
    datetime.datetime(2021, 5, 5, 21, 24, 26, 174274, tzinfo=datetime.timezone.utc)

    >>> datetime_from_string('2021-05-05T14:24:26.174274-0700')
    datetime.datetime(2021, 5, 5, 21, 24, 26, 174274, tzinfo=datetime.timezone.utc)

    >>> datetime_from_string(None)
    >>> str(datetime_from_string(''))
    Traceback (most recent call last):
    ...
    ValueError: time data '' does not match format '%Y-%m-%dT%H:%M:%S.%f%z'

    >>> datetime_from_string('2021-05-05T14:24:26.174274-07:00')
    Traceback (most recent call last):
    ...
    ValueError: time data '2021-05-05T14:24:26.174274-07:00' does not match format '%Y-%m-%dT%H:%M:%S.%f%z'
    """
    if s is None:
        return None
    if s.endswith('Z'):
        s = s[:-1] + '+0000'
    # The date parsing is done with 'strptime()' since 'fromisoformat()' isn't
    # available until Python 3.7
    try:
        d = datetime.strptime(s, '%Y-%m-%dT%H:%M:%S.%f%z')
    except ValueError as e1:
        try:
            # Trying without '.%f' (microseconds)
            d = datetime.strptime(s, '%Y-%m-%dT%H:%M:%S%z')
        except ValueError as e2:
            raise e1 from e2
    return d.astimezone(tz=timezone.utc)
