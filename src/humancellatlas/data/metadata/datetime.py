from datetime import (
    datetime,
    timedelta,
    timezone,
)
import re
from typing import (
    Optional,
)


def parse_jsonschema_date_time(s: str) -> Optional[datetime]:
    """
    Convert a string in JSONSchema `date-time` format

    https://json-schema.org/understanding-json-schema/reference/string.html#dates-and-times

    to a timezone-aware `datetime` instance. Only up to 6 digits of fractional
    seconds are supported. This is a deviation from the standard which allows an
    arbitrary number of digits (impracticably so) but Python does not support
    more and silent truncation or rounding is not an good option. I never
    observed more than six digits in the wild, anyways.

    No fractional seconds, UTC

    >>> parse_jsonschema_date_time('2021-05-05T21:24:26Z')
    datetime.datetime(2021, 5, 5, 21, 24, 26, tzinfo=datetime.timezone.utc)

    Fractional seconds, UTC

    >>> parse_jsonschema_date_time('2021-05-05T21:24:26.174274Z')
    datetime.datetime(2021, 5, 5, 21, 24, 26, 174274, tzinfo=datetime.timezone.utc)

    Same with zero offset

    >>> parse_jsonschema_date_time('2021-05-05T21:24:26.174274+00:00')
    datetime.datetime(2021, 5, 5, 21, 24, 26, 174274, tzinfo=datetime.timezone.utc)

    Same with negative zero offset

    >>> parse_jsonschema_date_time('2021-05-05T21:24:26.174274-00:00')
    datetime.datetime(2021, 5, 5, 21, 24, 26, 174274, tzinfo=datetime.timezone.utc)

    Short fraction :

    >>> parse_jsonschema_date_time('2021-05-05T21:24:26.5Z')
    datetime.datetime(2021, 5, 5, 21, 24, 26, 500000, tzinfo=datetime.timezone.utc)

    Overlong fraction:

    >>> parse_jsonschema_date_time('2021-05-05T21:24:26.1234567Z')
    Traceback (most recent call last):
        ...
    ValueError: ('Not an RFC-3339 datetime', '2021-05-05T21:24:26.1234567Z')


    >>> s1 = '2021-05-05T21:24:26.174274+00:00'
    >>> s2 = '2021-05-05T14:24:26.174274-07:00'
    >>> d1 = parse_jsonschema_date_time(s1)
    >>> d2 = parse_jsonschema_date_time(s2)
    >>> d1
    datetime.datetime(2021, 5, 5, 21, 24, 26, 174274, tzinfo=datetime.timezone.utc)

    >>> d1 == d2
    True

    >>> d1.tzinfo == d2.tzinfo
    False

    >>> parse_jsonschema_date_time('')
    Traceback (most recent call last):
        ...
    ValueError: ('Not an RFC-3339 datetime', '')

    Missing colon in offset:

    >>> parse_jsonschema_date_time('2021-05-05T14:24:26.174274-0700')
    Traceback (most recent call last):
        ...
    ValueError: ('Not an RFC-3339 datetime', '2021-05-05T14:24:26.174274-0700')

    Out of range hour: (this is just an sample; we're relying in datetime to
    enforce ranges on all components)

    >>> parse_jsonschema_date_time('2021-05-05T24:24:26Z')
    Traceback (most recent call last):
        ...
    ValueError: hour must be in 0..23

    Out of range time offset:

    >>> parse_jsonschema_date_time('2021-05-05T21:24:26.174274-24:00') #doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    ValueError: offset must be a timedelta strictly between -timedelta(hours=24) and timedelta(hours=24), not ...

    2020 was a leap year

    >>> parse_jsonschema_date_time('2020-02-29T00:00:00.0Z')
    datetime.datetime(2020, 2, 29, 0, 0, tzinfo=datetime.timezone.utc)

    2021 was not

    >>> parse_jsonschema_date_time('2021-02-29T00:00:00.0Z')
    Traceback (most recent call last):
        ...
    ValueError: day is out of range for month
    """
    pattern = re.compile(r'''
        (?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})
        [Tt]
        (?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})
        (?:
            \.(?P<fractional_second>\d{1,6})
        )?
        (?:
            (?P<zulu>[Zz])
            |
            (?P<offset_sign>[+-])(?P<offset_hour>\d{2}):(?P<offset_minute>\d{2})
        )
    ''', flags=re.VERBOSE)
    m = pattern.fullmatch(s)
    if m:
        g = m.groupdict()
        year, month, day = int(g['year']), int(g['month']), int(g['day'])
        hour, minute, second = int(g['hour']), int(g['minute']), int(g['second'])
        fractional_second = g['fractional_second']
        if fractional_second is None:
            microsecond = 0
        else:
            microsecond = int(fractional_second.ljust(6, '0'))
        if g['zulu']:
            tzinfo = timezone.utc
        else:
            offset_hour, offset_minute = int(g['offset_hour']), int(g['offset_minute'])
            if offset_hour == 0 and offset_minute == 0:
                tzinfo = timezone.utc
            else:
                sign = g['offset_sign']
                if sign == '-':
                    sign = -1
                elif sign == '+':
                    sign = 1
                else:
                    assert False, sign
                delta = sign * timedelta(hours=offset_hour, minutes=offset_minute)
                tzinfo = timezone(offset=delta)
        return datetime(year, month, day, hour, minute, second, microsecond, tzinfo=tzinfo)
    else:
        raise ValueError('Not an RFC-3339 datetime', s)
