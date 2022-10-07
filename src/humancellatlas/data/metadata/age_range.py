from typing import Optional

from dataclasses import dataclass


@dataclass(frozen=True)
class AgeRange:
    """
    >>> AgeRange.parse(' 1 - 2 ', 'second')
    AgeRange(min=1.0, max=2.0)

    >>> AgeRange.parse(' - ', 'second')
    AgeRange(min=0.0, max=inf)

    >>> AgeRange.parse('', 'years')
    AgeRange(min=0.0, max=inf)

    >>> r = AgeRange.parse('0-1', 'year'); r
    AgeRange(min=0.0, max=31536000.0)
    >>> 365 * 24 * 60 * 60 == r.max
    True

    >>> AgeRange.parse('1-', 'seconds')
    AgeRange(min=1.0, max=inf)

    >>> AgeRange.parse('-2', 'seconds')
    AgeRange(min=0.0, max=2.0)

    >>> AgeRange.parse('', 'blink')
    Traceback (most recent call last):
    ...
    ValueError: Cannot convert age '' with unit 'blink' to an AgeRange object

    >>> AgeRange.parse(' 1 - 2 ', 'blinks')
    Traceback (most recent call last):
    ...
    ValueError: Cannot convert age ' 1 - 2 ' with unit 'blinks' to an AgeRange object

    >>> AgeRange.parse('1-2-3', 'hours')
    Traceback (most recent call last):
    ...
    ValueError: Cannot convert age '1-2-3' with unit 'hours' to an AgeRange object

    >>> AgeRange.parse('one-2', 'days')
    Traceback (most recent call last):
    ...
    ValueError: Cannot convert age 'one-2' with unit 'days' to an AgeRange object

    >>> AgeRange(0, 1.0)
    Traceback (most recent call last):
    ...
    TypeError: ('Constructor arguments must be float values', 0)

    >>> AgeRange(0.0, 1)
    Traceback (most recent call last):
    ...
    TypeError: ('Constructor arguments must be float values', 1)
    """
    min: float
    max: float

    FACTORS = dict(year=365.0 * 24 * 3600,
                   month=365.0 * 24 * 3600 / 12,
                   week=7.0 * 24 * 3600,
                   day=24.0 * 3600,
                   hour=3600.0,
                   minute=60.0,
                   second=1.0)

    @classmethod
    def parse(cls, age: str, unit: str) -> 'AgeRange':
        def fail():
            return ValueError(f"Cannot convert age '{age}' with unit '{unit}' to an AgeRange object")

        age_ = [s.strip() for s in age.split('-')]
        unit_ = unit.lower().strip()

        try:
            factor = cls.FACTORS[unit_]
        except KeyError as e1:
            if unit_.endswith('s'):
                try:
                    factor = cls.FACTORS[unit_[:-1]]
                except KeyError as e2:
                    raise fail() from e2
            else:
                raise fail() from e1

        def cvt(value: str, default: float) -> Optional[float]:
            assert isinstance(default, float)
            try:
                return factor * float(value) if value else default
            except ValueError as e:
                raise fail() from e

        if len(age_) in (1, 2):
            self = cls(min=cvt(age_[0], 0.0), max=cvt(age_[-1], float('inf')))
            return self
        else:
            raise fail()

    def __post_init__(self):
        for v in self.min, self.max:
            if not isinstance(v, float):
                raise TypeError("Constructor arguments must be float values", v)

    any = None


AgeRange.any = AgeRange(min=0.0, max=float('inf'))
