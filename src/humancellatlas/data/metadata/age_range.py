from typing import Optional

from dataclasses import dataclass


@dataclass
class AgeRange:
    """
    >>> AgeRange('', 'second')
    AgeRange(min=None, max=None)

    >>> AgeRange(' 1 - 2 ', 'second')
    AgeRange(min=1, max=2)

    >>> AgeRange(' - ', 'second')
    AgeRange(min=0, max=315360000000)

    >>> AgeRange('1-', 'seconds')
    AgeRange(min=1, max=315360000000)

    >>> AgeRange('-2', 'seconds')
    AgeRange(min=0, max=2)
    """
    min: int
    max: int

    FACTORS = dict(year=365 * 24 * 3600,
                   month=365 * 24 * 3600 / 12,
                   week=7 * 24 * 3600,
                   day=24 * 3600,
                   hour=3600,
                   minute=60,
                   second=1)

    MAX_AGE = 10000 * FACTORS['year']

    @classmethod
    def parse(cls, age: str, unit: str) -> 'AgeRange':
        age = [s.strip() for s in age.split('-')]

        def cvt(value: str, default: Optional[int]) -> Optional[int]:
            if value:
                u = unit.lower().strip()
                try:
                    f = cls.FACTORS[u]
                except KeyError:
                    if u.endswith('s'):
                        try:
                            f = cls.FACTORS[u[:-1]]
                        except KeyError:
                            return None
                    else:
                        return None
                return f * int(value)
            else:
                return default

        if len(age) == 1:
            # FIXME
            # noinspection PyArgumentList
            return cls(min=cvt(age[0], None), max=cls.min)
        elif len(age) == 2:
            # FIXME
            # noinspection PyArgumentList
            return cls(min=cvt(age[0], 0), max=cvt(age[1], cls.MAX_AGE))
        else:
            # FIXME
            # noinspection PyArgumentList
            return cls(min=None, max=None)
