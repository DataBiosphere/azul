import math
from typing import (
    ClassVar,
    Generic,
    TypeVar,
)
from uuid import (
    UUID,
)

import attr

from azul import (
    JSON,
    reject,
    require,
)


class InvalidUUIDError(Exception):

    def __init__(self, uuid: str, *args):
        super().__init__(f'{uuid!r} is not a valid UUID.', *args)


class InvalidUUIDVersionError(InvalidUUIDError):

    def __init__(self, uuid: UUID):
        super().__init__(str(uuid), f'Not a valid RFC-4122 UUID (undefined version {uuid.version}).')


class InvalidUUIDPrefixError(Exception):

    def __init__(self, prefix: str):
        super().__init__(f'{prefix!r} is not a valid UUID prefix.')


def validate_uuid(uuid_str: str) -> None:
    """
    >>> validate_uuid('8f53d355-b2fa-4bab-a2f2-6852d852d2ec')

    >>> validate_uuid('foo')
    Traceback (most recent call last):
    ...
    azul.uuids.InvalidUUIDError: 'foo' is not a valid UUID.

    >>> validate_uuid('8F53d355-b2fa-4bab-a2f2-6852d852d2ec')
    Traceback (most recent call last):
    ...
    azul.uuids.InvalidUUIDError: '8F53d355-b2fa-4bab-a2f2-6852d852d2ec' is not a valid UUID.

    >>> validate_uuid('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa') # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    azul.uuids.InvalidUUIDVersionError: ("'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa' is not a valid UUID.",
    'Not a valid RFC-4122 UUID (undefined version 10).')
    """
    try:
        formatted_uuid = UUID(uuid_str)
    except ValueError:
        raise InvalidUUIDError(uuid_str)
    else:
        if str(formatted_uuid) != uuid_str:
            raise InvalidUUIDError(uuid_str)
        if formatted_uuid.version not in (1, 3, 4, 5):
            raise InvalidUUIDVersionError(formatted_uuid)


def validate_uuid_prefix(uuid_prefix: str) -> None:
    """
    # The empty string is a valid prefix
    >>> validate_uuid_prefix('')

    >>> validate_uuid_prefix('8f53')

    # A complete UUID is a valid prefix
    >>> validate_uuid_prefix('8f53d355-b2fa-4bab-a2f2-6852d852d2ec')

    >>> validate_uuid_prefix('8F53')
    Traceback (most recent call last):
    ...
    azul.uuids.InvalidUUIDPrefixError: '8F53' is not a valid UUID prefix.

    >>> validate_uuid_prefix('8')

    >>> validate_uuid_prefix('8f538f53')

    >>> validate_uuid_prefix('8f538f5-')
    Traceback (most recent call last):
    ...
    azul.RequirementError: UUID prefix ends with an invalid character: 8f538f5-

    >>> validate_uuid_prefix('8f538f-')
    Traceback (most recent call last):
    ...
    azul.RequirementError: UUID prefix ends with an invalid character: 8f538f-

    >>> validate_uuid_prefix('8f538f53a')
    Traceback (most recent call last):
    ...
    azul.uuids.InvalidUUIDPrefixError: '8f538f53a' is not a valid UUID prefix.
    """
    valid_uuid_str = '26a8fccd-bbd2-4342-9c19-6ed7c9bb9278'
    reject(uuid_prefix.endswith('-'),
           f'UUID prefix ends with an invalid character: {uuid_prefix}')
    try:
        validate_uuid(uuid_prefix + valid_uuid_str[len(uuid_prefix):])
    except InvalidUUIDError:
        raise InvalidUUIDPrefixError(uuid_prefix)


UUID_PARTITION = TypeVar('UUID_PARTITION', bound='UUIDPartition')


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class UUIDPartition(Generic[UUID_PARTITION]):
    """
    A binary partitioning of the UUID space. Most partitionings of the UUID
    space use a prefix of the hexadecimal representation of UUIDs. This class
    uses the binary representation and is therefore more granular.
    """
    prefix_length: int
    prefix: int

    root: ClassVar[UUID_PARTITION]  # see meta-class above

    # This stub is only needed to aid PyCharm's type inference. Without this,
    # a constructor invocation that doesn't refer to the class explicitly, but
    # through a variable will cause a warning. I suspect a bug in PyCharm:
    #
    # https://youtrack.jetbrains.com/issue/PY-44728
    #
    # noinspection PyDataclass
    def __init__(self, *, prefix_length: int, prefix: int) -> None: ...

    def __attrs_post_init__(self):
        reject(self.prefix_length == 0 and self.prefix != 0)
        require(0 <= self.prefix < 2 ** self.prefix_length)

    @classmethod
    def from_json(cls, partition: JSON) -> UUID_PARTITION:
        return cls(**partition)

    def to_json(self) -> JSON:
        return attr.asdict(self)

    def contains(self, member: UUID):
        """
        >>> p = UUIDPartition(prefix_length=7, prefix=0b0111_1111)
        >>> p.contains(UUID('fdd4524e-14c4-41d7-9071-6cadab09d75c'))
        False
        >>> p.contains(UUID('fed4524e-14c4-41d7-9071-6cadab09d75c'))
        True
        >>> p.contains(UUID('ffd4524e-14c4-41d7-9071-6cadab09d75c'))
        True
        """
        # UUIDs are 128 bit integers
        shift = 128 - self.prefix_length
        return member.int >> shift == self.prefix

    def divide(self, num_divisions: int) -> list[UUID_PARTITION]:
        """
        Divide this partition into a set of at least the given number of
        sub-partitions. The length of the return value will always be the
        smallest a power of two that is greater than ``num_divisions`.

        >>> sorted(UUIDPartition.root.divide(3)) # doctest: +NORMALIZE_WHITESPACE
        [UUIDPartition(prefix_length=2, prefix=0),\
        UUIDPartition(prefix_length=2, prefix=1),\
        UUIDPartition(prefix_length=2, prefix=2),\
        UUIDPartition(prefix_length=2, prefix=3)]
        """
        prefix_length = math.ceil(math.log2(num_divisions))
        num_divisions = 2 ** prefix_length
        cls = type(self)
        return [
            cls(prefix_length=self.prefix_length + prefix_length,
                prefix=(self.prefix << prefix_length) + prefix)
            for prefix in range(num_divisions)
        ]

    def __str__(self) -> str:
        """
        Represent this partition as a hexadecimal range. This range can be used
        to visually tell wether this partition contains a particular UUID: it
        does, if the UUID starts with any hexadecimal sequence in the range
        returned by this function.

        >>> str(UUIDPartition.root)
        '-'

                                                      0b1111_1110 == 0xfe
                                                      0b1111_1111 == 0xff
        >>> str(UUIDPartition(prefix_length=7, prefix=0b1111_111))
        'fe-ff'

        Leading zeroes in the high and low end of the range:

                                                      0b0000_1110 == 0x0e
                                                      0b0000_1111 == 0x0f
        >>> str(UUIDPartition(prefix_length=7, prefix=0b0000_111))
        '0e-0f'

        A partition twice as big (a binary prefix that's one bit shorter):

                                                      0b0000_1100 = 0x0c
                                                      0b0000_1101 = 0x0d
                                                      0b0000_1110 = 0x0e
                                                      0b0000_1111 = 0x0f
        >>> str(UUIDPartition(prefix_length=6, prefix=0b0000_11))
        '0c-0f'
        """
        shift = 4 - self.prefix_length % 4  # shift to align at nibble boundary
        all_ones = (1 << shift) - 1
        lo = self.prefix << shift
        hi = lo + all_ones

        hex_len = (self.prefix_length + 3) // 4

        def hex(i):
            return format(i, f'0{hex_len}x')[:hex_len]

        return '-'.join(map(hex, (lo, hi)))

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        cls.init_cls()

    @classmethod
    def init_cls(cls):
        cls.root = cls(prefix=0, prefix_length=0)


UUIDPartition.init_cls()
