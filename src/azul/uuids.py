from uuid import (
    UUID,
)

from azul import (
    reject,
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
