import uuid


class InvalidUUIDError(Exception):

    def __init__(self, entity_id: str):
        super().__init__(f'{entity_id} is not a valid UUID.')


class InvalidUUIDVersionError(InvalidUUIDError):

    def __init__(self, uuid: uuid.UUID):
        Exception.__init__(self, f'{str(uuid)} is not a valid RFC-4122 UUID (undefined version {uuid.version}).')


class InvalidUUIDPrefixError(Exception):

    def __init__(self, prefix: str):
        super().__init__(f'{prefix} is not a valid standard UUID prefix.')


def validate_uuid(uuid_str: str) -> uuid.UUID:
    """
    >>> validate_uuid('foo')
    Traceback (most recent call last):
    ...
    azul.uuids.InvalidUUIDError: foo is not a valid UUID.

    >>> validate_uuid('8f53d355-b2fa-4bab-a2f2-6852d852d2ec')
    UUID('8f53d355-b2fa-4bab-a2f2-6852d852d2ec')

    >>> validate_uuid('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa')
    Traceback (most recent call last):
    ...
    azul.uuids.InvalidUUIDVersionError: aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa is not a valid RFC-4122 UUID (undefined ve\
rsion 10).
    """
    try:
        formatted_uuid = uuid.UUID(uuid_str)
    except ValueError:
        raise InvalidUUIDError(uuid_str)
    else:
        if str(formatted_uuid) != uuid_str:
            raise InvalidUUIDError(uuid_str)
        if formatted_uuid.version not in (1, 3, 4, 5):
            raise InvalidUUIDVersionError(formatted_uuid)
    return formatted_uuid


def validate_uuid_prefix(uuid_prefix: str) -> str:
    """
    >>> validate_uuid_prefix('foo')
    Traceback (most recent call last):
    ...
    azul.uuids.InvalidUUIDPrefixError: foo is not a valid standard UUID prefix.

    >>> validate_uuid_prefix('8f53')
    '8f53'

    # The empty string is a valid prefix
    >>> validate_uuid_prefix('')
    ''

    # A complete UUID is a valid prefix
    >>> validate_uuid_prefix('8f53d355-b2fa-4bab-a2f2-6852d852d2ec')
    '8f53d355-b2fa-4bab-a2f2-6852d852d2ec'
    """
    valid_uuid_str = str(uuid.uuid4())
    try:
        validate_uuid(uuid_prefix + valid_uuid_str[len(uuid_prefix):])
    except InvalidUUIDError:
        raise InvalidUUIDPrefixError(uuid_prefix)
    else:
        return uuid_prefix
