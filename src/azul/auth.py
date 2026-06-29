from abc import (
    ABCMeta,
    abstractmethod,
)
from typing import (
    Final,
)

import attr
from chalice.app import (
    BadRequestError,
)

from azul import (
    config,
)
from azul.lib.strings import (
    looks_like_access_token,
    looks_like_redactable_jwt,
    redact,
)


class Authentication(metaclass=ABCMeta):

    @abstractmethod
    def identity(self) -> str:
        """
        A string uniquely identifying the authenticated entity, for at least
        some period of time.
        """
        raise NotImplementedError

    @abstractmethod
    def as_http_header(self) -> str:
        """
        A string representing the authenticated entity as an HTTP header
        name/value pair. Raises NotImplementedError if the authentication format
        does not support such a representation.
        """
        raise NotImplementedError

    @abstractmethod
    def __str__(self) -> str:
        """
        A redacted string representation that's safe to use in logs. Clients
        shouldn't call this method directly but use ``str()`` instead. To obtain
        an unredacted string representation, clients should use ``repr()``.
        Concrete subclasses must implement this method.
        """
        raise NotImplementedError


@attr.s(auto_attribs=True, frozen=True)
class BearerTokenAuthentication(Authentication, metaclass=ABCMeta):
    token: str

    @classmethod
    def for_token(cls, token: str) -> BearerTokenAuthentication:
        if looks_like_redactable_jwt(token):
            return PersonalAccessTokenAuthentication(token)
        elif looks_like_access_token(token):
            return AccessTokenAuthentication(token)
        else:
            raise BadRequestError('Unexpected token syntax')

    def identity(self) -> str:
        return self.token

    def as_http_header(self) -> str:
        return f'Authorization: Bearer {self.token}'

    def __str__(self) -> str:
        return f'{type(self).__name__}(token={redact(self.token, fullmatch=True)!r})'


class AccessTokenAuthentication(BearerTokenAuthentication):
    pass


class PersonalAccessTokenAuthentication(BearerTokenAuthentication):
    pass


@attr.s(auto_attribs=True, frozen=True)
class HMACAuthentication(Authentication):
    key_id: str

    def identity(self) -> str:
        return self.key_id

    def as_http_header(self) -> str:
        raise NotImplementedError

    def __str__(self) -> str:
        # Nothing to redact
        return repr(self)


class _IndexerAuthentication(Authentication):

    def identity(self) -> str:
        return config.ServiceAccount.indexer.id(config)

    def as_http_header(self) -> str:
        raise NotImplementedError

    def __repr__(self) -> str:
        return f'{type(self).__name__}(identity={self.identity()!r})'

    def __str__(self) -> str:
        # Nothing to redact
        return repr(self)


indexer_authentication: Final = _IndexerAuthentication()

del _IndexerAuthentication
