from abc import (
    ABCMeta,
    abstractmethod,
)
from typing import (
    Final,
)

import attr

from azul import (
    config,
)


@attr.s(auto_attribs=True, frozen=True)
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


@attr.s(auto_attribs=True, frozen=True)
class AccessTokenAuthentication(Authentication):
    access_token: str

    def identity(self) -> str:
        return self.access_token

    def as_http_header(self) -> str:
        return f'Authorization: Bearer {self.access_token}'


class PersonalAccessTokenAuthentication(AccessTokenAuthentication):
    pass


@attr.s(auto_attribs=True, frozen=True)
class HMACAuthentication(Authentication):
    key_id: str

    def identity(self) -> str:
        return self.key_id

    def as_http_header(self) -> str:
        raise NotImplementedError


class _IndexerAuthentication(Authentication):

    def identity(self) -> str:
        return config.ServiceAccount.indexer.id(config)

    def as_http_header(self) -> str:
        raise NotImplementedError


indexer_authentication: Final = _IndexerAuthentication()

del _IndexerAuthentication
