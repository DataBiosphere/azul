import abc
import attr


class Authentication(abc.ABC):

    @abc.abstractmethod
    def identity(self) -> str:
        """
        A string uniquely identifying the authenticated entity, for at least
        some period of time.
        """
        raise NotImplementedError


@attr.s(auto_attribs=True, frozen=True)
class OAuth2(Authentication):
    access_token: str

    def identity(self) -> str:
        return self.access_token


@attr.s(auto_attribs=True, frozen=True)
class HMACAuthentication(Authentication):
    key_id: str

    def identity(self) -> str:
        return self.key_id
