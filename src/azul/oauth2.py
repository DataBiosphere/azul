from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Sequence,
    Union,
)

import attr
from google.auth.transport.urllib3 import (
    AuthorizedHttp,
)
from google.oauth2.credentials import (
    Credentials as TokenCredentials,
)
from google.oauth2.service_account import (
    Credentials as ServiceAccountCredentials,
)
import urllib3

from azul import (
    cached_property,
)
from azul.http import (
    http_client,
)

ScopedCredentials = Union[ServiceAccountCredentials, TokenCredentials]


class CredentialsProvider(ABC):

    @abstractmethod
    def scoped_credentials(self) -> ScopedCredentials:
        raise NotImplementedError

    @abstractmethod
    def oauth2_scopes(self) -> Sequence[str]:
        raise NotImplementedError


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class OAuth2Client:
    credentials_provider: CredentialsProvider

    @property
    def credentials(self) -> ScopedCredentials:
        return self.credentials_provider.scoped_credentials()

    @cached_property
    def _http_client(self) -> urllib3.PoolManager:
        """
        A urllib3 HTTP client with OAuth 2.0 credentials.
        """
        # By default, AuthorizedHTTP attempts to refresh the credentials on a 401
        # response, which is never helpful. When using service account
        # credentials, a fresh token is obtained for every lambda invocation,
        # which will never persist long enough for the token to expire. User
        # tokens can expire, but attempting to refresh them raises
        # `google.auth.exceptions.RefreshError` due to the credentials not being
        # configured with (among other fields) the client secret.
        return AuthorizedHttp(self.credentials, http_client(), refresh_status_codes=())
