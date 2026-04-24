from abc import (
    ABCMeta,
    abstractmethod,
)
from collections.abc import (
    Sequence,
)
import json
import logging
from typing import (
    TypedDict,
)

import attr
from furl import (
    furl,
)
from google.auth.transport.urllib3 import (
    AuthorizedHttp,
)
import google.oauth2.credentials
import google.oauth2.service_account

from azul import (
    config,
)
from azul.http import (
    HasCachedHttpClient,
    HttpClient,
    HttpClientDecorator,
)
from azul.lib import (
    R,
    cached_property,
)

log = logging.getLogger(__name__)


class TokenInfo(TypedDict):
    azp: str  # "713613812354-aelk662bncv14d319dk8juce9p11um00.apps.googleusercontent.com",
    aud: str  # "713613812354-aelk662bncv14d319dk8juce9p11um00.apps.googleusercontent.com",
    sub: str  # "105096702580025601450",
    scope: str  # "https://www.googleapis.com/auth/userinfo.email openid",
    exp: str  # "1689645319",
    expires_in: str  # "3511",
    email: str  # "hannes@ucsc.edu",
    email_verified: str  # "true",
    access_type: str  # "online"


class OAuth2Client(HasCachedHttpClient):
    """
    A client for Google's implementation of the OAuth 2.0 authorization server
    API.
    """

    def token_info(self, access_token: str) -> TokenInfo:
        url = furl(url='https://www.googleapis.com/oauth2/v3/tokeninfo',
                   args=dict(access_token=access_token))
        response = self._http_client.request('GET', str(url))
        assert response.status != 400, R('The token is not valid')
        assert response.status == 200, R('Unexpected response status', response.status)
        token_info: TokenInfo = json.loads(response.data)
        return token_info


TokenCredentials = google.oauth2.credentials.Credentials
ServiceAccountCredentials = google.oauth2.service_account.Credentials
ScopedCredentials = ServiceAccountCredentials | TokenCredentials


class CredentialsProvider(metaclass=ABCMeta):

    @abstractmethod
    def scoped_credentials(self) -> ScopedCredentials:
        raise NotImplementedError

    @abstractmethod
    def oauth2_scopes(self) -> Sequence[str]:
        raise NotImplementedError


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class CredentialedClient(HasCachedHttpClient):
    """
    Instances of this class have a cached, authenticating HTTP client.
    """

    credentials_provider: CredentialsProvider

    @cached_property
    def _oauth_client(self) -> OAuth2Client:
        return OAuth2Client()

    @property
    def credentials(self) -> ScopedCredentials:
        return self.credentials_provider.scoped_credentials()

    @property
    def service_account_credentials(self) -> ServiceAccountCredentials:
        credentials = self.credentials
        assert isinstance(credentials, ServiceAccountCredentials), R(
            'Expecting service account credentials', type(credentials)
        )
        return credentials

    # The AuthorizedHttp class declares the second constructor argument to be a
    # PoolManager instance but, except for __del__, doesn't actually use methods
    # from the latter, only those from RequestMethods, at least in the scenarios
    # we use AuthorizedHttp in. The AuthorizedHttp.__del__ method calls `clear`
    # on the wrapped instance, so this adapter only provides that.
    #
    class _PoolManagerAdapter(HttpClientDecorator):

        def clear(self):
            pass

    def _create_http_client(self) -> HttpClient:
        """
        A urllib3 HTTP client with OAuth 2.0 credentials
        """
        # By default, AuthorizedHTTP attempts to refresh the credentials on a
        # 401 response, which is never helpful. When using service account
        # credentials, a fresh token is obtained for every lambda invocation,
        # which will never persist long enough for the token to expire. User
        # tokens can expire, but attempting to refresh them raises
        # `google.auth.exceptions.RefreshError` due to the credentials not being
        # configured with (among other fields) the client secret.
        #
        return AuthorizedHttp(self.credentials,
                              self._PoolManagerAdapter(super()._create_http_client()),
                              refresh_status_codes=())

    def validate(self):
        """
        Validate the credentials from the provider this client was initialized
        with. Raises an exception if the credentials are invalid, or if their
        validity cannot be determined.

        For a user's access token to be valid, it must not be expired, and
        originate from a Google OAuth 2.0 client belonging to the current
        Google Cloud project.

        For service account credentials (those with a private key) to be valid,
        the associated access token must not be expired and the email associated
        with the token must be that of the service account itself.

        For a service account's access token (a bare access token created from
        the service account's private key by some other party) to be valid, the
        token must not be expired and the service account must belong to the
        current Google Cloud project.

        :raise AssertionError: if the token is definitely invalid

        :raise Exception: if the validity of the token cannot be determined
        """
        credentials = self.credentials
        token_info = self._oauth_client.token_info(str(credentials.token))
        # The error messages here intentionally lack detail, for confidentiality
        if isinstance(credentials, ServiceAccountCredentials):
            # Actual service account credentials
            assert token_info['email_verified'] == 'true', R(
                'Service account email is not verified')
            assert token_info['email'] == credentials.service_account_email, R(
                'Service account email does not match')
        elif isinstance(credentials, TokenCredentials):
            authorized_party = token_info['azp']
            email = token_info.get('email')
            if authorized_party.endswith('.apps.googleusercontent.com'):
                # A user's access token originating from an OAuth 2.0 client
                azul_client_id = config.google_oauth2_client_id
                assert azul_client_id is not None, R(
                    'Acceptance of OAuth 2.0 user access tokens is disabled')
                project_id = self._project_id_from_client_id(azul_client_id)
                authorized_project_id = self._project_id_from_client_id(authorized_party)
                assert project_id == authorized_project_id, R(
                    'OAuth 2.0 client project does not match')
            elif email is not None and email.endswith('.iam.gserviceaccount.com'):
                # A service account's bare access token
                assert token_info['email_verified'] == 'true', R(
                    'Service account email is not verified')
                local_part, _, host = email.partition('@')
                host, _, domain = host.partition('.')
                assert host == config.google_project(), R(
                    'Service account project does not match')
            else:
                assert False, 'Unexpected type of authorized party'
        else:
            assert False, type(credentials)

    def _project_id_from_client_id(self, client_id):
        return client_id.split('-', 1)[0]
