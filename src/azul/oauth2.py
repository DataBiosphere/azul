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
    NotRequired,
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
from azul.lib.types import (
    JSONTypedDict,
    is_of_type,
)

log = logging.getLogger(__name__)


class Authorization(JSONTypedDict):
    """
    The response from the authorization server's authorization endpoint
    (https://accounts.google.com/o/oauth2/v2/auth) in case the user has
    granted the authorization request. In a traditional authorization code
    flow, this information is encoded in the redirect URL as query
    parameters. With the Google Sign-In Javascript library, the callback is
    invoked with a JSON object of this shape, as documented in

    https://developers.google.com/identity/oauth2/web/reference/js-reference#CodeResponse
    """
    #: The authorization code, a temporary secret that indicates the user's
    #: consent, authorizing the application represented by the client ID to
    #: access resources owned by the user
    code: str
    #: Space-separated list of scopes granted by the user, which may be a subset
    #: of those requested by the application
    scope: str
    #: Optional, application-defined state that the authorization server passes
    #: through to from the request to the response
    state: NotRequired[str]


class TokenResponse(JSONTypedDict):
    access_token: str
    expires_in: int
    #: See `:attr:Authorization.scope`
    scope: str
    #: Should always be 'Bearer'
    token_type: str
    #: A JWT token identifying the user, only present if the authorization code
    #: was requested for the `openid` scope, and if the user granted that scope
    id_token: NotRequired[str]


class TokenForCodeResponse(TokenResponse):
    #: A long term secret that can be used to obtain more access tokens
    refresh_token: str
    #: Only present when the user grants time-based access
    refresh_token_expires_in: NotRequired[int]


class TokenInfoResponse(TypedDict):
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

    def token_for_code(self,
                       *,
                       authorization_code: str,
                       client_id: str,
                       client_secret: str,
                       redirect_uri: str | None = None
                       ) -> TokenForCodeResponse:
        """
        Obtain OAuth 2.0 tokens in exchange for an authorization code. This
        interaction is part of the authorization code flow.

        :param authorization_code: a temporary secret that indicates the user's
                                   consent, authorizing the application
                                   represented by the client ID to access
                                   resources owned by the user

        :param client_id: identifies the application authorized by the user

        :param client_secret: proof that the requestor is part of the application

        :param redirect_uri: the redirect URI that was used when the
                             ``authorization code`` was requested or None, if
                             the authorization code was requested without
                             specifying a redirect URI, e.g., when using the
                             Google Sign-In (GSI) library's authorizationCode
                             flow.
        """
        if redirect_uri is None:
            # Undocumented but crucial (https://stackoverflow.com/a/48121098/4171119)
            redirect_uri = 'postmessage'
        fields = {
            'grant_type': 'authorization_code',
            'code': authorization_code,
            'client_id': client_id,
            'client_secret': client_secret,
            'redirect_uri': redirect_uri
        }
        url = furl('https://oauth2.googleapis.com/token')
        response = self._http_client.request('POST', str(url), fields=fields)
        assert response.status == 200, R(
            'Unexpected status of response from authorization server', response.status)
        response = json.loads(response.data)
        assert is_of_type(response, TokenForCodeResponse)
        assert response['token_type'] == 'Bearer'
        return response

    def token_for_refresh(self,
                          *,
                          refresh_token: str,
                          client_id: str,
                          client_secret: str
                          ) -> TokenResponse:
        fields = {
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'client_id': client_id,
            'client_secret': client_secret,
        }
        url = furl('https://oauth2.googleapis.com/token')
        response = self._http_client.request('POST', str(url), fields=fields)
        assert response.status == 200, R(
            'Unexpected status of response from authorization server', response.status)
        response = json.loads(response.data)
        assert is_of_type(response, TokenResponse)
        assert response['token_type'] == 'Bearer'
        return response

    def token_info(self, access_token: str) -> TokenInfoResponse:
        url = furl(url='https://www.googleapis.com/oauth2/v3/tokeninfo',
                   args=dict(access_token=access_token))
        response = self._http_client.request('GET', str(url))
        assert response.status != 400, R('The token is not valid')
        assert response.status == 200, R('Unexpected response status', response.status)
        response = json.loads(response.data)
        assert is_of_type(response, TokenInfoResponse)
        return response


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
