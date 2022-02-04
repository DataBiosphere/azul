from abc import (
    ABC,
    abstractmethod,
)
import json
import logging
from typing import (
    ContextManager,
    Sequence,
    Union,
)

import attr
from chalice import (
    UnauthorizedError,
)
from google.auth.transport.requests import (
    Request,
)
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
    RequirementError,
    cache,
    cached_property,
    config,
)
from azul.auth import (
    OAuth2,
)
from azul.deployment import (
    aws,
)
from azul.http import (
    http_client,
)
from azul.strings import (
    trunc_ellipses,
)
from azul.types import (
    MutableJSON,
)

log = logging.getLogger(__name__)

Credentials = Union[ServiceAccountCredentials, TokenCredentials]


class CredentialsProvider(ABC):

    @abstractmethod
    def scoped_credentials(self) -> Credentials:
        raise NotImplementedError

    @abstractmethod
    def oauth2_scopes(self) -> Sequence[str]:
        raise NotImplementedError

    @abstractmethod
    def insufficient_access(self, resource: str) -> Exception:
        raise NotImplementedError


class AbstractServiceAccountCredentialsProvider(CredentialsProvider):

    def oauth2_scopes(self) -> Sequence[str]:
        # Minimum scopes required for SAM registration
        return [
            'email',
            'openid'
        ]

    @cache
    def scoped_credentials(self) -> ServiceAccountCredentials:
        with self._credentials() as file_name:
            credentials = ServiceAccountCredentials.from_service_account_file(file_name)
        credentials = credentials.with_scopes(self.oauth2_scopes())
        credentials.refresh(Request())  # Obtain access token
        return credentials

    @abstractmethod
    def _credentials(self) -> ContextManager[str]:
        """
        Context manager that provides the file name for the temporary file
        containing the service account credentials.
        """
        raise NotImplementedError

    def insufficient_access(self, resource: str):
        return RequirementError(
            f'The service account (SA) {self.scoped_credentials().service_account_email!r} is not '
            f'authorized to access {resource} or that resource does not exist. Make sure '
            f'that it exists, that the SA is registered with SAM and has been granted read '
            f'access to the resource.'
        )


class ServiceAccountCredentialsProvider(AbstractServiceAccountCredentialsProvider):

    def oauth2_scopes(self) -> Sequence[str]:
        return [
            *super().oauth2_scopes(),
            'https://www.googleapis.com/auth/devstorage.read_only',
            'https://www.googleapis.com/auth/bigquery.readonly'
        ]

    def _credentials(self):
        return aws.service_account_credentials()


class PublicServiceAccountCredentialsProvider(AbstractServiceAccountCredentialsProvider):

    def _credentials(self):
        return aws.public_service_account_credentials()


class UserCredentialsProvider(CredentialsProvider):

    def __init__(self, token: OAuth2):
        self.token = token

    def oauth2_scopes(self) -> Sequence[str]:
        return ['email']

    @cache
    def scoped_credentials(self) -> TokenCredentials:
        # FIXME: this assumes the user has selected all required scopes.
        return TokenCredentials(self.token.identity(), scopes=self.oauth2_scopes())

    def identity(self) -> str:
        return self.token.identity()

    def insufficient_access(self, resource: str):
        scopes = ', '.join(self.oauth2_scopes())
        return UnauthorizedError(
            f'The current user is not authorized to access {resource} or that '
            f'resource does not exist. Make sure that it exists, that the user '
            f'is registered with Terra, that the provided access token is not '
            f'expired, and that the following access scopes were granted when '
            f'authenticating: {scopes}.'
        )


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class TerraClient:
    """
    A client to a service in the Broad Institute's Terra ecosystem.
    """
    credentials_provider: CredentialsProvider

    @property
    def credentials(self) -> Credentials:
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

    def _request(self,
                 method,
                 url,
                 *,
                 fields=None,
                 headers=None,
                 body=None
                 ) -> urllib3.HTTPResponse:
        log.debug('_request(%r, %r, fields=%r, headers=%r, body=%r)',
                  method, url, fields, headers, body)
        response = self._http_client.request(method,
                                             url,
                                             fields=fields,
                                             headers=headers,
                                             body=body)
        assert isinstance(response, urllib3.HTTPResponse)
        if log.isEnabledFor(logging.DEBUG):
            log.debug('_request(…) -> %r', trunc_ellipses(response.data, 256))
        header_name = 'WWW-Authenticate'
        try:
            header_value = response.headers[header_name]
        except KeyError:
            pass
        else:
            log.warning('_request(…) -> %r %r: %r',
                        response.status, header_name, header_value)
        return response

    @classmethod
    def with_service_account_credentials(cls) -> 'TerraClient':
        return cls(credentials_provider=ServiceAccountCredentialsProvider())

    @classmethod
    def with_public_service_account_credentials(cls) -> 'TerraClient':
        return cls(credentials_provider=PublicServiceAccountCredentialsProvider())

    @classmethod
    def with_user_credentials(cls, token: OAuth2) -> 'TerraClient':
        return cls(credentials_provider=UserCredentialsProvider(token))


class SAMClient(TerraClient):
    """
    A client to Broad's SAM (https://github.com/broadinstitute/sam). TDR uses
    SAM for authorization, and SAM uses Google OAuth 2.0 for authentication.
    """

    def register_with_sam(self) -> None:
        """
        Register the current service account with SAM.

        https://github.com/DataBiosphere/jade-data-repo/blob/develop/docs/register-sa-with-sam.md
        """
        email = self.credentials.service_account_email
        response = self._request('POST',
                                 f'{config.sam_service_url}/register/user/v1',
                                 body='')
        if response.status == 201:
            log.info('Google service account %r successfully registered with SAM.', email)
        elif response.status == 409:
            log.info('Google service account %r previously registered with SAM.', email)
        elif response.status == 500 and b'Cannot update googleSubjectId' in response.data:
            raise RuntimeError(
                'Unable to register service account. SAM does not allow re-registration of a '
                'new service account whose name matches that of another previously registered '
                'service account. Please refer to the troubleshooting section of the README.',
                email
            )
        else:
            raise RuntimeError('Unexpected response during SAM registration', response.data)

    def _insufficient_access(self, resource: str) -> Exception:
        return self.credentials_provider.insufficient_access(resource)

    def _check_response(self,
                        endpoint: str,
                        response: urllib3.HTTPResponse
                        ) -> MutableJSON:
        if response.status == 200:
            return json.loads(response.data)
        elif response.status == 401:
            raise self._insufficient_access(endpoint)
        else:
            raise RequirementError('Unexpected API response', endpoint, response.status)
