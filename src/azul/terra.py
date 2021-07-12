from abc import (
    ABC,
    abstractmethod,
)
import json
import logging
from time import (
    sleep,
)
from typing import (
    ContextManager,
    Dict,
    Sequence,
    Union,
)

import attr
from chalice import (
    UnauthorizedError,
)
from furl import (
    furl,
)
from google.api_core.exceptions import (
    Forbidden,
)
from google.auth.transport.requests import (
    Request,
)
from google.auth.transport.urllib3 import (
    AuthorizedHttp,
)
from google.cloud import (
    bigquery,
)
from google.cloud.bigquery import (
    QueryJob,
)
from google.cloud.bigquery.table import (
    TableListItem,
)
from google.oauth2.credentials import (
    Credentials as TokenCredentials,
)
from google.oauth2.service_account import (
    Credentials as ServiceAccountCredentials,
)
from more_itertools import (
    one,
)
import urllib3

from azul import (
    RequirementError,
    cache,
    cached_property,
    config,
    require,
)
from azul.auth import (
    OAuth2,
)
from azul.bigquery import (
    BigQueryRows,
)
from azul.deployment import (
    aws,
)
from azul.drs import (
    DRSClient,
)
from azul.http import (
    http_client,
)
from azul.indexer import (
    Prefix,
    SourceSpec,
)
from azul.strings import (
    trunc_ellipses,
)
from azul.types import (
    JSON,
    MutableJSON,
)

log = logging.getLogger(__name__)

Credentials = Union[ServiceAccountCredentials, TokenCredentials]


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class TDRSourceSpec(SourceSpec):
    project: str
    name: str
    is_snapshot: bool

    _type_dataset = 'dataset'

    _type_snapshot = 'snapshot'

    @classmethod
    def _parse(cls, spec: str, prefix: Prefix) -> 'TDRSourceSpec':
        """
        Construct an instance from its string representation, using the syntax
        'tdr:{project}:{type}/{name}:{prefix}' ending with an optional
        '/{partition_prefix_length}'.

        >>> s = TDRSourceSpec.parse('tdr:foo:snapshot/bar:')
        >>> s # doctest: +NORMALIZE_WHITESPACE
        TDRSourceSpec(prefix=Prefix(common='', partition=None),
              project='foo',
              name='bar',
              is_snapshot=True)
        >>> s.bq_name
        'bar'
        >>> str(s)
        'tdr:foo:snapshot/bar:'

        >>> d = TDRSourceSpec.parse('tdr:foo:dataset/bar:42/2')
        >>> d # doctest: +NORMALIZE_WHITESPACE
        TDRSourceSpec(prefix=Prefix(common='42', partition=2),
                      project='foo',
                      name='bar',
                      is_snapshot=False)
        >>> d.bq_name
        'datarepo_bar'
        >>> str(d)
        'tdr:foo:dataset/bar:42/2'

        >>> TDRSourceSpec.parse('tdr:foo:baz/bar:42')
        Traceback (most recent call last):
        ...
        AssertionError: baz

        >>> TDRSourceSpec.parse('tdr:foo:snapshot/bar:n32')
        Traceback (most recent call last):
        ...
        azul.uuids.InvalidUUIDPrefixError: 'n32' is not a valid UUID prefix.
        """
        # BigQuery (and by extension the TDR) does not allow : or / in dataset names
        service, project, name = spec.split(':')
        type, name = name.split('/')
        assert service == 'tdr', service
        if type == cls._type_snapshot:
            is_snapshot = True
        elif type == cls._type_dataset:
            is_snapshot = False
        else:
            assert False, type
        self = cls(prefix=prefix,
                   project=project,
                   name=name,
                   is_snapshot=is_snapshot)
        return self

    @property
    def bq_name(self):
        return self.name if self.is_snapshot else f'datarepo_{self.name}'

    def __str__(self) -> str:
        """
        >>> s = 'tdr:foo:snapshot/bar:'
        >>> s == str(TDRSourceSpec.parse(s))
        True

        >>> s = 'tdr:foo:snapshot/bar:22'
        >>> s == str(TDRSourceSpec.parse(s))
        True

        >>> s = 'tdr:foo:snapshot/bar:22/2'
        >>> s == str(TDRSourceSpec.parse(s))
        True
        """
        source_type = self._type_snapshot if self.is_snapshot else self._type_dataset
        return ':'.join([
            'tdr',
            self.project,
            f'{source_type}/{self.name}',
            str(self.prefix)
        ])

    @property
    def type_name(self):
        return self._type_snapshot if self.is_snapshot else self._type_dataset

    def qualify_table(self, table_name: str) -> str:
        return '.'.join((self.project, self.bq_name, table_name))


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
        return response


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


class TDRClient(SAMClient):
    """
    A client for the Broad Institute's Terra Data Repository aka "Jade".
    """

    @cache
    def lookup_source_project(self, source: TDRSourceSpec) -> str:
        """
        Return the name of the Google Cloud project containing the source
        (snapshot or dataset) with the specified name.
        """
        return self._lookup_source(source)['dataProject']

    @cache
    def lookup_source_id(self, source: TDRSourceSpec) -> str:
        """
        Return the primary identifier of the source (snapshot or dataset) with
        the specified name.
        """
        return self._lookup_source(source)['id']

    def check_api_access(self, source: TDRSourceSpec) -> None:
        """
        Verify that the client is authorized to read from the TDR service API.
        """
        self._lookup_source(source)
        log.info('TDR client is authorized for API access to %s.', source)

    def _lookup_source(self, source: TDRSourceSpec) -> JSON:
        resource = f'{source.type_name} {source.name!r} via the TDR API'
        tdr_path = source.type_name + 's'
        endpoint = self._repository_endpoint(tdr_path)
        params = dict(filter=source.bq_name, limit='2')
        response = self._request('GET', endpoint, fields=params)
        response = self._check_response(endpoint, response)
        try:
            # FIXME: Once filteredTotal is deployed in Terra prod, we can
            #        remove this try/except block.
            #        https://github.com/DataBiosphere/azul/issues/3195
            total = response['filteredTotal']
        except KeyError:
            total = response['total']
        if total == 0:
            raise self._insufficient_access(resource)
        elif total == 1:
            snapshot_id = one(response['items'])['id']
            endpoint = self._repository_endpoint(tdr_path, snapshot_id)
            response = self._request('GET', endpoint)
            require(response.status == 200,
                    f'Failed to access {resource} after resolving its ID to {snapshot_id!r}')
            return json.loads(response.data)
        else:
            raise RequirementError('Ambiguous response from TDR API', endpoint, response)

    def check_bigquery_access(self, source: TDRSourceSpec):
        """
        Verify that the client is authorized to read from TDR BigQuery tables.
        """
        resource = f'BigQuery dataset {source.bq_name!r} in Google Cloud project {source.project!r}'
        bigquery = self._bigquery(source.project)
        try:
            tables = list(bigquery.list_tables(source.bq_name, max_results=1))
            if tables:
                table: TableListItem = one(tables)
                self.run_sql(f'''
                    SELECT *
                    FROM `{table.project}.{table.dataset_id}.{table.table_id}`
                    LIMIT 1
                ''')
            else:
                raise RuntimeError(f'{resource} contains no tables')
        except Forbidden:
            raise self._insufficient_access(resource)
        else:
            log.info('TDR client is authorized to access tables in %s', resource)

    @cache
    def _bigquery(self, project: str) -> bigquery.Client:
        return bigquery.Client(project=project, credentials=self.credentials)

    def run_sql(self, query: str) -> BigQueryRows:
        delays = (10, 20, 40, 80)
        assert sum(delays) < config.contribution_lambda_timeout(retry=False)
        if log.isEnabledFor(logging.DEBUG):
            log.debug('Query: %r', self._trunc_query(query))
        for attempt, delay in enumerate((*delays, None)):
            job: QueryJob = self._bigquery(self.credentials.project_id).query(query)
            try:
                result = job.result()
            except Forbidden as e:
                if 'Exceeded rate limits' in e.message and delay is not None:
                    log.warning('Exceeded BigQuery rate limit during attempt %i/%i. '
                                'Retrying in %is.',
                                attempt + 1, len(delays) + 1, delay, exc_info=e)
                    sleep(delay)
                else:
                    raise e
            else:
                if log.isEnabledFor(logging.DEBUG):
                    log.debug('Job info: %s', json.dumps(self._job_info(job)))
                return result
        assert False

    def _trunc_query(self, query: str) -> str:
        return trunc_ellipses(query, 2048)

    def _job_info(self, job: QueryJob) -> JSON:
        # noinspection PyProtectedMember
        stats = job._properties['statistics']['query']
        if config.debug < 2:
            ignore = ('referencedTables', 'statementType', 'queryPlan')
            stats = {k: v for k, v in stats.items() if k not in ignore}
        return {
            'stats': stats,
            'query': self._trunc_query(job.query)
        }

    def _repository_endpoint(self, *path: str) -> str:
        return furl(config.tdr_service_url,
                    path=('api', 'repository', 'v1', *path)).url

    def _check_response(self,
                        endpoint: str,
                        response: urllib3.HTTPResponse
                        ) -> MutableJSON:
        if response.status == 200:
            return json.loads(response.data)
        elif response.status == 401:
            raise self._insufficient_access(endpoint)
        else:
            raise RequirementError('Unexpected response from TDR API', response.status)

    def snapshot_names_by_id(self) -> Dict[str, str]:
        """
        List the TDR snapshots accessible to the current credentials.
        """
        limit = 100
        endpoint = self._repository_endpoint('snapshots')
        response = self._request('GET', endpoint, fields=dict(limit=str(limit)))
        response = self._check_response(endpoint, response)
        # FIXME: Implement paging
        #        https://github.com/DataBiosphere/azul/issues/3093
        require(response['total'] <= limit, 'Too many snapshots', response['total'])
        return {
            snapshot['id']: snapshot['name']
            for snapshot in response['items']
        }

    @classmethod
    def with_service_account_credentials(cls) -> 'TDRClient':
        return cls(credentials_provider=ServiceAccountCredentialsProvider())

    @classmethod
    def with_public_service_account_credentials(cls) -> 'TDRClient':
        return cls(credentials_provider=PublicServiceAccountCredentialsProvider())

    @classmethod
    def with_user_credentials(cls, token: OAuth2) -> 'TDRClient':
        return cls(credentials_provider=UserCredentialsProvider(token))

    def drs_client(self):
        return DRSClient(http_client=self._http_client)
