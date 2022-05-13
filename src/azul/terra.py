from abc import (
    ABC,
    abstractmethod,
)
from collections.abc import (
    Sequence,
)
import json
import logging
from time import (
    sleep,
)
from typing import (
    ClassVar,
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
    InternalServerError,
    ServiceUnavailable,
)
from google.auth.transport.requests import (
    Request,
)
from google.cloud import (
    bigquery,
)
from google.cloud.bigquery import (
    QueryJob,
    QueryJobConfig,
    QueryPriority,
)
from more_itertools import (
    one,
)
import urllib3
from urllib3.exceptions import (
    TimeoutError,
)
from urllib3.response import (
    HTTPResponse,
)

from azul import (
    RequirementError,
    cache,
    config,
    mutable_furl,
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
from azul.indexer import (
    SourceSpec,
)
from azul.oauth2 import (
    CredentialsProvider,
    OAuth2Client,
    ServiceAccountCredentials,
    TokenCredentials,
)
from azul.strings import (
    trunc_ellipses,
)
from azul.types import (
    JSON,
    MutableJSON,
)

log = logging.getLogger(__name__)


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class TDRSourceSpec(SourceSpec):
    project: str
    name: str
    is_snapshot: bool

    _type_dataset = 'dataset'

    _type_snapshot = 'snapshot'

    @classmethod
    def parse(cls, spec: str) -> 'TDRSourceSpec':
        """
        Construct an instance from its string representation, using the syntax
        'tdr:{project}:{type}/{name}:{prefix}' ending with an optional
        '/{partition_prefix_length}'.

        >>> s = TDRSourceSpec.parse('tdr:foo:snapshot/bar:/0')
        >>> s # doctest: +NORMALIZE_WHITESPACE
        TDRSourceSpec(prefix=Prefix(common='', partition=0),
                      project='foo',
                      name='bar',
                      is_snapshot=True)
        >>> s.bq_name
        'bar'
        >>> str(s)
        'tdr:foo:snapshot/bar:/0'

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

        >>> TDRSourceSpec.parse('tdr:foo:baz/bar:42/0')
        Traceback (most recent call last):
        ...
        AssertionError: baz

        >>> TDRSourceSpec.parse('tdr:foo:snapshot/bar:n32/0')
        Traceback (most recent call last):
        ...
        azul.uuids.InvalidUUIDPrefixError: 'n32' is not a valid UUID prefix.
        """
        rest, prefix = cls._parse(spec)
        # BigQuery (and by extension the TDR) does not allow : or / in dataset names
        service, project, name = rest.split(':')
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
        assert spec == str(self), spec
        return self

    @property
    def bq_name(self):
        return self.name if self.is_snapshot else f'datarepo_{self.name}'

    def __str__(self) -> str:
        """
        The inverse of :meth:`parse`.

        >>> s = 'tdr:foo:snapshot/bar:/0'
        >>> s == str(TDRSourceSpec.parse(s))
        True

        >>> s = 'tdr:foo:snapshot/bar:22/0'
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

    def contains(self, other: 'SourceSpec') -> bool:
        """
        >>> p = TDRSourceSpec.parse

        >>> p('tdr:foo:snapshot/bar:/0').contains(p('tdr:foo:snapshot/bar:/0'))
        True

        >>> p('tdr:foo:snapshot/bar:/0').contains(p('tdr:bar:snapshot/bar:/0'))
        False

        >>> p('tdr:foo:snapshot/bar:/0').contains(p('tdr:foo:dataset/bar:/0'))
        False

        >>> p('tdr:foo:snapshot/bar:/0').contains(p('tdr:foo:snapshot/baz:/0'))
        False
        """
        return (
            isinstance(other, TDRSourceSpec)
            and super().contains(other)
            and self.is_snapshot == other.is_snapshot
            and self.project == other.project
            and self.name == other.name
        )


class TerraCredentialsProvider(CredentialsProvider, ABC):

    @abstractmethod
    def insufficient_access(self, resource: str) -> Exception:
        raise NotImplementedError


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class ServiceAccountCredentialsProvider(TerraCredentialsProvider):
    service_account: config.ServiceAccount

    def oauth2_scopes(self) -> Sequence[str]:
        # Minimum scopes required for SAM registration
        return [
            'https://www.googleapis.com/auth/userinfo.email',
            'openid'
        ]

    @cache
    def scoped_credentials(self) -> ServiceAccountCredentials:
        with aws.service_account_credentials(self.service_account) as file_name:
            credentials = ServiceAccountCredentials.from_service_account_file(file_name)
        credentials = credentials.with_scopes(self.oauth2_scopes())
        credentials.refresh(Request())  # Obtain access token
        return credentials

    def insufficient_access(self, resource: str):
        return RequirementError(
            f'The service account (SA) {self.scoped_credentials().service_account_email!r} is not '
            f'authorized to access {resource} or that resource does not exist. Make sure '
            f'that it exists, that the SA is registered with SAM and has been granted read '
            f'access to the resource.'
        )


class IndexerServiceAccountCredentialsProvider(ServiceAccountCredentialsProvider):

    def oauth2_scopes(self) -> Sequence[str]:
        return [
            *super().oauth2_scopes(),
            'https://www.googleapis.com/auth/devstorage.read_only',
            'https://www.googleapis.com/auth/bigquery.readonly'
        ]


class UserCredentialsProvider(TerraCredentialsProvider):

    def __init__(self, token: OAuth2):
        self.token = token

    def oauth2_scopes(self) -> Sequence[str]:
        return ['https://www.googleapis.com/auth/userinfo.email']

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


class TerraClientException(Exception):
    pass


class TerraTimeoutException(TerraClientException):

    def __init__(self, url: furl, timeout: float):
        super().__init__(f'No response from {url} within {timeout} seconds')


class TerraStatusException(TerraClientException):

    def __init__(self, url: furl, response: HTTPResponse):
        super().__init__(f'Unexpected response from {url}',
                         response.status, response.data)


class TerraNameConflictException(TerraClientException):

    def __int__(self, url: furl, source_name: str, response_json: JSON):
        super().__init__(f'More than one source named {source_name!r}',
                         str(url), response_json)


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class TerraClient(OAuth2Client):
    """
    A client to a service in the Broad Institute's Terra ecosystem.
    """
    credentials_provider: TerraCredentialsProvider

    def _request(self,
                 method: str,
                 url: furl,
                 *,
                 headers=None,
                 body=None
                 ) -> urllib3.HTTPResponse:
        timeout = config.terra_client_timeout
        log.debug('_request(%r, %s, headers=%r, timeout=%r, body=%r)',
                  method, url, headers, timeout, body)
        try:
            response = self._http_client.request(method,
                                                 str(url),
                                                 headers=headers,
                                                 # FIXME: Service should return 503 response when Terra client times out
                                                 #        https://github.com/DataBiosphere/azul/issues/3968
                                                 timeout=timeout,
                                                 retries=False,
                                                 body=body)
        except TimeoutError:
            raise TerraTimeoutException(url, timeout)

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
        url = config.sam_service_url.set(path='/register/user/v1')
        response = self._request('POST', url, body='')
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
            raise TerraStatusException(url, response)

    def is_registered(self) -> bool:
        """
        Check whether the user or service account associated with the current
        client's credentials is registered with SAM.
        """
        endpoint = config.sam_service_url.set(path='/register/users/v1')
        response = self._request('GET', endpoint)
        if response.status == 200:
            return True
        elif response.status == 404:
            return False
        else:
            raise TerraStatusException(endpoint, response)

    def _insufficient_access(self, resource: str) -> Exception:
        return self.credentials_provider.insufficient_access(resource)


class TDRClient(SAMClient):
    """
    A client for the Broad Institute's Terra Data Repository aka "Jade".
    """

    @attr.s(frozen=True, kw_only=True, auto_attribs=True)
    class TDRSource:
        project: str
        id: str
        location: str

    @cache
    def lookup_source(self, source_spec: TDRSourceSpec) -> TDRSource:
        source = self._lookup_source(source_spec)
        storage = one(
            storage
            for dataset in (s['dataset'] for s in source['source'])
            for storage in dataset['storage']
            if storage['cloudResource'] == 'bigquery'
        )
        return self.TDRSource(project=source['dataProject'],
                              id=source['id'],
                              location=storage['region'])

    def check_api_access(self, source: TDRSourceSpec) -> None:
        """
        Verify that the client is authorized to read from the TDR service API.
        """
        self._lookup_source(source)
        log.info('TDR client is authorized for API access to %s.', source)

    def _lookup_source(self, source: TDRSourceSpec) -> JSON:
        tdr_path = source.type_name + 's'
        endpoint = self._repository_endpoint(tdr_path)
        endpoint.set(args=dict(filter=source.bq_name, limit='2'))
        response = self._request('GET', endpoint)
        response = self._check_response(endpoint, response)
        total = response['filteredTotal']
        if total == 0:
            raise self._insufficient_access(str(endpoint))
        elif total == 1:
            snapshot_id = one(response['items'])['id']
            endpoint = self._repository_endpoint(tdr_path, snapshot_id)
            response = self._request('GET', endpoint)
            require(response.status == 200,
                    endpoint,
                    response,
                    exception=TerraStatusException)
            return json.loads(response.data)
        else:
            raise TerraNameConflictException(endpoint, source.bq_name, response)

    def check_bigquery_access(self, source: TDRSourceSpec):
        """
        Verify that the client is authorized to read from TDR BigQuery tables.
        """
        resource = f'BigQuery dataset {source.bq_name!r} in Google Cloud project {source.project!r}'
        try:
            self.run_sql(f'''
                SELECT links_id
                FROM `{source.project}.{source.bq_name}.links`
                LIMIT 1
            ''')
        except Forbidden:
            raise self._insufficient_access(resource)
        else:
            log.info('TDR client is authorized to access tables in %s', resource)

    @cache
    def _bigquery(self, project: str) -> bigquery.Client:
        return bigquery.Client(project=project, credentials=self.credentials)

    def run_sql(self, query: str) -> BigQueryRows:
        bigquery = self._bigquery(self.credentials.project_id)
        if log.isEnabledFor(logging.DEBUG):
            log.debug('Query (%r characters total): %r',
                      len(query), self._trunc_query(query))
        if config.bigquery_batch_mode:
            job_config = QueryJobConfig(priority=QueryPriority.BATCH)
            job: QueryJob = bigquery.query(query, job_config=job_config)
            result = job.result()
        else:
            delays = (10, 20, 40, 80)
            assert sum(delays) < config.contribution_lambda_timeout(retry=False)
            for attempt, delay in enumerate((*delays, None)):
                job: QueryJob = bigquery.query(query)
                try:
                    result = job.result()
                except (Forbidden, InternalServerError, ServiceUnavailable) as e:
                    if delay is None:
                        raise e
                    elif isinstance(e, Forbidden) and 'Exceeded rate limits' not in e.message:
                        raise e
                    else:
                        log.warning('BigQuery job error during attempt %i/%i. Retrying in %is.',
                                    attempt + 1, len(delays) + 1, delay, exc_info=e)
                        sleep(delay)
                else:
                    break
            else:
                assert False
        if log.isEnabledFor(logging.DEBUG):
            log.debug('Job info: %s', json.dumps(self._job_info(job)))
        return result

    def _trunc_query(self, query: str) -> str:
        return trunc_ellipses(query, 2048)

    def _job_info(self, job: QueryJob) -> JSON:
        # noinspection PyProtectedMember
        stats = job._properties['statistics']['query']
        if config.debug < 2:
            ignore = ('referencedTables', 'statementType', 'queryPlan')
            stats = {k: v for k, v in stats.items() if k not in ignore}
        return {
            'job_id': job.job_id,
            'stats': stats,
            'query': self._trunc_query(job.query)
        }

    def _repository_endpoint(self, *path: str) -> mutable_furl:
        return config.tdr_service_url.set(path=('api', 'repository', 'v1', *path))

    def _check_response(self,
                        endpoint: furl,
                        response: urllib3.HTTPResponse
                        ) -> MutableJSON:
        if response.status == 200:
            return json.loads(response.data)
        elif response.status == 401:
            raise self._insufficient_access(str(endpoint))
        else:
            raise TerraStatusException(endpoint, response)

    page_size: ClassVar[int] = 200

    def snapshot_names_by_id(self) -> dict[str, str]:
        """
        List the TDR snapshots accessible to the current credentials.
        """
        endpoint = self._repository_endpoint('snapshots')
        snapshots = []
        # FIXME: Defend against concurrent changes while listing snapshots
        #        https://github.com/DataBiosphere/azul/issues/3979
        while True:
            endpoint.set(args={
                'offset': len(snapshots),
                'limit': self.page_size,
                'sort': 'created_date',
                'direction': 'asc'
            })
            response = self._request('GET', endpoint)
            response = self._check_response(endpoint, response)
            new_snapshots = response['items']
            if new_snapshots:
                snapshots += new_snapshots
            else:
                total = response['filteredTotal']
                require(len(snapshots) == total, snapshots, total)
                break
        return {
            snapshot['id']: snapshot['name']
            for snapshot in snapshots
        }

    @classmethod
    def for_indexer(cls) -> 'TDRClient':
        return cls(
            credentials_provider=IndexerServiceAccountCredentialsProvider(
                service_account=config.ServiceAccount.indexer
            )
        )

    @classmethod
    def for_anonymous_user(cls) -> 'TDRClient':
        return cls(
            credentials_provider=ServiceAccountCredentialsProvider(
                service_account=config.ServiceAccount.public
            )
        )

    @classmethod
    def for_registered_user(cls, token: OAuth2) -> 'TDRClient':
        return cls(credentials_provider=UserCredentialsProvider(token))

    def drs_client(self):
        return DRSClient(http_client=self._http_client)
