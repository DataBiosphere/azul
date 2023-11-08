from abc import (
    ABCMeta,
    abstractmethod,
)
from collections.abc import (
    Sequence,
)
import json
import logging
from time import (
    sleep,
    time,
)
from typing import (
    ClassVar,
    Optional,
)

import attrs
from chalice import (
    UnauthorizedError,
)
from furl import (
    furl,
)
from google.api_core.exceptions import (
    BadRequest,
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
    MaxRetryError,
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
    SourceRef as BaseSourceRef,
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


@attrs.frozen(kw_only=True)
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


class SourceRef(BaseSourceRef[TDRSourceSpec, 'TDRSourceRef']):
    pass


class TerraCredentialsProvider(CredentialsProvider, metaclass=ABCMeta):

    @abstractmethod
    def insufficient_access(self, resource: str) -> Exception:
        raise NotImplementedError


@attrs.frozen(kw_only=True)
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

    def __init__(self, authentication: OAuth2):
        self.token = authentication.identity()

    def oauth2_scopes(self) -> Sequence[str]:
        return ['https://www.googleapis.com/auth/userinfo.email']

    @cache
    def scoped_credentials(self) -> TokenCredentials:
        # FIXME: this assumes the user has selected all required scopes.
        return TokenCredentials(self.token, scopes=self.oauth2_scopes())

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


class TerraConcurrentModificationException(TerraClientException):

    def __init__(self) -> None:
        super().__init__('Snapshot listing changed while we were paging through it')


@attrs.frozen(kw_only=True)
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
        retries = config.terra_client_retries
        log.debug('_request(%r, %s, headers=%r, timeout=%r, body=%r)',
                  method, url, headers, timeout, body)
        start = time()
        try:
            # Limited retries on I/O errors such as refused or dropped
            # connections. The latter are actually very likely if connections
            # from the pool are reused after a long period of idleness.
            retry = urllib3.Retry(total=None,
                                  connect=retries,
                                  read=0,
                                  redirect=0,
                                  status=0,
                                  other=retries)
            response = self._http_client.request(method,
                                                 str(url),
                                                 headers=headers,
                                                 # FIXME: Service should return 503 response when Terra client times out
                                                 #        https://github.com/DataBiosphere/azul/issues/3968
                                                 timeout=timeout,
                                                 retries=retry,
                                                 body=body)
        except (TimeoutError, MaxRetryError):
            raise TerraTimeoutException(url, timeout)

        assert isinstance(response, urllib3.HTTPResponse)
        if log.isEnabledFor(logging.DEBUG):
            log.debug('_request(…) -> %.3fs %r %r',
                      time() - start, response.status, trunc_ellipses(response.data, 256))
        header_name = 'WWW-Authenticate'
        try:
            header_value = response.headers[header_name]
        except KeyError:
            pass
        else:
            log.warning('_request(…) -> %r: %r', header_name, header_value)
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
        endpoint = config.sam_service_url.set(path='/register/user/v1')
        response = self._request('GET', endpoint)
        auth_header = response.headers.get('WWW-Authenticate')
        if response.status == 200:
            return True
        elif response.status == 404:
            return False
        elif response.status == 401 and auth_header and 'invalid_token' in auth_header:
            raise PermissionError('The provided authentication is invalid')
        else:
            raise TerraStatusException(endpoint, response)

    def _insufficient_access(self, resource: str) -> Exception:
        return self.credentials_provider.insufficient_access(resource)


class TDRClient(SAMClient):
    """
    A client for the Broad Institute's Terra Data Repository aka "Jade".
    """

    # FIXME: Eliminate azul.terra.TDRClient.TDRSource
    #        https://github.com/DataBiosphere/azul/issues/5524
    @attrs.frozen(kw_only=True)
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

    def _retrieve_source(self, source: SourceRef) -> MutableJSON:
        endpoint = self._repository_endpoint(source.spec.type_name + 's', source.id)
        response = self._request('GET', endpoint)
        response = self._check_response(endpoint, response)
        require(source.spec.name == response['name'],
                'Source name changed unexpectedly', source, response)
        return response

    def _lookup_source(self, source: TDRSourceSpec) -> MutableJSON:
        endpoint = self._repository_endpoint(source.type_name + 's')
        endpoint.set(args=dict(filter=source.bq_name, limit='2'))
        response = self._request('GET', endpoint)
        response = self._check_response(endpoint, response)
        total = response['filteredTotal']
        if total == 0:
            raise self._insufficient_access(str(endpoint))
        elif total == 1:
            source_id = one(response['items'])['id']
            return self._retrieve_source(SourceRef(id=source_id, spec=source))
        else:
            raise TerraNameConflictException(endpoint, source.bq_name, response)

    def check_bigquery_access(self, source: TDRSourceSpec):
        """
        Verify that the client is authorized to read from TDR BigQuery tables.
        """
        resource = f'BigQuery dataset {source.bq_name!r} in Google Cloud project {source.project!r}'
        try:
            self.run_sql(f'''
                SELECT *
                FROM `{source.project}.{source.bq_name}.INFORMATION_SCHEMA.TABLES`
                LIMIT 1
            ''')
        except Forbidden:
            raise self._insufficient_access(resource)
        else:
            log.info('TDR client is authorized to access tables in %s', resource)

    @cache
    def _bigquery(self, project: str) -> bigquery.Client:
        # We get a false warning from PyCharm here, probably because of
        #
        # https://youtrack.jetbrains.com/issue/PY-23400/regression-PEP484-type-annotations-in-docstrings-nearly-completely-broken
        #
        # Google uses the docstring syntax to annotate types in its BQ client.
        #
        # noinspection PyTypeChecker
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
                except (BadRequest, Forbidden, InternalServerError, ServiceUnavailable) as e:
                    if delay is None:
                        raise e
                    elif isinstance(e, Forbidden) and 'Exceeded rate limits' not in e.message:
                        raise e
                    elif (isinstance(e, BadRequest)
                          and 'project does not have the reservation in the data region' not in e.message):
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
        # FIXME: Azul sometimes conflates 401 and 403
        #        https://github.com/DataBiosphere/azul/issues/4463
        elif response.status in (401, 403):
            raise self._insufficient_access(str(endpoint))
        else:
            raise TerraStatusException(endpoint, response)

    page_size: ClassVar[int] = 1000

    def snapshot_ids(self) -> set[str]:
        """
        List the IDs of the TDR snapshots accessible to the current credentials.
        Much faster than listing the snapshots' names.
        """
        endpoint = self._repository_endpoint('snapshots', 'roleMap')
        response = self._request('GET', endpoint)
        response = self._check_response(endpoint, response)
        return set(response['roleMap'].keys())

    def snapshot_names_by_id(self,
                             *,
                             filter: Optional[str] = None
                             ) -> dict[str, str]:
        """
        List the TDR snapshots accessible to the current credentials.

        :param filter: Unless None, a string that must occur in the description
                       or name of the snapshots to be listed
        """
        # For reference: https://github.com/DataBiosphere/jade-data-repo/blob
        # /22ff5c57d46db42c874639e1ffa6ad833c51e29f
        # /src/main/java/bio/terra/service/snapshot/SnapshotDao.java#L550
        #
        # The creation of a snapshot is only one of the two ways a snapshot is
        # added to the list. The other way is making an existing snapshot
        # accessible. Sorting by creation date only defends against the first
        # scenario, not the second. Also note that as we page through
        # snapshots, a snapshot we already retrieved might be removed and
        # another one added. If the added one precedes the current page, we
        # won't notice at all.
        #
        endpoint = self._repository_endpoint('snapshots')
        snapshots = {}
        before = 0
        while True:
            args = dict(offset=before,
                        limit=self.page_size,
                        sort='created_date',
                        direction='asc')
            if filter is not None:
                args['filter'] = filter
            endpoint.set(args=args)
            response = self._request('GET', endpoint)
            response = self._check_response(endpoint, response)
            snapshots.update({
                snapshot['id']: snapshot['name']
                for snapshot in response['items']
            })
            after = len(snapshots)
            total = response['filteredTotal']
            if after == total:
                break
            elif after > total or after == before:
                # Something is off if we got more snapshots than reported by TDR
                # or if there was no progress even though we got fewer than that.
                raise TerraConcurrentModificationException()
            before = after
        return snapshots

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
    def for_registered_user(cls, authentication: OAuth2) -> 'TDRClient':
        self = cls(credentials_provider=UserCredentialsProvider(authentication))
        try:
            self.validate()
        except RequirementError as e:
            log.warning('Invalid credentials', exc_info=e)
            raise UnauthorizedError('Invalid credentials')
        else:
            return self

    def drs_client(self) -> DRSClient:
        return DRSClient(http_client=self._http_client)
