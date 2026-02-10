from abc import (
    ABCMeta,
    abstractmethod,
)
from collections.abc import (
    Sequence,
)
from enum import (
    StrEnum,
    auto,
)
import json
import logging
from time import (
    sleep,
)
from typing import (
    ClassVar,
    Optional,
    Self,
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
    DatasetReference,
    QueryJob,
    QueryJobConfig,
    QueryPriority,
)
from google.cloud.bigquery.table import (
    RowIterator,
)
from more_itertools import (
    one,
)
import urllib3
import urllib3.exceptions
import urllib3.response

from azul import (
    Config,
    R,
    RequirementError,
    cache,
    config,
    mutable_furl,
    reject,
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
    DRSObject,
)
from azul.http import (
    HttpClient,
    LimitedRetryHttpClient,
    LimitedTimeoutException,
    Propagate429HttpClient,
)
from azul.indexer import (
    SourceRef,
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
    json_dict,
    json_int,
    json_list,
    json_mapping,
    json_str,
)

log = logging.getLogger(__name__)


@attrs.frozen(kw_only=True)
class TDRSourceSpec(SourceSpec):
    class Type(StrEnum):
        bigquery = auto()
        parquet = auto()

    class Domain(StrEnum):
        gcp = auto()
        azure = auto()

    type: Type
    domain: Domain
    subdomain: str

    @classmethod
    def parse(cls, spec: str) -> Self:
        """
        Construct an instance from its string representation, using the syntax
        'tdr:{type}:{domain}:{subdomain}:{name}'.

        >>> s = TDRSourceSpec.parse('tdr:bigquery:gcp:foo:bar')
        >>> s # doctest: +NORMALIZE_WHITESPACE
        TDRSourceSpec(name='bar',
                      type=<Type.bigquery: 'bigquery'>,
                      domain=<Domain.gcp: 'gcp'>,
                      subdomain='foo')

        >>> str(s)
        'tdr:bigquery:gcp:foo:bar'

        >>> TDRSourceSpec.parse('tdr:spam:gcp:foo:bar')
        Traceback (most recent call last):
        ...
        ValueError: 'spam' is not a valid TDRSourceSpec.Type

        >>> TDRSourceSpec.parse('tdr:bigquery:eggs:foo:bar')
        Traceback (most recent call last):
        ...
        ValueError: 'eggs' is not a valid TDRSourceSpec.Domain
        """
        # BigQuery (and by extension the TDR) does not allow : or / in dataset names
        service, type, domain, subdomain, name = spec.split(':')
        assert service == 'tdr', service
        type = cls.Type(type)
        reject(type == cls.Type.parquet, 'Parquet sources are not yet supported')
        domain = cls.Domain(domain)
        reject(domain == cls.Domain.azure, 'Azure sources are not yet supported')
        self = cls(type=type,
                   domain=domain,
                   subdomain=subdomain,
                   name=name)
        assert spec == str(self), spec
        return self

    def __str__(self) -> str:
        """
        The inverse of :meth:`parse`.

        >>> s = 'tdr:bigquery:gcp:foo:bar'
        >>> s == str(TDRSourceSpec.parse(s))
        True
        """
        return ':'.join([
            'tdr',
            self.type.value,
            self.domain.value,
            self.subdomain,
            self.name,
        ])

    def qualify_table(self, table_name: str) -> str:
        return '.'.join((self.subdomain, self.name, table_name))


class TDRSourceRef(SourceRef[TDRSourceSpec]):
    pass


class TerraCredentialsProvider(CredentialsProvider, metaclass=ABCMeta):

    @abstractmethod
    def insufficient_access(self, resource: str) -> Exception:
        raise NotImplementedError


@attrs.frozen(kw_only=True)
class ServiceAccountCredentialsProvider(TerraCredentialsProvider):
    service_account: Config.ServiceAccount

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


class TerraStatusException(TerraClientException):

    def __init__(self, url: furl, response: urllib3.response.HTTPResponse):
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

    def _create_http_client(self) -> HttpClient:
        return Propagate429HttpClient(
            LimitedRetryHttpClient(
                super()._create_http_client()
            )
        )

    def _request(self,
                 method: str,
                 url: furl,
                 *,
                 headers=None,
                 body=None
                 ) -> urllib3.HTTPResponse:
        response = self._http_client.request(method,
                                             str(url),
                                             headers=headers,
                                             body=body)

        assert isinstance(response, urllib3.HTTPResponse)
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
        email = self.service_account_credentials.service_account_email
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


class TDRClient(SAMClient, DRSClient):
    """
    A client for the Broad Institute's Terra Data Repository aka "Jade".
    """

    @cache
    def lookup_source(self, source_spec: TDRSourceSpec) -> str:
        """
        Validate that the repository's reported values for the snapshot's Google
        project name and storage location match our expectations, and return the
        snapshot's UUID.
        """
        source = self._lookup_source(source_spec)
        actual_project = source['dataProject']
        require(actual_project == source_spec.subdomain,
                'Actual Google project of TDR source differs from configured one',
                actual_project, source_spec.subdomain)
        storage = one(
            resource
            for resource in map(json_dict, json_list(source['storage']))
            if json_str(resource['cloudResource']) == 'bigquery'
        )
        actual_location = json_str(storage['region'])
        # Uppercase is standard for multi-regions in the documentation but TDR
        # returns 'us' in lowercase
        require(actual_location.lower() == config.tdr_source_location.lower(),
                'Actual storage location of TDR source differs from configured one',
                actual_location, config.tdr_source_location)
        return json_str(source['id'])

    def _retrieve_source(self, source: TDRSourceRef) -> MutableJSON:
        endpoint = self._repository_endpoint('snapshots', source.id)
        response = self._request('GET', endpoint)
        response = self._check_response(endpoint, response)
        require(source.spec.name == response['name'],
                'Source name changed unexpectedly', source, response)
        return response

    def _lookup_source(self, source: TDRSourceSpec) -> MutableJSON:
        endpoint = self._repository_endpoint('snapshots')
        endpoint.set(args=dict(filter=source.name, limit='2'))
        response = self._request('GET', endpoint)
        response = self._check_response(endpoint, response)
        total = json_int(response['filteredTotal'])
        if total == 0:
            raise self._insufficient_access(str(endpoint))
        elif total == 1:
            return json_dict(one(json_list(response['items'])))
        else:
            raise TerraNameConflictException(endpoint, source.name, response)

    def check_bigquery_access(self, source: TDRSourceSpec):
        """
        Verify that the client is authorized to read from TDR BigQuery tables.
        """
        resource = f'BigQuery dataset {source.name!r} in Google Cloud project {source.subdomain!r}'
        try:
            self.run_sql(f'''
                SELECT *
                FROM `{source.subdomain}.{source.name}.INFORMATION_SCHEMA.TABLES`
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

    class _EmptySQLResult(Exception):
        pass

    _retryable_exceptions = (
        BadRequest,
        Forbidden,
        InternalServerError,
        ServiceUnavailable,
        _EmptySQLResult
    )

    def run_sql(self, query: str) -> BigQueryRows:
        bigquery = self._bigquery(self.service_account_credentials.project_id)
        if log.isEnabledFor(logging.DEBUG):
            log.debug('Query (%r characters total): %r',
                      len(query), self._trunc_query(query))
        job: QueryJob
        if config.bigquery_batch_mode:
            job_config = QueryJobConfig(priority=QueryPriority.BATCH)
            job = bigquery.query(query, job_config=job_config)
            result = job.result()
            job_info = self._job_info(job, result)
        else:
            delays = (10, 20, 40, 80)
            assert sum(delays) < config.contribution_lambda_timeout(retry=False)
            for attempt, delay in enumerate((*delays, None)):
                job = bigquery.query(query)
                try:
                    result = job.result()
                    job_info = self._job_info(job, result)
                    if not self._job_has_result(job_info):
                        raise self._EmptySQLResult
                except self._retryable_exceptions as e:
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
            log.debug('Job info: %s', json.dumps(job_info))
        return result

    def list_tables(self, source: TDRSourceSpec) -> set[str]:
        bigquery = self._bigquery(self.service_account_credentials.project_id)
        ref = DatasetReference(project=source.subdomain, dataset_id=source.name)
        return {
            table.to_api_repr()['tableReference']['tableId']
            for table in bigquery.list_tables(ref)
        }

    def _trunc_query(self, query: str) -> str:
        return trunc_ellipses(query, 2048)

    def _job_info(self, job: QueryJob, result: RowIterator) -> JSON:
        # noinspection PyProtectedMember
        stats = job._properties['statistics']['query']
        if config.debug < 2:
            ignore = ('referencedTables', 'statementType', 'queryPlan')
            stats = {k: v for k, v in stats.items() if k not in ignore}
        return {
            'job_id': job.job_id,
            'total_rows': result.total_rows,
            'stats': stats,
            'query': self._trunc_query(job.query)
        }

    def _job_has_result(self, job_info: JSON) -> bool:
        # In order to detect when a BigQuery job silently fails (i.e. returns
        # no rows when there should be some), we check for an expected field in
        # the jobs stats that we have observed missing in prior silent failures.
        return 'totalBytesProcessed' in json_mapping(job_info['stats'])

    def _repository_endpoint(self, *path: str) -> mutable_furl:
        return config.tdr_service_url.set(path=('api', 'repository', 'v1', *path))

    def _duos_endpoint(self, *path: str) -> mutable_furl:
        url = config.duos_service_url
        assert url is not None
        return url.set(path=('api', *path))

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
        response = self._request('GET', endpoint, headers={'Connection': 'close'})
        response = self._check_response(endpoint, response)
        return set(json_dict(response['roleMap']).keys())

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
                json_str(snapshot['id']): json_str(snapshot['name'])
                for snapshot in map(json_dict, json_list(response['items']))
            })
            after = len(snapshots)
            total = json_int(response['filteredTotal'])
            if after == total:
                break
            elif after > total or after == before:
                # Something is off if we got more snapshots than reported by TDR
                # or if there was no progress even though we got fewer than that.
                raise TerraConcurrentModificationException()
            before = after
        return snapshots

    @classmethod
    def for_indexer(cls) -> Self:
        return cls(
            credentials_provider=IndexerServiceAccountCredentialsProvider(
                service_account=config.ServiceAccount.indexer
            )
        )

    @classmethod
    def for_anonymous_user(cls) -> Self:
        return cls(
            credentials_provider=ServiceAccountCredentialsProvider(
                service_account=config.ServiceAccount.public
            )
        )

    @classmethod
    def for_registered_user(cls, authentication: OAuth2) -> Self:
        self = cls(credentials_provider=UserCredentialsProvider(authentication))
        try:
            self.validate()
        except AssertionError as e:
            if R.caused(e):
                log.warning('Invalid credentials', exc_info=e)
                raise UnauthorizedError('Invalid credentials')
            else:
                raise
        else:
            return self

    def drs_object(self, drs_url: furl) -> DRSObject:
        return DRSObject(url=drs_url, http_client=self._http_client)

    def get_duos(self,
                 source: TDRSourceRef
                 ) -> tuple[str, MutableJSON] | tuple[None, None]:
        """
        Return the DUOS ID and DUOS dataset registration information for the
        given TDR snapshot.
        """
        body = self._retrieve_source(source)
        try:
            duos_id = json_str(json_dict(body['duosFirecloudGroup'])['duosId'])
        except (KeyError, AssertionError):
            log.warning('No DUOS ID available for %r', source.spec)
            return None, None
        else:
            url = self._duos_endpoint('dataset', 'registration', duos_id)
            try:
                response = self._request('GET', url)
            except LimitedTimeoutException:
                body = {'studyDescription': '[Description currently not available]'}
                return duos_id, body
            if response.status == 401:
                body = {'studyDescription': '[Description currently not accessible]'}
                return duos_id, body
            elif response.status == 404:
                log.warning('No DUOS dataset registration with ID %r from %r',
                            duos_id, source.spec)
                return None, None
            else:
                body = self._check_response(url, response)
                consent_group = json_dict(one(json_list(body['consentGroups'])))
                require(duos_id == json_str(consent_group['datasetIdentifier']),
                        'Mismatched identifiers', duos_id, consent_group)
                return duos_id, body
