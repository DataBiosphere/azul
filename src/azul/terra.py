from functools import (
    lru_cache,
)
import json
import logging
from time import (
    sleep,
)

import attr
import certifi
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
from google.cloud.bigquery.table import (
    TableListItem,
)
from google.oauth2.service_account import (
    Credentials,
)
from more_itertools import (
    one,
)
import urllib3

from azul import (
    RequirementError,
    cached_property,
    config,
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

log = logging.getLogger(__name__)


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class TDRSource:
    project: str
    name: str
    is_snapshot: bool

    _type_dataset = 'dataset'

    _type_snapshot = 'snapshot'

    @classmethod
    def parse(cls, source: str) -> 'TDRSource':
        """
        Construct an instance from its string representation, using the syntax
        'tdr:{project}:{source_type}/{source_name}'.

        >>> s = TDRSource.parse('tdr:foo:snapshot/bar')
        >>> s
        TDRSource(project='foo', name='bar', is_snapshot=True)
        >>> s.bq_name
        'bar'
        >>> str(s)
        'tdr:foo:snapshot/bar'

        >>> d = TDRSource.parse('tdr:foo:dataset/bar')
        >>> d
        TDRSource(project='foo', name='bar', is_snapshot=False)
        >>> d.bq_name
        'datarepo_bar'
        >>> str(d)
        'tdr:foo:dataset/bar'

        >>> TDRSource.parse('baz:foo:dataset/bar')
        Traceback (most recent call last):
        ...
        AssertionError: baz

        >>> TDRSource.parse('tdr:foo:baz/bar')
        Traceback (most recent call last):
        ...
        AssertionError: baz
        """
        # BigQuery (and by extension the TDR) does not allow : or / in dataset names
        service, project, source = source.split(':')
        source_type, source_name = source.split('/')
        assert service == 'tdr', service
        if source_type == cls._type_snapshot:
            return cls(project=project, name=source_name, is_snapshot=True)
        elif source_type == cls._type_dataset:
            return cls(project=project, name=source_name, is_snapshot=False)
        else:
            assert False, source_type

    @property
    def bq_name(self):
        return self.name if self.is_snapshot else f'datarepo_{self.name}'

    def __str__(self) -> str:
        source_type = self._type_snapshot if self.is_snapshot else self._type_dataset
        return f'tdr:{self.project}:{source_type}/{self.name}'

    @property
    def type_name(self):
        return self._type_snapshot if self.is_snapshot else self._type_dataset

    def qualify_table(self, table_name: str) -> str:
        return '.'.join((self.project, self.bq_name, table_name))


class TerraClient:
    """
    A client to a service in the Broad Institute's Terra ecosystem.
    """

    @cached_property
    def credentials(self) -> Credentials:
        with aws.service_account_credentials() as file_name:
            return Credentials.from_service_account_file(file_name)

    oauth_scopes = [
        'email',
        'openid',
        'https://www.googleapis.com/auth/devstorage.read_only'
    ]

    @cached_property
    def oauthed_http(self) -> AuthorizedHttp:
        """
        A urllib3 HTTP client with OAuth credentials.
        """
        return AuthorizedHttp(self.credentials.with_scopes(self.oauth_scopes),
                              urllib3.PoolManager(ca_certs=certifi.where()))

    def get_access_token(self) -> str:
        credentials = self.credentials.with_scopes(self.oauth_scopes)
        credentials.refresh(Request())
        return credentials.token


class SAMClient(TerraClient):
    """
    A client to Broad's SAM (https://github.com/broadinstitute/sam). TDR uses
    SAM for authorization, and SAM uses Google OAuth for authentication.
    """

    def register_with_sam(self) -> None:
        """
        Register the current service account with SAM.

        https://github.com/DataBiosphere/jade-data-repo/blob/develop/docs/register-sa-with-sam.md
        """
        token = self.get_access_token()
        response = self.oauthed_http.request('POST',
                                             f'{config.sam_service_url}/register/user/v1',
                                             body='',
                                             headers={'Authorization': f'Bearer {token}'})
        if response.status == 201:
            log.info('Google service account successfully registered with SAM.')
        elif response.status == 409:
            log.info('Google service account previously registered with SAM.')
        elif response.status == 500 and b'Cannot update googleSubjectId' in response.data:
            raise RuntimeError(
                'Unable to register service account. SAM does not allow re-registration of a '
                'new service account whose name matches that of another previously registered '
                'service account. Please refer to the troubleshooting section of the README.',
                self.credentials.service_account_email
            )
        else:
            raise RuntimeError('Unexpected response during SAM registration', response.data)

    def _insufficient_access(self, resource: str):
        return RequirementError(
            f'The service account (SA) {self.credentials.service_account_email!r} is not '
            f'authorized to access {resource} or that resource does not exist. Make sure '
            f'that it exists, that the SA is registered with SAM and has been granted read '
            f'access to the resource.'
        )


class TDRClient(SAMClient):
    """
    A client for the Broad Institute's Terra Data Repository aka "Jade".
    """

    def check_api_access(self, source: TDRSource) -> None:
        """
        Verify that the client is authorized to read from the TDR service API.
        """
        resource = f'{source.type_name} {source.name!r} via the TDR API'
        tdr_path = source.type_name + 's'
        endpoint = self._repository_endpoint(tdr_path)
        params = dict(filter=source.bq_name, limit='2')
        response = self.oauthed_http.request('GET', endpoint, fields=params)
        if response.status == 200:
            response = json.loads(response.data)
            total = response['total']
            if total == 0:
                raise self._insufficient_access(resource)
            elif total == 1:
                snapshot_id = one(response['items'])['id']
                endpoint = self._repository_endpoint(tdr_path, snapshot_id)
                response = self.oauthed_http.request('GET', endpoint)
                if response.status == 200:
                    response = json.loads(response.data)
                    # FIXME: Response now contains a reference to the Google
                    #        project name in a property called `dataProject`.
                    #        Use this approach (or reuse this code) to avoid
                    #        hardcoding the project ID.
                    #        https://github.com/DataBiosphere/azul/issues/2504
                    log.info('TDR client is authorized for API access to %s: %r', resource, response)
                else:
                    assert False, snapshot_id
            else:
                raise RuntimeError('Ambiguous response from TDR API', endpoint)
        elif response.status == 401:
            raise self._insufficient_access(endpoint)
        else:
            raise RuntimeError('Unexpected response from TDR API', response.status)

    def check_bigquery_access(self, source: TDRSource):
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

    @lru_cache(maxsize=None)
    def _bigquery(self, project: str) -> bigquery.Client:
        with aws.service_account_credentials():
            return bigquery.Client(project=project)

    def run_sql(self, query: str) -> BigQueryRows:
        delays = (10, 20, 40, 80)
        assert sum(delays) < config.contribution_lambda_timeout
        for attempt, delay in enumerate((*delays, None)):
            job = self._bigquery(self.credentials.project_id).query(query)
            try:
                return job.result()
            except Forbidden as e:
                if 'Exceeded rate limits' in e.message and delay is not None:
                    log.warning('Exceeded BigQuery rate limit during attempt %i/%i. '
                                'Retrying in %is.',
                                attempt + 1, len(delays) + 1, delay, exc_info=e)
                    sleep(delay)
                else:
                    raise e
        assert False

    def _repository_endpoint(self, *path: str) -> str:
        return furl(config.tdr_service_url,
                    path=('api', 'repository', 'v1', *path)).url


class TerraDRSClient(DRSClient, TerraClient):

    def __init__(self) -> None:
        super().__init__(http_client=self.oauthed_http)
