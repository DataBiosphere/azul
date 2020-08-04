from functools import (
    lru_cache,
)
import json
import logging

import attr
import certifi
from google.auth.transport.requests import (
    Request,
)
from google.auth.transport.urllib3 import (
    AuthorizedHttp,
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
from azul.dss import (
    shared_credentials,
)
from azul.types import (
    JSON,
)

log = logging.getLogger(__name__)


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class TDRSource:
    project: str
    tdr_name: str
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
        TDRSource(project='foo', tdr_name='bar', is_snapshot=True)
        >>> s.bq_name
        'bar'
        >>> str(s)
        'tdr:foo:snapshot/bar'

        >>> d = TDRSource.parse('tdr:foo:dataset/bar')
        >>> d
        TDRSource(project='foo', tdr_name='bar', is_snapshot=False)
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
            return cls(project=project, tdr_name=source_name, is_snapshot=True)
        elif source_type == cls._type_dataset:
            return cls(project=project, tdr_name=source_name, is_snapshot=False)
        else:
            assert False, source_type

    @property
    def bq_name(self):
        return self.tdr_name if self.is_snapshot else f'datarepo_{self.tdr_name}'

    def __str__(self) -> str:
        source_type = self._type_snapshot if self.is_snapshot else self._type_dataset
        return f'tdr:{self.project}:{source_type}/{self.tdr_name}'


class SAMClient:
    """
    A client to Broad's SAM (https://github.com/broadinstitute/sam). TDR uses
    SAM for authorization, and SAM uses Google OAuth for authentication.
    """

    @cached_property
    def credentials(self) -> Credentials:
        with shared_credentials() as file_name:
            return Credentials.from_service_account_file(file_name)

    oauth_scopes = ['email', 'openid']

    @cached_property
    def oauthed_http(self) -> AuthorizedHttp:
        """
        A urllib3 HTTP client with OAuth credentials.
        """
        return AuthorizedHttp(self.credentials.with_scopes(self.oauth_scopes),
                              urllib3.PoolManager(ca_certs=certifi.where()))

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
        else:
            raise RuntimeError('Unexpected response during SAM registration', response.data)

    def get_access_token(self) -> str:
        credentials = self.credentials.with_scopes(self.oauth_scopes)
        credentials.refresh(Request())
        return credentials.token


class TDRClient(SAMClient):

    def verify_authorization(self) -> None:
        """
        Verify that the current service account has repository read access to
        TDR datasets and snapshots.
        """
        # List snapshots
        response = self.oauthed_http.request('GET', self._repository_endpoint('snapshots'))
        if response.status == 200:
            log.info('Google service account is authorized for TDR access.')
        elif response.status == 401:
            raise RequirementError('Google service account is not authorized for TDR access. '
                                   'Make sure that the SA is registered with SAM and has been '
                                   'granted repository read access for datasets and snapshots.')
        else:
            raise RuntimeError('Unexpected response from TDR service', response.status)

    def get_source_id(self, source: TDRSource) -> str:
        """
        Retrieve the ID of a dataset/snapshot from the TDR service API.
        """
        return self._get_source_info(source)['id']

    @lru_cache
    def _get_source_info(self, source: TDRSource) -> JSON:
        endpoint = self._repository_endpoint('snapshots' if source.is_snapshot else 'datasets')
        response = self.oauthed_http.request('GET', endpoint, fields={'filter': source.tdr_name})
        if response.status != 200:
            raise RuntimeError('Failed to list snapshots', response.data)
        items = json.loads(response.data)['items']
        # If the snapshot/dataset's name is a substring of any others' names,
        # then those will be included by the filter
        return one(item for item in items if item['name'] == source.tdr_name)

    def _repository_endpoint(self, path_suffix: str):
        return f'{config.tdr_service_url}/api/repository/v1/{path_suffix}'
