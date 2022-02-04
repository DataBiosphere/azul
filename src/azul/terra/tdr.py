import json
import logging
from time import (
    sleep,
)
from typing import (
    ClassVar,
    Dict,
)

import attr
from furl import (
    furl,
)
from google.api_core.exceptions import (
    Forbidden,
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

from azul import (
    JSON,
    RequirementError,
    cache,
    config,
    require,
)
from azul.bigquery import (
    BigQueryRows,
)
from azul.drs import (
    DRSClient,
)
from azul.indexer import (
    Prefix,
    SourceSpec,
)
from azul.strings import (
    trunc_ellipses,
)
from azul.terra import (
    SAMClient,
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

        >>> TDRSourceSpec.parse('baz:foo:dataset/bar:')
        Traceback (most recent call last):
        ...
        AssertionError: baz

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
        # FIXME: Move parsing of prefix to SourceSpec
        #        https://github.com/DataBiosphere/azul/issues/3073
        service, project, name, prefix = spec.split(':')
        type, name = name.split('/')
        assert service == 'tdr', service
        if type == cls._type_snapshot:
            is_snapshot = True
        elif type == cls._type_dataset:
            is_snapshot = False
        else:
            assert False, type
        self = cls(prefix=Prefix.parse(prefix),
                   project=project,
                   name=name,
                   is_snapshot=is_snapshot)
        assert spec == str(self), (spec, str(self), self)
        return self

    @property
    def bq_name(self):
        return self.name if self.is_snapshot else f'datarepo_{self.name}'

    def __str__(self) -> str:
        """
        The inverse of :meth:`parse`.

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

    def contains(self, other: 'SourceSpec') -> bool:
        """
        >>> p = TDRSourceSpec.parse

        >>> p('tdr:foo:snapshot/bar:').contains(p('tdr:foo:snapshot/bar:'))
        True

        >>> p('tdr:foo:snapshot/bar:').contains(p('tdr:bar:snapshot/bar:'))
        False

        >>> p('tdr:foo:snapshot/bar:').contains(p('tdr:foo:dataset/bar:'))
        False

        >>> p('tdr:foo:snapshot/bar:').contains(p('tdr:foo:snapshot/baz:'))
        False
        """
        return (
            isinstance(other, TDRSourceSpec)
            and super().contains(other)
            and self.is_snapshot == other.is_snapshot
            and self.project == other.project
            and self.name == other.name
        )


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
        resource = f'{source.type_name} {source.name!r} via the TDR API'
        tdr_path = source.type_name + 's'
        endpoint = self._repository_endpoint(tdr_path)
        params = dict(filter=source.bq_name, limit='2')
        response = self._request('GET', endpoint, fields=params)
        response = self._check_response(endpoint, response)
        total = response['filteredTotal']
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
                except Forbidden as e:
                    if 'Exceeded rate limits' in e.message and delay is not None:
                        log.warning('Exceeded BigQuery rate limit during attempt %i/%i. '
                                    'Retrying in %is.',
                                    attempt + 1, len(delays) + 1, delay, exc_info=e)
                        sleep(delay)
                    else:
                        raise e
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

    def _repository_endpoint(self, *path: str) -> str:
        return str(furl(config.tdr_service_url,
                        path=('api', 'repository', 'v1', *path)))

    page_size: ClassVar[int] = 200

    def snapshot_names_by_id(self) -> Dict[str, str]:
        """
        List the TDR snapshots accessible to the current credentials.
        """
        endpoint = self._repository_endpoint('snapshots')
        snapshots = []
        while True:
            response = self._request('GET', endpoint, fields={
                'offset': len(snapshots),
                'limit': self.page_size
            })
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

    def drs_client(self):
        return DRSClient(http_client=self._http_client)
