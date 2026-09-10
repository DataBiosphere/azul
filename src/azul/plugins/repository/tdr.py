from abc import (
    ABC,
    abstractmethod,
)
from collections import (
    defaultdict,
)
import datetime
import logging
import time
from typing import (
    Iterable,
    TypeVar,
)

from azul import (
    config,
)
from azul.auth import (
    Authentication,
    BearerTokenAuthentication,
    indexer_authentication,
)
from azul.drs import (
    AccessMethod,
    DRSObject,
)
from azul.indexer import (
    Bundle,
    SourcedBundleFQID,
)
from azul.lib import (
    R,
    cache_per_thread,
    cached_property,
)
from azul.lib.bigquery import (
    BigQueryRows,
    backtick,
)
from azul.lib.strings import (
    assert_signed_url_redactable,
    longest_common_prefix,
)
from azul.lib.time import (
    format_dcp2_datetime,
    parse_dcp2_version,
)
from azul.lib.types import (
    JSON,
)
from azul.plugins import (
    File,
    RepositoryPlugin,
)
from azul.terra import (
    TDRClient,
    TDRSourceRef,
    TDRSourceSpec,
)

log = logging.getLogger(__name__)


class TDRBundleFQID(SourcedBundleFQID[TDRSourceRef]):
    pass


class TDRBundle(Bundle[TDRBundleFQID], ABC):

    @classmethod
    def canning_qualifier(cls):
        return 'tdr'

    def drs_uri(self, manifest_entry: JSON) -> str | None:
        return manifest_entry.get('drs_uri')


T = TypeVar('T')

TDR_BUNDLE = TypeVar('TDR_BUNDLE', bound=TDRBundle)


class TDRPlugin[TDR_BUNDLE: TDRBundle,
                TDR_BUNDLE_FQID: TDRBundleFQID](
    RepositoryPlugin[
        TDR_BUNDLE,
        TDRSourceSpec,
        TDRSourceRef,
        TDR_BUNDLE_FQID
    ]
):

    @cached_property
    def _common_source_filter(self) -> str:
        # We filter by prefix of snapshot names in an attempt to speed up the
        # listing by limiting the number of irrelevant snapshots returned. Note
        # that TDR does a substring match, not a prefix match, but determining
        # the longest common substring is complicated and, as of yet, I haven't
        # found a trustworthy, reusable implementation.
        return longest_common_prefix(spec.name for spec in self.sources)

    def list_sources(self,
                     authentication: Authentication | None
                     ) -> list[TDRSourceRef]:
        tdr = self._authenticated_tdr(authentication)
        snapshots_by_id = tdr.list_snapshots(filter=self._common_source_filter)
        return [
            TDRSourceRef(id=id,
                         spec=TDRSourceSpec(name=snapshot['name'],
                                            type=TDRSourceSpec.Type.bigquery,
                                            domain=TDRSourceSpec.Domain.gcp,
                                            subdomain=snapshot['dataProject']),
                         prefix=None)
            for id, snapshot in snapshots_by_id.items()
        ]

    def list_source_ids(self,
                        authentication: Authentication | None
                        ) -> set[str]:
        tdr = self._authenticated_tdr(authentication)
        return tdr.list_snapshot_ids()

    @property
    def tdr(self):
        return self._tdr()

    # To utilize the caching of certain responses that's occurring within
    # the TDR and DRS client instances (from the TDR API and identifiers.org,
    # respectively), we need to cache these client instances. If we cached the
    # client instances within the plugin instance, we would get one client
    # instance per plugin instance. The plugin is instantiated frequently and in
    # a variety of contexts.
    #
    # Because of that, caching the plugin instances would be a more invasive
    # change than simply caching the client instances per plugin class. That's
    # why these are class methods. The clients use urllib3, whose thread-safety
    # is disputed (https://github.com/urllib3/urllib3/issues/1252), so have to
    # cache client instances per-class AND per-thread.

    @classmethod
    @cache_per_thread
    def _tdr(cls):
        return TDRClient.for_indexer()

    @classmethod
    @cache_per_thread
    def _authenticated_tdr(cls, authentication: Authentication | None) -> TDRClient:
        if authentication is None:
            return TDRClient.for_anonymous_user()
        elif authentication is indexer_authentication:
            return cls._tdr()
        elif isinstance(authentication, BearerTokenAuthentication):
            return TDRClient.for_registered_user(authentication)
        else:
            raise PermissionError('Unsupported authentication', type(authentication))

    def _lookup_source_id(self, spec: TDRSourceSpec) -> str:
        return self.tdr.lookup_source(spec)

    def fetch_bundle(self, bundle_fqid: TDRBundleFQID) -> TDR_BUNDLE:
        self._assert_source(bundle_fqid.source)
        now = time.time()
        bundle = self._emulate_bundle(bundle_fqid)
        log.info('It took %.003fs to download bundle %s.%s',
                 time.time() - now, bundle.uuid, bundle.version)
        return bundle

    @classmethod
    def format_version(cls, version: datetime.datetime) -> str:
        return format_dcp2_datetime(version)

    def _run_sql(self, query) -> BigQueryRows:
        return self.tdr.run_sql(query)

    def _full_table_name(self, source: TDRSourceSpec, table_name: str) -> str:
        return source.qualify_table(table_name)

    @abstractmethod
    def _emulate_bundle(self, bundle_fqid: TDRBundleFQID) -> TDR_BUNDLE:
        raise NotImplementedError

    def drs_object(self,
                   drs_uri: str,
                   authentication: Authentication | None = None
                   ) -> DRSObject:
        drs_url = self._resolve_drs_uri(drs_uri)
        tdr_url = config.tdr_service_url
        # Authenticate only if the DRS server is TDR so that we don't leak user
        # or service account tokens to untrusted servers.
        if (drs_url.scheme, drs_url.host) == (tdr_url.scheme, tdr_url.host):
            drs_client = self._authenticated_tdr(authentication)
        else:
            drs_client = self._unauthenticated_drs
        return drs_client.drs_object(drs_url)

    def file_download_url(self,
                          file: File,
                          authentication: Authentication | None,
                          replica: str | None = None,
                          *,
                          requester_pays: bool = False
                          ) -> str | None:
        assert replica is None or replica == 'gcp', R(
            'Invalid replica', replica)
        if file.drs_uri is None:
            return None
        else:
            if requester_pays and config.tdr_requester_pays_project is not None:
                headers = {'x-user-project': config.tdr_requester_pays_project}
            else:
                headers = None
            drs_client = self.drs_object(file.drs_uri, authentication)
            access = drs_client.get(access_method=AccessMethod.gs, headers=headers)
            assert access.method is AccessMethod.https, R(str(access.method))
            assert access.headers is None, R(str(access.headers))
            signed_url = access.url
            assert_signed_url_redactable(signed_url)
            return signed_url

    def validate_version(self, version: str) -> None:
        parse_dcp2_version(version)

    def find_in_source(self,
                       source: TDRSourceSpec,
                       string: str
                       ) -> Iterable[JSON]:
        log.info('Validating snapshot %s', source)
        query = f'''
            SELECT table_name, column_name
            FROM {backtick(self._full_table_name(source, 'INFORMATION_SCHEMA.COLUMNS'))}
        '''
        table_columns = defaultdict(list)
        for row in self._run_sql(query):
            table_name, column_name = row['table_name'], row['column_name']
            assert isinstance(table_name, str), table_name
            assert isinstance(column_name, str), column_name
            table_columns[table_name].append(column_name)
        for table_name, columns in table_columns.items():
            log.info('Validating table %s', table_name)
            for column in columns:
                query = f'''
                    SELECT datarepo_row_id, {column}
                    FROM {backtick(self._full_table_name(source, table_name))}
                    WHERE CONTAINS_SUBSTR({column}, {string!r})
                '''
                for row in self._run_sql(query):
                    match = {
                        'catalog': self.catalog,
                        'spec': str(source),
                        'table': table_name,
                        'column': column,
                        'row_id': row['datarepo_row_id'],
                        'value': row[column]
                    }
                    log.warning('Undesired string found: %r', match)
                    yield match
