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
    Callable,
    Iterable,
    TypeVar,
)

from chalice import (
    UnauthorizedError,
)
from furl import (
    furl,
)

from azul import (
    cache_per_thread,
    cached_property,
    config,
    require,
)
from azul.auth import (
    Authentication,
    OAuth2,
)
from azul.bigquery import (
    BigQueryRows,
    backtick,
)
from azul.drs import (
    AccessMethod,
    DRSObject,
)
from azul.indexer import (
    Bundle,
    SourcedBundleFQID,
)
from azul.plugins import (
    RepositoryFileDownload,
    RepositoryPlugin,
)
from azul.strings import (
    longest_common_prefix,
)
from azul.terra import (
    TDRClient,
    TDRSourceRef,
    TDRSourceSpec,
)
from azul.time import (
    format_dcp2_datetime,
    parse_dcp2_version,
)
from azul.types import (
    JSON,
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

    def _auth_fallback(self,
                       authentication: Authentication | None,
                       tdr_callback: Callable[[TDRClient], T]
                       ) -> T:
        # The line below raises UnauthorizedError for invalid tokens. We don't
        # want to fall back to anonymous authentication in that case.
        tdr = self._user_authenticated_tdr(authentication)
        try:
            return tdr_callback(tdr)
        except UnauthorizedError:
            if authentication is None or tdr.is_registered():
                raise
            else:
                # Fall back to anonymous access if the request is authenticated
                # using an unregistered account.
                tdr = self._user_authenticated_tdr(None)
                return tdr_callback(tdr)

    @cached_property
    def _common_source_filter(self) -> str:
        # We filter by prefix of snapshot names in an attempt to speed up the
        # listing by limiting the number of irrelevant snapshots returned. Note
        # that TDR does a substring match, not a prefix match, but determining
        # the longest common substring is complicated and, as of yet, I haven't
        # found a trustworthy, reusable implementation.
        return longest_common_prefix(spec.name for spec in self.sources)

    def list_accessible_sources(self,
                                authentication: Authentication | None
                                ) -> list[TDRSourceRef]:
        names_by_id = self._auth_fallback(authentication,
                                          lambda tdr: tdr.snapshot_names_by_id(filter=self._common_source_filter))
        return self._match_sources(names_by_id)

    def list_accessible_source_ids(self,
                                   authentication: Authentication | None
                                   ) -> set[str]:
        return self._auth_fallback(authentication,
                                   lambda tdr: tdr.snapshot_ids())

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
    def _user_authenticated_tdr(cls,
                                authentication: Authentication | None
                                ) -> TDRClient:
        if authentication is None:
            tdr = TDRClient.for_anonymous_user()
        elif isinstance(authentication, OAuth2):
            tdr = TDRClient.for_registered_user(authentication)
        else:
            raise PermissionError('Unsupported authentication format',
                                  type(authentication))
        return tdr

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
            drs_client = self._user_authenticated_tdr(authentication)
        else:
            drs_client = self._unauthenticated_drs
        return drs_client.drs_object(drs_url)

    def file_download_class(self) -> type[RepositoryFileDownload]:
        return TDRFileDownload

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


class TDRFileDownload(RepositoryFileDownload):
    _location: str | None = None

    needs_drs_uri = True

    def update(self,
               plugin: RepositoryPlugin,
               authentication: Authentication | None
               ) -> None:
        require(self.replica is None or self.replica == 'gcp')
        if self.file.drs_uri is None:
            assert self.location is None, self
            assert self.retry_after is None, self
        else:
            drs_client = plugin.drs_object(self.file.drs_uri, authentication)
            access = drs_client.get(access_method=AccessMethod.gs)
            require(access.method is AccessMethod.https, access.method)
            require(access.headers is None, access.headers)
            signed_url = access.url
            args = furl(signed_url).args
            require('X-Goog-Signature' in args, args)
            self._location = signed_url

    @property
    def location(self) -> str | None:
        return self._location

    @property
    def retry_after(self) -> int | None:
        return None
