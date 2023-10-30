from abc import (
    ABC,
    abstractmethod,
)
from collections.abc import (
    Sequence,
    Set,
)
import datetime
import logging
import time
from typing import (
    Callable,
    Optional,
    Type,
    TypeVar,
)

import attr
from chalice import (
    UnauthorizedError,
)
from furl import (
    furl,
)

from azul import (
    CatalogName,
    cache_per_thread,
    config,
    require,
)
from azul.auth import (
    Authentication,
    OAuth2,
)
from azul.drs import (
    AccessMethod,
    DRSClient,
)
from azul.indexer import (
    BUNDLE_FQID,
    Bundle,
    SOURCE_REF,
    SOURCE_SPEC,
    SourcedBundleFQID,
)
from azul.plugins import (
    BUNDLE,
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

    def drs_uri(self, manifest_entry: JSON) -> Optional[str]:
        return manifest_entry.get('drs_uri')


T = TypeVar('T')


@attr.s(kw_only=True, auto_attribs=True, frozen=True)
class TDRPlugin(RepositoryPlugin[BUNDLE, SOURCE_SPEC, SOURCE_REF, BUNDLE_FQID]):
    _sources: Set[TDRSourceSpec]

    @classmethod
    def create(cls, catalog: CatalogName) -> 'RepositoryPlugin':
        return cls(sources=frozenset(
            TDRSourceSpec.parse(spec)
            for spec in config.sources(catalog))
        )

    @property
    def sources(self) -> Set[TDRSourceSpec]:
        return self._sources

    def _auth_fallback(self,
                       authentication: Optional[Authentication],
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

    def list_sources(self,
                     authentication: Optional[Authentication]
                     ) -> list[TDRSourceRef]:
        configured_specs_by_name = {spec.name: spec for spec in self.sources}
        # Filter by prefix of snapshot names in an attempt to speed up the
        # listing by limiting the number of irrelevant snapshots returned. Note
        # that TDR does a substring match, not a prefix match, but determining
        # the longest common substring is complicated and, as of yet, I haven't
        # found a trustworthy, reusable implementation.
        filter = longest_common_prefix(configured_specs_by_name.keys())
        snapshots = self._auth_fallback(authentication,
                                        lambda tdr: tdr.snapshot_names_by_id(filter=filter))
        snapshot_ids_by_name = {
            name: id
            for id, name in snapshots.items()
            if name in configured_specs_by_name
        }
        return [
            TDRSourceRef(id=id,
                         spec=configured_specs_by_name[name])
            for name, id in snapshot_ids_by_name.items()
        ]

    def list_source_ids(self,
                        authentication: Optional[Authentication]
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
                                authentication: Optional[Authentication]
                                ) -> TDRClient:
        if authentication is None:
            tdr = TDRClient.for_anonymous_user()
        elif isinstance(authentication, OAuth2):
            tdr = TDRClient.for_registered_user(authentication)
        else:
            raise PermissionError('Unsupported authentication format',
                                  type(authentication))
        return tdr

    @classmethod
    @cache_per_thread
    def _drs_client(cls,
                    authentication: Optional[Authentication] = None
                    ) -> DRSClient:
        return cls._user_authenticated_tdr(authentication).drs_client()

    def _lookup_source_id(self, spec: TDRSourceSpec) -> str:
        return self.tdr.lookup_source(spec).id

    def list_bundles(self,
                     source: TDRSourceRef,
                     prefix: str
                     ) -> list[TDRBundleFQID]:
        self._assert_source(source)
        log.info('Listing bundles with prefix %r in source %r.', prefix, source)
        bundle_fqids = self._list_bundles(source, prefix)
        log.info('There are %i bundle(s) with prefix %r in source %r.',
                 len(bundle_fqids), prefix, source)
        return bundle_fqids

    def fetch_bundle(self, bundle_fqid: TDRBundleFQID) -> TDRBundle:
        self._assert_source(bundle_fqid.source)
        now = time.time()
        bundle = self._emulate_bundle(bundle_fqid)
        log.info('It took %.003fs to download bundle %s.%s',
                 time.time() - now, bundle.uuid, bundle.version)
        return bundle

    def portal_db(self) -> Sequence[JSON]:
        return []

    @classmethod
    def format_version(cls, version: datetime.datetime) -> str:
        return format_dcp2_datetime(version)

    def _run_sql(self, query):
        return self.tdr.run_sql(query)

    def _full_table_name(self, source: TDRSourceSpec, table_name: str) -> str:
        return source.qualify_table(table_name)

    @abstractmethod
    def _list_bundles(self,
                      source: TDRSourceRef,
                      prefix: str
                      ) -> list[TDRBundleFQID]:
        raise NotImplementedError

    @abstractmethod
    def _emulate_bundle(self, bundle_fqid: TDRBundleFQID) -> TDRBundle:
        raise NotImplementedError

    def drs_client(self,
                   authentication: Optional[Authentication] = None
                   ) -> DRSClient:
        return self._drs_client(authentication)

    def file_download_class(self) -> Type[RepositoryFileDownload]:
        return TDRFileDownload

    def validate_version(self, version: str) -> None:
        parse_dcp2_version(version)


class TDRFileDownload(RepositoryFileDownload):
    _location: Optional[str] = None

    needs_drs_uri = True

    def update(self,
               plugin: RepositoryPlugin,
               authentication: Optional[Authentication]
               ) -> None:
        require(self.replica is None or self.replica == 'gcp')
        if self.drs_uri is None:
            assert self.location is None, self
            assert self.retry_after is None, self
        else:
            drs_client = plugin.drs_client(authentication)
            access = drs_client.get_object(self.drs_uri, access_method=AccessMethod.gs)
            require(access.method is AccessMethod.https, access.method)
            require(access.headers is None, access.headers)
            signed_url = access.url
            args = furl(signed_url).args
            require('X-Goog-Signature' in args, args)
            self._location = signed_url

    @property
    def location(self) -> Optional[str]:
        return self._location

    @property
    def retry_after(self) -> Optional[int]:
        return None
