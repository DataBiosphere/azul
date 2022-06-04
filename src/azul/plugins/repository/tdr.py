from abc import (
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
    Optional,
    Type,
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
    Bundle,
    SourcedBundleFQID,
)
from azul.plugins import (
    RepositoryFileDownload,
    RepositoryPlugin,
)
from azul.terra import (
    SourceRef as TDRSourceRef,
    TDRClient,
    TDRSourceSpec,
)
from azul.time import (
    format_dcp2_datetime,
)
from azul.types import (
    JSON,
)

log = logging.getLogger(__name__)

TDRBundleFQID = SourcedBundleFQID[TDRSourceRef]


@attr.s(kw_only=True, auto_attribs=True, frozen=True)
class TDRPlugin(RepositoryPlugin[TDRSourceSpec, TDRSourceRef]):
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

    def _user_authenticated_tdr(self,
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

    def list_sources(self,
                     authentication: Optional[Authentication]
                     ) -> list[TDRSourceRef]:
        tdr = self._user_authenticated_tdr(authentication)
        try:
            snapshots = tdr.snapshot_names_by_id()
        except UnauthorizedError:
            if authentication is not None and tdr.token_is_valid():
                # Fall back to anonymous access if the user-provided credentials
                # are valid but lack authorization
                return self.list_sources(None)
            else:
                raise

        configured_specs_by_name = {spec.name: spec for spec in self.sources}
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

    @property
    def tdr(self):
        return self._tdr()

    # To utilize the caching of certain TDR responses that's occurring within
    # the client instance we need to cache client instances. If we cached the
    # client instance within the plugin instance, we would get one client
    # instance per plugin instance. The plugin is instantiated frequently and in
    # a variety of contexts.
    #
    # Because of that, caching the plugin instances would be a more invasive
    # change than simply caching the client instance per plugin class. That's
    # why this is a class method. The client uses urllib3, whose thread-safety
    # is disputed (https://github.com/urllib3/urllib3/issues/1252), so have to
    # cache client instances per-class AND per-thread.

    @classmethod
    @cache_per_thread
    def _tdr(cls):
        return TDRClient.for_indexer()

    def verify_source(self, ref: TDRSourceRef) -> None:
        return self.tdr.verify_source(ref)

    def lookup_source_id(self, spec: TDRSourceSpec) -> str:
        return self.tdr.lookup_source(spec).id

    def list_bundles(self, source: TDRSourceRef, prefix: str) -> list[TDRBundleFQID]:
        self._assert_source(source)
        log.info('Listing bundles with prefix %r in source %r.', prefix, source)
        bundle_fqids = self._list_bundles(source, prefix)
        log.info('There are %i bundle(s) with prefix %r in source %r.',
                 len(bundle_fqids), prefix, source)
        return bundle_fqids

    def fetch_bundle(self, bundle_fqid: TDRBundleFQID) -> Bundle:
        self._assert_source(bundle_fqid.source)
        now = time.time()
        bundle = self._emulate_bundle(bundle_fqid)
        log.info("It took %.003fs to download bundle %s.%s",
                 time.time() - now, bundle.uuid, bundle.version)
        return bundle

    def portal_db(self) -> Sequence[JSON]:
        return []

    def drs_uri(self, drs_path: Optional[str]) -> Optional[str]:
        if drs_path is None:
            return None
        else:
            netloc = config.tdr_service_url.netloc
            return f'drs://{netloc}/{drs_path}'

    @classmethod
    def format_version(cls, version: datetime.datetime) -> str:
        return format_dcp2_datetime(version)

    def _run_sql(self, query):
        return self.tdr.run_sql(query)

    def _full_table_name(self, source: TDRSourceSpec, table_name: str) -> str:
        return source.qualify_table(table_name)

    @abstractmethod
    def _list_bundles(self, source: TDRSourceRef, prefix: str) -> list[TDRBundleFQID]:
        raise NotImplementedError

    @abstractmethod
    def _emulate_bundle(self, bundle_fqid: SourcedBundleFQID) -> Bundle:
        raise NotImplementedError

    def drs_client(self,
                   authentication: Optional[Authentication] = None
                   ) -> DRSClient:
        return self._user_authenticated_tdr(authentication).drs_client()

    def file_download_class(self) -> Type[RepositoryFileDownload]:
        return TDRFileDownload


class TDRFileDownload(RepositoryFileDownload):
    _location: Optional[str] = None

    needs_drs_path = True

    def update(self,
               plugin: RepositoryPlugin,
               authentication: Optional[Authentication]
               ) -> None:
        require(self.replica is None or self.replica == 'gcp')
        drs_uri = plugin.drs_uri(self.drs_path)
        if drs_uri is None:
            assert self.location is None, self
            assert self.retry_after is None, self
        else:
            drs_client = plugin.drs_client(authentication)
            access = drs_client.get_object(drs_uri, access_method=AccessMethod.gs)
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
