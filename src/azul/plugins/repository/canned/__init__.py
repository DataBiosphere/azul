"""
This repository plugin allows reading from a canned staging area like the one in
the GitHub repo https://github.com/HumanCellAtlas/schema-test-data .

NOTE: Use of this plugin requires a GitHub personal access token specified in an
environment variable `GITHUB_TOKEN`. See:
https://github.com/DataBiosphere/hca-metadata-api#github-credentials

Due to this requirement, this plugin cannot be used to index data directly from
the canned staging area, however it can be used with the `can_bundle.py` script
to create a local canned bundle from files in the canned staging area.
"""
from collections.abc import (
    Sequence,
    Set,
)
from dataclasses import (
    dataclass,
)
import logging
import time
from typing import (
    Optional,
    Type,
    cast,
)

from furl import (
    furl,
)

from azul import (
    CatalogName,
    config,
    lru_cache,
    require,
)
from azul.auth import (
    Authentication,
)
from azul.drs import (
    DRSClient,
)
from azul.http import (
    HasCachedHttpClient,
)
from azul.indexer import (
    SimpleSourceSpec,
    SourceRef,
    SourcedBundleFQID,
)
from azul.plugins import (
    RepositoryFileDownload,
    RepositoryPlugin,
)
from azul.plugins.metadata.hca.bundle import (
    HCABundle,
)
from azul.time import (
    parse_dcp2_version,
)
from azul.types import (
    JSON,
    MutableJSON,
    MutableJSONs,
)
from azul.uuids import (
    validate_uuid_prefix,
)
from humancellatlas.data.metadata.helpers.staging_area import (
    GitHubStagingAreaFactory,
    StagingArea,
)

log = logging.getLogger(__name__)


class CannedSourceRef(SourceRef[SimpleSourceSpec, 'CannedSourceRef']):
    pass


class CannedBundleFQID(SourcedBundleFQID[CannedSourceRef]):
    pass


class CannedBundle(HCABundle[CannedBundleFQID]):

    @classmethod
    def canning_qualifier(cls) -> str:
        return 'gh.hca'

    def drs_uri(self, manifest_entry: JSON) -> Optional[str]:
        return 'dss'


@dataclass(frozen=True)
class Plugin(RepositoryPlugin[CannedBundle, SimpleSourceSpec, CannedSourceRef, CannedBundleFQID],
             HasCachedHttpClient):
    _sources: Set[SimpleSourceSpec]

    @classmethod
    def create(cls, catalog: CatalogName) -> RepositoryPlugin:
        return cls(
            frozenset(
                SimpleSourceSpec.parse(name)
                for name in config.sources(catalog)
            )
        )

    @property
    def sources(self) -> Set[SimpleSourceSpec]:
        return self._sources

    def list_sources(self,
                     authentication: Optional[Authentication]
                     ) -> list[CannedSourceRef]:
        return [
            CannedSourceRef(id=self._lookup_source_id(spec), spec=spec)
            for spec in self._sources
        ]

    def _lookup_source_id(self, spec: SimpleSourceSpec) -> str:
        return spec.name

    @lru_cache
    def staging_area(self, source_spec: SimpleSourceSpec) -> StagingArea:
        factory = GitHubStagingAreaFactory.from_url(source_spec.name)
        return factory.load_staging_area()

    def _assert_source(self, source: CannedSourceRef):
        assert source.spec in self.sources, (source, self.sources)

    def list_bundles(self,
                     source: CannedSourceRef,
                     prefix: str
                     ) -> list[CannedBundleFQID]:
        self._assert_source(source)
        prefix = source.spec.prefix.common + prefix
        validate_uuid_prefix(prefix)
        log.info('Listing bundles with prefix %r in source %r.', prefix, source)
        bundle_fqids = []
        for link in self.staging_area(source.spec).links.values():
            if link.uuid.startswith(prefix):
                bundle_fqids.append(CannedBundleFQID(source=source,
                                                     uuid=link.uuid,
                                                     version=link.version))
        log.info('There are %i bundle(s) with prefix %r in source %r.',
                 len(bundle_fqids), prefix, source)
        return bundle_fqids

    def fetch_bundle(self, bundle_fqid: CannedBundleFQID) -> CannedBundle:
        self._assert_source(bundle_fqid.source)
        now = time.time()
        staging_area = self.staging_area(bundle_fqid.source.spec)
        version, manifest, metadata = staging_area.get_bundle(bundle_fqid.uuid)
        if bundle_fqid.version is None:
            bundle_fqid = CannedBundleFQID(source=bundle_fqid.source,
                                           uuid=bundle_fqid.uuid,
                                           version=version)
        bundle = CannedBundle(fqid=bundle_fqid,
                              manifest=cast(MutableJSONs, manifest),
                              metadata_files=cast(MutableJSON, metadata))
        assert version == bundle.version, (version, bundle)
        log.info('It took %.003fs to download bundle %s.%s',
                 time.time() - now, bundle.uuid, bundle.version)
        return bundle

    def portal_db(self) -> Sequence[JSON]:
        return []

    def _construct_file_url(self, source_url: str, file_name: str) -> str:
        """
        >>> plugin = Plugin(_sources=set())
        >>> source_url = 'https://github.com/USER/REPO/tree/REF/tests'

        >>> plugin._construct_file_url(source_url, 'foo.zip')
        'https://github.com/USER/REPO/raw/REF/tests/foo.zip'

        >>> plugin._construct_file_url(source_url, '')
        Traceback (most recent call last):
        ...
        azul.RequirementError: file_name cannot be empty
        """
        url = furl(source_url)
        require(url.path.segments[2] == 'tree', source_url)
        url.path.segments[2] = 'raw'
        require(len(file_name) > 0, 'file_name cannot be empty')
        require(not file_name.endswith('/'), file_name)
        for segment in file_name.split('/'):
            url.path.segments.append(segment)
        return str(url)

    def _direct_file_url(self,
                         file_uuid: str,
                         *,
                         file_version: Optional[str] = None,
                         ) -> Optional[str]:
        # Check all sources for the file. If a file_version was specified return
        # when we find a match, otherwise continue checking all sources and
        # return the URL for the match with the latest (largest) version.
        found_version = None
        found_url = None
        for source_spec in self.sources:
            staging_area = self.staging_area(source_spec)
            try:
                descriptor = staging_area.descriptors[file_uuid]
            except KeyError:
                continue
            else:
                actual_file_version = descriptor.content['file_version']
                if file_version:
                    if file_version == actual_file_version:
                        file_name = descriptor.content['file_name']
                        return self._construct_file_url(source_spec.name, file_name)
                else:
                    if found_version is None or actual_file_version > found_version:
                        file_name = descriptor.content['file_name']
                        found_url = self._construct_file_url(source_spec.name, file_name)
                        found_version = actual_file_version
        return found_url

    def file_download_class(self) -> Type[RepositoryFileDownload]:
        return CannedFileDownload

    def drs_client(self,
                   authentication: Optional[Authentication] = None
                   ) -> DRSClient:
        assert authentication is None, type(authentication)
        return DRSClient(http_client=self._http_client)

    def validate_version(self, version: str) -> None:
        parse_dcp2_version(version)


class CannedFileDownload(RepositoryFileDownload):
    _location: Optional[str] = None
    _retry_after: Optional[int] = None

    def update(self,
               plugin: RepositoryPlugin,
               authentication: Optional[Authentication]
               ) -> None:
        assert isinstance(plugin, Plugin)
        url = plugin._direct_file_url(file_uuid=self.file_uuid,
                                      file_version=self.file_version)
        self._location = url

    @property
    def location(self) -> Optional[str]:
        return self._location

    @property
    def retry_after(self) -> Optional[int]:
        return self._retry_after
