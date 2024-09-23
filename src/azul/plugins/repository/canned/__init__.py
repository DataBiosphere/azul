"""
This repository plugin allows reading from a canned staging area like the one in
the GitHub repo https://github.com/HumanCellAtlas/schema-test-data .

NOTE: This plugin's purpose is for testing and verification of a canned staging
area, and should not be used to create catalogs on a deployment. It can however
be used with the `can_bundle.py` script to create a local canned bundle from the
files in the canned staging area.
"""
from dataclasses import (
    dataclass,
)
import logging
from pathlib import (
    Path,
)
from tempfile import (
    TemporaryDirectory,
)
import time
from typing import (
    AbstractSet,
    Optional,
    Sequence,
    Type,
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
)
from azul.uuids import (
    validate_uuid_prefix,
)
from humancellatlas.data.metadata.helpers.staging_area import (
    CannedStagingAreaFactory,
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
    _sources: AbstractSet[SimpleSourceSpec]

    @classmethod
    def create(cls, catalog: CatalogName) -> RepositoryPlugin:
        return cls(
            frozenset(
                SimpleSourceSpec.parse(name)
                for name in config.sources(catalog)
            )
        )

    @property
    def sources(self) -> AbstractSet[SimpleSourceSpec]:
        return self._sources

    def list_sources(self,
                     authentication: Optional[Authentication]
                     ) -> list[CannedSourceRef]:
        return [
            CannedSourceRef(id=self._lookup_source_id(spec), spec=spec)
            for spec in self._sources
        ]

    def _lookup_source_id(self, spec: SimpleSourceSpec) -> str:
        return str(spec)

    def parse_github_url(self, url: furl) -> tuple[furl, Path, str]:
        """
        Parse a GitHub URL.

        :param url: A GitHub URL of the format
                    https://github.com/<OWNER>/<NAME>/tree/<REF>[/<PATH>]. Note
                    that REF can be the name of a branch, the name of a tag, or
                    a commit SHA. If REF contains special characters like `/`,
                    '?` or `#` they must be URL-encoded. This is especially
                    noteworthy for `/` in branch names.

        :return: A tuple containing the URL of a GitHub repository, a relative
                 path inside that repository, and a Git ref.

        >>> plugin = Plugin(_sources=set())

        >>> plugin.parse_github_url(furl('https://github.com/OWNER/NAME/tree/REF/tests'))
        (furl('https://github.com/OWNER/NAME.git'), PosixPath('tests'), 'REF')
        """
        require(url.scheme == 'https', url)
        require(url.host == 'github.com', url)
        owner, name, slug, ref, *path = url.path.segments
        require(slug == 'tree', url)
        remote_url = furl(url.origin)
        remote_url.path.add((owner, f'{name}.git'))
        return remote_url, Path(*path), ref

    @lru_cache
    def staging_area(self, url: str) -> StagingArea:
        """
        Process the contents of a staging area.

        :param url: The URL of a staging area located in a GitHub repository.

        :return: A StagingArea object containing the contents of the staging
                 area's JSON files.
        """
        with TemporaryDirectory() as tmpdir:
            remote_url, path, ref = self.parse_github_url(furl(url))
            factory = CannedStagingAreaFactory.clone_remote(remote_url,
                                                            Path(tmpdir),
                                                            ref)
            return factory.load_staging_area(path)

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
        staging_area = self.staging_area(source.spec.name)
        for link in staging_area.links.values():
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
        staging_area = self.staging_area(bundle_fqid.source.spec.name)
        version, manifest, metadata, links = staging_area.get_bundle_parts(bundle_fqid.uuid)
        if bundle_fqid.version is None:
            bundle_fqid = CannedBundleFQID(source=bundle_fqid.source,
                                           uuid=bundle_fqid.uuid,
                                           version=version)
        bundle = CannedBundle(fqid=bundle_fqid,
                              manifest=manifest,
                              metadata=metadata,
                              links=links)
        assert version == bundle.version, (version, bundle)
        log.info('It took %.003fs to download bundle %s.%s',
                 time.time() - now, bundle.uuid, bundle.version)
        return bundle

    def portal_db(self) -> Sequence[JSON]:
        return []

    def _construct_file_url(self, url: furl, file_name: str) -> furl:
        """
        >>> plugin = Plugin(_sources=set())
        >>> url = furl('https://github.com/OWNER/REPO/tree/REF/tests')

        >>> plugin._construct_file_url(url, 'foo.zip')
        furl('https://github.com/OWNER/REPO/raw/REF/tests/data/foo.zip')

        >>> plugin._construct_file_url(url, '')
        Traceback (most recent call last):
        ...
        azul.RequirementError: file_name cannot be empty
        """
        require(url.path.segments[2] == 'tree', str(url))
        file_url = furl(url)
        file_url.path.segments[2] = 'raw'
        file_url.path.segments.append('data')
        require(len(file_name) > 0, 'file_name cannot be empty')
        require(not file_name.endswith('/'), file_name)
        for segment in file_name.split('/'):
            file_url.path.segments.append(segment)
        return file_url

    def _direct_file_url(self,
                         file_uuid: str,
                         *,
                         file_version: Optional[str] = None,
                         ) -> Optional[furl]:
        # Check all sources for the file. If a file_version was specified return
        # when we find a match, otherwise continue checking all sources and
        # return the URL for the match with the latest (largest) version.
        found_version = None
        found_url = None
        for source_spec in self.sources:
            staging_area = self.staging_area(source_spec.name)
            try:
                descriptor = staging_area.descriptors[file_uuid]
            except KeyError:
                continue
            else:
                staging_area_url = furl(source_spec.name)
                actual_file_version = descriptor.content['file_version']
                if file_version:
                    if file_version == actual_file_version:
                        file_name = descriptor.content['file_name']
                        return self._construct_file_url(staging_area_url, file_name)
                else:
                    if found_version is None or actual_file_version > found_version:
                        file_name = descriptor.content['file_name']
                        found_url = self._construct_file_url(staging_area_url, file_name)
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
    _location: Optional[furl] = None
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
        return None if self._location is None else str(self._location)

    @property
    def retry_after(self) -> Optional[int]:
        return self._retry_after
