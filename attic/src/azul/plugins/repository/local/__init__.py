import json
from pathlib import (
    Path,
)
from typing import (
    AbstractSet,
    Iterable,
    List,
    Optional,
    Type,
)

import attr

from azul import (
    CatalogName,
    JSON,
    config,
)
from azul.auth import (
    Authentication,
)
from azul.indexer import (
    Bundle,
    SOURCE_REF,
    SimpleSourceSpec,
    SourceRef,
    SourcedBundleFQID,
)
from azul.plugins import (
    RepositoryFileDownload,
    RepositoryPlugin,
)
from azul.types import (
    JSONs,
)
from azul.uuids import (
    validate_uuid_prefix,
)


class LocalSourceRef(SourceRef[SimpleSourceSpec, 'LocalSourceRef']):
    pass


class LocalBundle(Bundle):

    def drs_path(self, manifest_entry: JSON) -> Optional[str]:
        return None


@attr.s(kw_only=True, auto_attribs=True, frozen=True)
class Plugin(RepositoryPlugin[SimpleSourceSpec, LocalSourceRef]):
    """
    This plugin can be used to index bundles saved to the developer's local
    machine during the integration test. This can be useful if those bundles
    are important for testing cases that do not occur in any bundles accessible
    via any remote repository. Since the resulting catalog is not backed by an
    actual data repository, much functionality remains unsupported/unimplemented.

    The canned bundles must be placed in a directory that will be packaged
    when the lambdas are deployed, e.g. `src/azul/`. The source spec syntax is
    a path relative to the project root pointing to a directory containing the
    canned bundles. An example catalog configuration using this plugin is:

    'AZUL_CATALOGS': json.dumps({
        ...
        'it0largebundles': dict(atlas='hca',
                                internal=True,
                                plugins=dict(metadata=dict(name='hca'),
                                             repository=dict(name='local')),
                                sources=[
                                    'data/:'
                                ]),
        ...
    }),
    """
    _sources: AbstractSet[SimpleSourceSpec]

    def lookup_source_id(self, spec: SimpleSourceSpec) -> str:
        return spec.name

    @classmethod
    def create(cls, catalog: CatalogName) -> 'RepositoryPlugin':
        assert config.catalogs[catalog].is_integration_test_catalog, catalog
        return cls(sources=frozenset(
            SimpleSourceSpec.parse(name).effective
            for name in config.sources(catalog))
        )

    @property
    def sources(self) -> AbstractSet[SimpleSourceSpec]:
        return self._sources

    def list_sources(self,
                     authentication: Optional[Authentication]
                     ) -> Iterable[LocalSourceRef]:
        return [
            LocalSourceRef(id=self.lookup_source_id(spec), spec=spec)
            for spec in self._sources
        ]

    _manifest_ext = '.manifest.json'
    _metadata_ext = '.metadata.json'

    @property
    def local_path(self) -> Path:
        return Path(__file__).parent

    def list_bundles(self,
                     source: LocalSourceRef,
                     prefix: str
                     ) -> List[SourcedBundleFQID[LocalSourceRef]]:
        source_prefix = source.spec.prefix.common
        validate_uuid_prefix(source_prefix + prefix)
        directory = self.local_path / source.spec.name
        files = directory.glob(f'{source_prefix}{prefix}*{self._manifest_ext}')
        bundle_fqids = []
        for file in files:
            fqid, _, suffix = file.name.rpartition(self._manifest_ext)
            assert suffix == ''
            uuid, version = fqid.split('.', 1)
            bundle_fqids.append(SourcedBundleFQID(uuid=uuid,
                                                  version=version,
                                                  source=source))
        return bundle_fqids

    def fetch_bundle(self, bundle_fqid: SourcedBundleFQID[SOURCE_REF]) -> Bundle:
        basename = f'{bundle_fqid.uuid}.{bundle_fqid.version}'
        path = str(self.local_path / bundle_fqid.source.spec.name / basename)
        with open(path + self._manifest_ext) as f:
            manifest = json.load(f)
        with open(path + self._metadata_ext) as f:
            metadata = json.load(f)
        return LocalBundle(fqid=bundle_fqid, manifest=manifest, metadata_files=metadata)

    def portal_db(self) -> JSONs:
        return []

    def dss_deletion_subscription_query(self, prefix: str) -> JSON:
        return {}

    def dss_subscription_query(self, prefix: str) -> JSON:
        return {}

    def file_download_class(self) -> Type['RepositoryFileDownload']:
        raise NotImplementedError

    def direct_file_url(self,
                        file_uuid:
                        str,
                        *,
                        file_version: Optional[str] = None,
                        replica: Optional[str] = None) -> Optional[str]:
        raise NotImplementedError

    def drs_uri(self, drs_path: str) -> str:
        raise NotImplementedError
