import json
import logging
import time
from typing import (
    AbstractSet,
    Iterable,
    List,
    Optional,
    Type,
)
import uuid

import attr

from azul import (
    CatalogName,
    JSON,
    cache_per_thread,
    config,
)
from azul.auth import (
    Authentication,
    OAuth2,
)
from azul.indexer import (
    Bundle,
    Checksums,
    SourceRef,
    SourcedBundleFQID,
)
from azul.plugins import (
    RepositoryFileDownload,
    RepositoryPlugin,
)
from azul.terra.workspace import (
    TerraSourceSpec,
    Workspace,
    WorkspaceClient,
)
from azul.types import (
    JSONs,
)
from azul.uuids import (
    validate_uuid_prefix,
)

log = logging.getLogger(__name__)


class TerraSourceRef(SourceRef[TerraSourceSpec, 'TerraSourceRef']):
    pass


TerraBundleFQID = SourcedBundleFQID[TerraSourceRef]


class TerraBundle(Bundle):

    def drs_path(self, manifest_entry: JSON) -> Optional[str]:
        assert manifest_entry.get('drs_path') is None, manifest_entry
        return None

    def add_project(self, workspace: Workspace) -> None:
        document_name = 'project_0.json'
        assert document_name not in self.metadata_files, self.metadata_files.keys()
        project_id = self._synthesize_entity_uuid(workspace, 'project')
        metadata = {
            **self._schema_info('project'),
            'project_core': {
                'project_shortname': workspace.name,
                'project_title': workspace.name,
                'project_description': workspace.attributes.get('description')
            },
            'provenance': {
                'document_id': project_id,
                'submission_date': workspace.created_date,
                'update_date': workspace.update_date
            }
        }
        self.metadata_files[document_name] = metadata
        self._add_manifest_entry(document_name, project_id, workspace.update_date, metadata)

    def add_links(self, workspace: Workspace) -> None:
        document_name = 'links.json'
        links_id = self._synthesize_entity_uuid(workspace, 'links')
        assert document_name not in self.metadata_files, self.metadata_files.keys()
        metadata = {
            **self._schema_info('links'),
            'links': []
        }
        self.metadata_files[document_name] = metadata
        self._add_manifest_entry(document_name, links_id, workspace.update_date, metadata)

    def _add_manifest_entry(self,
                            document_name: str,
                            entity_id: str,
                            entity_version: str,
                            metadata: JSON,
                            ) -> None:
        self.manifest.append({
            'uuid': entity_id,
            'version': entity_version,
            'size': len(json.dumps(metadata)),
            'name': document_name,
            'content-type': 'application/json',
            'indexed': True,
            **Checksums.empty().to_json()
        })

    def _schema_info(self, entity_type: str) -> JSON:
        # Minimum structure necessary for the HCA metadata API to accept the
        # bundle. Saves us the trouble of writing a new metadata plugin.
        return {
            'schema_type': entity_type,
            'schema_version': '1.1.1',
            'describedBy': f'//{entity_type}'
        }

    def _synthesize_entity_uuid(self,
                                workspace: Workspace,
                                entity_type: str
                                ) -> str:
        # FIXME: Wranglers to provide project UUID as workspace attribute
        #        https://github.com/DataBiosphere/azul/issues/3826
        namespace = uuid.UUID('cc5af9be-2e07-4e19-92cb-3dc844624697')
        return str(uuid.uuid5(namespace, f'{entity_type}/{workspace.qualname}'))


@attr.s(kw_only=True, auto_attribs=True, frozen=True)
class Plugin(RepositoryPlugin[TerraSourceSpec, TerraSourceRef]):
    _sources: AbstractSet[TerraSourceSpec]

    @classmethod
    def create(cls, catalog: CatalogName) -> 'RepositoryPlugin':
        return cls(sources=frozenset(
            TerraSourceSpec.parse(spec).effective
            for spec in config.sources(catalog)
        ))

    @property
    def sources(self) -> AbstractSet[TerraSourceSpec]:
        return self._sources

    @property
    def client(self) -> WorkspaceClient:
        return self._client()

    # See :meth:`azul.plugins.repository.tdr.Plugin._tdr` for why we cache
    # per-thread
    @classmethod
    @cache_per_thread
    def _client(cls) -> WorkspaceClient:
        return WorkspaceClient.with_service_account_credentials()

    def _user_authenticated_client(self,
                                   authentication: Optional[Authentication]
                                   ) -> WorkspaceClient:
        if authentication is None:
            tdr = WorkspaceClient.with_public_service_account_credentials()
        elif isinstance(authentication, OAuth2):
            tdr = WorkspaceClient.with_user_credentials(authentication)
        else:
            raise PermissionError('Unsupported authentication format',
                                  type(authentication))
        return tdr

    def list_sources(self,
                     authentication: Optional[Authentication]
                     ) -> Iterable[TerraSourceRef]:
        client = self._user_authenticated_client(authentication)
        specs_by_qualname = {
            spec.qualname: spec
            for spec in self.sources
        }
        workspaces_by_qualname = {
            workspace.qualname: workspace
            for workspace in client.list_workspaces()
            if workspace.qualname in specs_by_qualname
        }
        return [
            TerraSourceRef(spec=specs_by_qualname[qualname],
                           id=workspace.id)
            for qualname, workspace in workspaces_by_qualname.items()
        ]

    def lookup_source_id(self, spec: TerraSourceSpec) -> str:
        return self.client.get_workspace(spec).id

    def list_bundles(self,
                     source: TerraSourceRef,
                     prefix: str
                     ) -> List[TerraBundleFQID]:
        self._assert_source(source)
        log.info('Listing bundles with prefix %r in source %r.', prefix, source)
        prefix = source.spec.prefix.common + prefix
        validate_uuid_prefix(prefix)
        if source.id.startswith(prefix):
            bundle_fqids = [self._workspace_fqid(source)]
        else:
            log.warning('Prefix %s does not include source %r', prefix, source)
            bundle_fqids = []
        log.info('There are %i bundle(s) with prefix %r in source %r.',
                 len(bundle_fqids), prefix, source)
        return bundle_fqids

    def fetch_bundle(self, bundle_fqid: TerraBundleFQID) -> TerraBundle:
        self._assert_source(bundle_fqid.source)
        now = time.time()
        bundle = self._emulate_bundle(bundle_fqid)
        log.info("It took %.003fs to download bundle %s.%s",
                 time.time() - now, bundle.uuid, bundle.version)
        return bundle

    def file_download_class(self) -> Type['RepositoryFileDownload']:
        raise NotImplementedError

    def direct_file_url(self,
                        file_uuid: str,
                        *,
                        file_version: Optional[str] = None,
                        replica: Optional[str] = None
                        ) -> Optional[str]:
        raise NotImplementedError

    def drs_uri(self, drs_path: str) -> str:
        raise NotImplementedError

    def dss_deletion_subscription_query(self, prefix: str) -> JSON:
        return {}

    def dss_subscription_query(self, prefix: str) -> JSON:
        return {}

    def portal_db(self) -> JSONs:
        return []

    def _workspace_fqid(self,
                        source: TerraSourceRef,
                        ) -> TerraBundleFQID:
        workspace = self.client.get_workspace(source.spec)
        assert source.id == workspace.id, (source, workspace)
        return TerraBundleFQID(source=source,
                               uuid=workspace.id,
                               version=workspace.update_date)

    def _emulate_bundle(self, bundle_fqid: TerraBundleFQID) -> TerraBundle:
        bundle = TerraBundle(fqid=bundle_fqid,
                             manifest=[],
                             metadata_files={})
        workspace = self.client.get_workspace(bundle_fqid.source.spec)
        bundle.add_project(workspace)
        bundle.add_links(workspace)
        return bundle
