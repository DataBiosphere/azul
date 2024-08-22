from collections import (
    defaultdict,
)
import json
import logging
from pathlib import (
    Path,
)
from typing import (
    ClassVar,
    Mapping,
    Self,
    Sequence,
    TypeVar,
)
from uuid import (
    UUID,
    uuid5,
)

import attr
from furl import (
    furl,
)

import git

from azul import (
    reject,
    require,
)
from azul.indexer.document import (
    EntityReference,
)
from azul.types import (
    JSON,
    MutableJSON,
)
from humancellatlas.data.metadata.api import (
    Bundle,
)
from humancellatlas.data.metadata.helpers.schema_validation import (
    SchemaValidator,
)

log = logging.getLogger(__name__)


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class JsonFile:
    """
    A JSON file in the staging area.
    """
    uuid: str
    version: str
    name: str
    content: MutableJSON
    _validator: ClassVar[SchemaValidator] = SchemaValidator()

    def __attrs_post_init__(self):
        self._validator.validate_json(self.content, self.name)

    @classmethod
    def from_json(cls, file_name: str, content: MutableJSON) -> 'JsonFile':
        def parse_file_name(file_name: str) -> Sequence[str]:
            suffix = '.json'
            assert file_name.endswith(suffix), file_name
            return file_name[:-len(suffix)].split('_')

        schema_type = content['schema_type']
        if schema_type == 'links':
            subgraph_id, version, project_id = parse_file_name(file_name)
            return LinksFile(uuid=subgraph_id,
                             version=version,
                             name=file_name,
                             content=content,
                             project_id=project_id)
        else:
            entity_id, version = parse_file_name(file_name)
            if schema_type == 'file_descriptor':
                return DescriptorFile(uuid=entity_id,
                                      version=version,
                                      name=file_name,
                                      content=content)
            else:  # 'biomaterial', 'protocol', 'file', ...
                return MetadataFile(uuid=entity_id,
                                    version=version,
                                    name=file_name,
                                    content=content)


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class LinksFile(JsonFile):
    """
    A file describing the links between entities in a subgraph.
    """
    project_id: str


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class MetadataFile(JsonFile):
    """
    A file describing one entity (e.g. biomaterial, protocol) in a subgraph.
    """
    pass


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class DescriptorFile(JsonFile):
    """
    A file containing the checksums and other information for asserting the
    integrity of a data file.
    """
    namespace: ClassVar[UUID] = UUID('5767014a-c431-4019-8703-0ab1b3e9e4d0')

    @property
    def manifest_entry(self):
        """
        The content of a descriptor transformed into a format ready to create a
        ManifestEntry object.
        """
        return {
            'content-type': self.content['content_type'],
            'crc32c': self.content['crc32c'],
            'indexed': False,
            'name': self.content['file_name'],
            's3_etag': self.content['s3_etag'],
            'sha1': self.content['sha1'],
            'sha256': self.content['sha256'],
            'size': self.content['size'],
            'uuid': str(uuid5(self.namespace, self.content['file_name'])),
            'version': self.content['file_version']
        }


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class StagingArea:
    links: Mapping[str, LinksFile]  # Key is the subgraph ID aka links_id
    metadata: Mapping[str, MetadataFile]  # Key is the entity ID
    descriptors: Mapping[str, DescriptorFile]  # Key is the entity ID

    def get_bundle(self, subgraph_id: str) -> Bundle:
        """
        Return a bundle from the staging area
        """
        version, manifest, metadata, links = self.get_bundle_parts(subgraph_id)
        return Bundle(subgraph_id, version, manifest, metadata, links)

    def get_bundle_parts(self,
                         subgraph_id: str
                         ) -> tuple[str, MutableJSON, MutableJSON, MutableJSON]:
        """
        Return the components to create a bundle from the staging area
        """
        links_file = self.links[subgraph_id]
        manifest = {}
        metadata = {}
        entity_ids_by_type = self._entity_ids_by_type(subgraph_id)
        for entity_type, entity_ids in entity_ids_by_type.items():
            # Sort entity_ids to produce the same ordering on multiple runs
            for entity_id in sorted(entity_ids):
                metadata_file = self.metadata[entity_id]
                json_content = metadata_file.content
                key = str(EntityReference(entity_type=entity_type, entity_id=entity_id))
                metadata[key] = json_content
                if entity_type.endswith('_file'):
                    file_manifest = self.descriptors[entity_id].manifest_entry
                    manifest[key] = file_manifest
                else:
                    pass
        return links_file.version, manifest, metadata, links_file.content

    def _entity_ids_by_type(self,
                            subgraph_id: str
                            ) -> dict[str, set[str]]:
        """
        Return a mapping of entity types (e.g. 'analysis_file',
        'cell_suspension') to a set of entity IDs
        """
        links_file: LinksFile = self.links[subgraph_id]
        links_json = links_file.content
        entity_ids = defaultdict(set)
        # Project ID is only mentioned in the links JSON if there is a
        # supplementary_file_link so add it in here to make sure it is included.
        entity_ids['project'].add(links_file.project_id)
        link: JSON
        for link in links_json['links']:
            link_type = link['link_type']
            if link_type == 'process_link':
                entity_type = link['process_type']
                entity_id = link['process_id']
                entity_ids[entity_type].add(entity_id)
                for category in ('input', 'output', 'protocol'):
                    for file in link[f'{category}s']:
                        entity_type = file[f'{category}_type']
                        entity_id = file[f'{category}_id']
                        entity_ids[entity_type].add(entity_id)
            elif link_type == 'supplementary_file_link':
                for file in link['files']:
                    entity_type = file['file_type']
                    entity_id = file['file_id']
                    entity_ids[entity_type].add(entity_id)
            else:
                raise ValueError('Unknown link type', link_type)
        entity_ids.default_factory = None
        return entity_ids


JSON_FILE = TypeVar('JSON_FILE', bound=JsonFile)


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class CannedStagingAreaFactory:
    #: Path to a local directory containing one or more staging areas
    base_path: Path

    @classmethod
    def clone_remote(cls, remote_url: furl, local_path: Path, ref: str) -> Self:
        """
        Clone a remote Git repository and return a factory for staging areas
        inside that clone.

        :param remote_url: The URL of a remote Git repository containing one or
                           more staging areas

        :param local_path: The path to an empty local directory where the
                           repository will be cloned

        :param ref: A Git ref (branch, tag, or commit SHA)
        """
        log.debug('Cloning %s into %s', remote_url, local_path)
        repo = git.Repo.clone_from(str(remote_url), local_path)
        log.debug('Checking out ref %s', ref)
        repo.git.checkout(ref)
        return cls(base_path=local_path)

    def load_staging_area(self, path: Path) -> StagingArea:
        """
        Create and return a staging area object from the files in a local
        staging area.

        :param path: The relative path from `self.base_path` to a local staging
                     area
        """
        path = self.base_path / path
        staging_area_folders = {p.name for p in path.iterdir()}
        expected_folders = {'data', 'descriptors', 'links', 'metadata'}
        require(expected_folders == staging_area_folders,
                'Invalid staging area', path)
        return StagingArea(links=self._get_link_files(path),
                           metadata=self._get_metadata_files(path),
                           descriptors=self._get_descriptor_files(path))

    def _get_link_files(self, path: Path) -> dict[str, LinksFile]:
        """
        Return a mapping of file ID to file content for all the link files in
        the staging area.
        """
        return self._get_files(path=path / 'links', file_cls=LinksFile)

    def _get_metadata_files(self, path: Path) -> dict[str, MetadataFile]:
        """
        Return a mapping of file ID to file content for all the metadata files
        in the staging area.
        """
        files = {}
        for sub_dir in (path / 'metadata').iterdir():
            assert sub_dir.is_dir()
            files.update(self._get_files(path=sub_dir, file_cls=MetadataFile))
        return files

    def _get_descriptor_files(self, path: Path) -> dict[str, DescriptorFile]:
        """
        Return a mapping of file ID to file content for all the descriptor files
        in the staging area.
        """
        files = {}
        for sub_dir in (path / 'descriptors').iterdir():
            assert sub_dir.is_dir()
            files.update(self._get_files(path=sub_dir, file_cls=DescriptorFile))
        return files

    def _get_files(self,
                   path: Path,
                   file_cls: type[JSON_FILE]
                   ) -> dict[str, JSON_FILE]:
        """
        Return a mapping of file ID to file content for all the files found in
        the directory at the given path.
        """
        files = {}
        log.debug('Reading files in %s', path)
        for file in path.iterdir():
            assert file.is_file()
            with open(file, 'r') as f:
                content = json.load(f)
            file_name = file.name
            json_file = JsonFile.from_json(file_name, content)
            require(isinstance(json_file, file_cls), json_file)
            self._add_file(files, json_file)
        return files

    def _add_file(self, files: dict[str, JSON_FILE], file: JSON_FILE) -> None:
        """
        Add `file` to `files`. If a file with the same ID already exists in
        `files`, the file with the most recent version will be kept.
        """
        try:
            existing_version = files[file.uuid].version
        except KeyError:
            files[file.uuid] = file
        else:
            reject(file.version == existing_version, file)
            if file.version > existing_version:
                files[file.uuid] = file
            else:
                log.debug('Discarding previous %s version of file %s',
                          existing_version, file)
