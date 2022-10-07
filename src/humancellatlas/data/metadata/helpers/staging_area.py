from collections import (
    defaultdict,
)
import json
import logging
import os
from typing import (
    ClassVar,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)
from uuid import (
    UUID,
    uuid5,
)

import attr
from furl import (
    furl,
)
from github import (
    Github,
    UnknownObjectException,
)
from github.Repository import (
    Repository,
)

from humancellatlas.data.metadata.api import (
    JSON,
)
from humancellatlas.data.metadata.helpers.exception import (
    reject,
    require,
)
from humancellatlas.data.metadata.helpers.schema_validation import (
    SchemaValidator
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
    content: JSON
    _validator: ClassVar[SchemaValidator] = SchemaValidator()

    def __attrs_post_init__(self):
        self._validator.validate_json(self.content, self.name)

    @classmethod
    def from_json(cls, file_name: str, content: JSON) -> 'JsonFile':
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

    def get_bundle(self, subgraph_id: str) -> Tuple[str, List[JSON], JSON]:
        """
        Return a tuple consisting of the version of the downloaded bundle, a
        list of the manifest entries for all metadata files in the bundle, and a
        dictionary mapping the file name of each metadata file in the bundle to
        the JSON contents of that file.
        """
        log.debug('Composing bundle %s', subgraph_id)

        links_file = self.links[subgraph_id]
        manifest = []
        metadata = {
            'links.json': links_file.content
        }
        entity_ids_by_type = self._entity_ids_by_type(subgraph_id)
        for entity_type, entity_ids in entity_ids_by_type.items():
            # Sort entity_ids to produce the same ordering on multiple runs
            for i, entity_id in enumerate(sorted(entity_ids)):
                json_file_name = f'{entity_type}_{i}.json'
                metadata_file = self.metadata[entity_id]
                json_content = metadata_file.content
                metadata[json_file_name] = json_content
                file_manifest = {
                    'content-type': 'application/json;',
                    'crc32c': '0' * 8,
                    'indexed': True,
                    'name': json_file_name,
                    's3_etag': None,
                    'sha1': None,
                    'sha256': '0' * 64,
                    'size': len(json.dumps(json_content)),
                    'uuid': metadata_file.uuid,
                    'version': metadata_file.version
                }
                manifest.append(file_manifest)
                if entity_type.endswith('_file'):
                    file_manifest = self.descriptors[entity_id].manifest_entry
                    manifest.append(file_manifest)
                else:
                    pass
        log.debug('Composed bundle with %i metadata files', len(metadata))
        return links_file.version, manifest, metadata

    def _entity_ids_by_type(self,
                            subgraph_id: str
                            ) -> Mapping[str, Set[str]]:
        """
        Parse the links in a subgraph and return a mapping of the entity types
        (e.g. 'analysis_file', 'cell_suspension') to a set of entity IDs
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


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class GitHubStagingAreaFactory:
    repo: Repository
    ref: str
    path: Tuple[str, ...]

    @classmethod
    def create(cls,
               owner: str,
               name: str,
               ref: str,
               path: Optional[str] = None
               ) -> 'GitHubStagingAreaFactory':
        """
        :param owner: The owner of the GitHub repository
        :param name: The name of the GitHub repository
        :param ref: A branch name, tag, or commit SHA
        :param path: The path inside the repository to the base of the staging area
        """
        token = os.environ['GITHUB_TOKEN']  # A GitHub personal access token
        log.debug('Requesting GitHub repo %s', (owner, name, ref, path))
        github_api = Github(token)
        rate_limit = github_api.get_rate_limit()
        log.debug('GitHub API rate limit limit=%i, remaining=%i, reset=%s',
                  rate_limit.core.limit, rate_limit.core.remaining, rate_limit.core.reset)
        repo = github_api.get_repo(f'{owner}/{name}')
        path = () if path == '' else tuple(path.split('/'))
        return cls(repo=repo, ref=ref, path=path)

    @classmethod
    def from_url(cls, url: str) -> 'GitHubStagingAreaFactory':
        """
        :param url: The URL of a staging area in a GitHub repository with syntax
                    `https://github.com/<OWNER>/<NAME>/tree/<REF>[/<PATH>]`.
                    Note that REF can be a branch, tag, or commit SHA. If REF
                    contains special characters like `/`, '?` or `#` they must
                    be URL-encoded. This is especially noteworthy for `/` since
                    it's the only way to distinguish slashes in REF from those
                    in PATH. The slashes in PATH must not be URL-encoded, while
                    occurrences of `#` and `?` must.
        """
        parsed_url = furl(url)
        require(parsed_url.scheme == 'https', url)
        require(parsed_url.host == 'github.com', url)
        require(len(parsed_url.path.segments) > 3, url)
        require(parsed_url.path.segments[2] == 'tree', url)
        owner, name = parsed_url.path.segments[0:2]
        ref = parsed_url.path.segments[3]
        path = '/'.join(parsed_url.path.segments[4:])
        return cls.create(owner=owner, name=name, ref=ref, path=path)

    def load_staging_area(self) -> StagingArea:
        staging_area_folders = self._get_folders(path=self.path)
        expected_folders = {'data', 'descriptors', 'links', 'metadata'}
        require(set(staging_area_folders) == expected_folders,
                f'{self.repo.full_name} {self.path} is not a valid staging area')
        links = self._get_files(path=self.path + ('links',))
        metadata = {}
        for folder in self._get_folders(path=self.path + ('metadata',)):
            files = self._get_files(path=self.path + ('metadata', folder))
            metadata.update(files)
        descriptors = {}
        for folder in self._get_folders(path=self.path + ('descriptors',)):
            files = self._get_files(path=self.path + ('descriptors', folder))
            descriptors.update(files)
        return StagingArea(links=links, metadata=metadata, descriptors=descriptors)

    def _get_folders(self, path: Tuple[str, ...]) -> List[str]:
        folders = []
        log.debug('Getting list of folders in %s', path)
        try:
            path_contents = self.repo.get_contents(path='/'.join(path), ref=self.ref)
        except UnknownObjectException as e:
            raise ValueError('Github path not found', self.repo.full_name, self.path) from e
        for content in path_contents:
            if content.type == 'dir':
                folders.append(content.name)
        return folders

    def _get_files(self,
                   path: Tuple[str, ...],
                   ) -> Mapping[str, Union[LinksFile, MetadataFile, DescriptorFile]]:
        files = {}
        log.debug('Getting contents of %s', path)
        path_str = '/'.join(path)
        try:
            path_contents = self.repo.get_contents(path=path_str, ref=self.ref)
        except UnknownObjectException as e:
            raise ValueError('Github path not found', self.repo.full_name, path_str) from e
        for content in path_contents:
            if content.type == 'dir':
                log.warning('Unexpected folder %s found in %s/%s',
                            content.name, self.repo.full_name, path_str)
                continue
            file_name = content.name
            file_json = json.loads(content.decoded_content)
            file = JsonFile.from_json(file_name, file_json)
            if isinstance(file, LinksFile):
                require(path[-1] == 'links', content.path)
            elif isinstance(file, DescriptorFile):
                require(path[-2] == 'descriptors', content.path)
            else:
                require(path[-2] == 'metadata', content.path)
            try:
                existing_version = files[file.uuid].version
            except KeyError:
                files[file.uuid] = file
            else:
                # If multiple files with the same ID were found keep the
                # one with the largest version.
                reject(file.version == existing_version, file)
                if file.version > existing_version:
                    files[file.uuid] = file
                else:
                    pass
        return files
