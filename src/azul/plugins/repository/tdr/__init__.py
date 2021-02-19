from collections import (
    defaultdict,
)
from concurrent.futures.thread import (
    ThreadPoolExecutor,
)
import datetime
from itertools import (
    groupby,
)
import json
import logging
from operator import (
    itemgetter,
)
import time
from typing import (
    AbstractSet,
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    cast,
)
from urllib.parse import (
    unquote,
)

import attr
from furl import (
    furl,
)
import google.cloud.storage as gcs
from more_itertools import (
    one,
)

from azul import (
    CatalogName,
    RequirementError,
    cached_property,
    config,
    reject,
    require,
)
from azul.bigquery import (
    BigQueryRow,
    BigQueryRows,
)
from azul.deployment import (
    aws,
)
from azul.drs import (
    AccessMethod,
    DRSClient,
)
from azul.indexer import (
    Bundle,
    SourceRef,
    SourcedBundleFQID,
)
from azul.indexer.document import (
    EntityID,
    EntityReference,
    EntityType,
)
from azul.plugins import (
    RepositoryFileDownload,
    RepositoryPlugin,
)
from azul.terra import (
    TDRClient,
    TDRSourceName,
    TerraDRSClient,
)
from azul.types import (
    JSON,
    JSONs,
    is_optional,
)
from azul.uuids import (
    validate_uuid_prefix,
)

log = logging.getLogger(__name__)

Entities = Set[EntityReference]
EntitiesByType = Dict[EntityType, Set[EntityID]]


@attr.s(frozen=True, auto_attribs=True)
class Links:
    project: EntityReference
    processes: Entities = attr.Factory(set)
    protocols: Entities = attr.Factory(set)
    inputs: Entities = attr.Factory(set)
    outputs: Entities = attr.Factory(set)
    supplementary_files: Entities = attr.Factory(set)

    @classmethod
    def from_json(cls, project: EntityReference, links_json: JSON) -> 'Links':
        """
        A `links.json` file, in a more accessible form.

        :param links_json: The contents of a `links.json` file.

        :param project: A reference to the project the given `links.json`
                        belongs to.
        """
        self = cls(project)
        for link in links_json['links']:
            link_type = link['link_type']
            if link_type == 'process_link':
                self.processes.add(EntityReference(entity_type=link['process_type'],
                                                   entity_id=link['process_id']))
                for category in ('input', 'output', 'protocol'):
                    plural = category + 's'
                    target = getattr(self, plural)
                    for entity in cast(JSONs, link[plural]):
                        target.add(EntityReference(entity_type=entity[category + '_type'],
                                                   entity_id=entity[category + '_id']))
            elif link_type == 'supplementary_file_link':
                associate = EntityReference(entity_type=link['entity']['entity_type'],
                                            entity_id=link['entity']['entity_id'])
                # For MVP, only project entities can have associated supplementary files.
                require(associate == project,
                        'Supplementary file must be associated with the current project',
                        project, associate)
                for entity in cast(JSONs, link['files']):
                    self.supplementary_files.add(
                        EntityReference(entity_type='supplementary_file',
                                        entity_id=entity['file_id']))
            else:
                raise RequirementError('Unexpected link_type', link_type)
        return self

    def all_entities(self) -> Entities:
        return set.union(*(value if isinstance(value, set) else {value}
                           for field, value in attr.asdict(self, recurse=False).items()))

    def dangling_inputs(self) -> Entities:
        return {
            input_
            for input_ in self.inputs
            if input_.entity_type.endswith('_file') and not (
                input_ in self.outputs or
                input_ in self.supplementary_files
            )
        }


class TDRSourceRef(SourceRef[TDRSourceName, 'TDRSourceRef']):

    # Stub is needed to aid PyCharm type hinting. Without this, instantiations
    # of TDRSourceRef cause PyCharm to warn about the `name` parameter.
    #
    def __init__(self, *, id: str, name: TDRSourceName) -> None:
        super().__init__(id=id, name=name)


TDRBundleFQID = SourcedBundleFQID[TDRSourceRef]


@attr.s(kw_only=True, auto_attribs=True, frozen=True)
class Plugin(RepositoryPlugin[TDRSourceName, TDRSourceRef]):
    _sources: AbstractSet[TDRSourceName]

    @classmethod
    def create(cls, catalog: CatalogName) -> 'RepositoryPlugin':
        return cls(sources=frozenset(
            TDRSourceName.parse(name)
            for name in config.tdr_sources(catalog))
        )

    @property
    def sources(self) -> AbstractSet[TDRSourceName]:
        return self._sources

    @cached_property
    def tdr(self):
        return TDRClient()

    def _assert_source(self, source: TDRSourceRef):
        assert source.name in self.sources, (source, self.sources)

    def lookup_source_id(self, name: TDRSourceName) -> str:
        return self.tdr.lookup_source_id(name)

    def list_bundles(self, source: TDRSourceRef, prefix: str) -> List[TDRBundleFQID]:
        self._assert_source(source)
        log.info('Listing bundles with prefix %r in source %r.', prefix, source)
        bundle_fqids = self._list_links_ids(source, prefix)
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

    def dss_deletion_subscription_query(self, prefix: str) -> JSON:
        return {}

    def dss_subscription_query(self, prefix: str) -> JSON:
        return {}

    def drs_uri(self, drs_path: str) -> str:
        netloc = furl(config.tdr_service_url).netloc
        return f'drs://{netloc}/{drs_path}'

    def direct_file_url(self,
                        file_uuid: str,
                        *,
                        file_version: Optional[str] = None,
                        replica: Optional[str] = None
                        ) -> Optional[str]:
        return None

    @classmethod
    def format_version(cls, version: datetime.datetime) -> str:
        return version.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    def _run_sql(self, query):
        return self.tdr.run_sql(query)

    def _full_table_name(self, source: TDRSourceName, table_name: str) -> str:
        return source.qualify_table(table_name)

    def _list_links_ids(self, source: TDRSourceRef, prefix: str) -> List[TDRBundleFQID]:

        validate_uuid_prefix(prefix)
        current_bundles = self._query_latest_version(source.name, f'''
            SELECT links_id, version
            FROM {self._full_table_name(source.name, 'links')}
            WHERE STARTS_WITH(links_id, '{prefix}')
        ''', group_by='links_id')
        return [
            SourcedBundleFQID(source=source,
                              uuid=row['links_id'],
                              version=self.format_version(row['version']))
            for row in current_bundles
        ]

    def _query_latest_version(self, source: TDRSourceName, query: str, group_by: str) -> List[BigQueryRow]:
        iter_rows = self._run_sql(query)
        key = itemgetter(group_by)
        groups = groupby(sorted(iter_rows, key=key), key=key)
        return [self._choose_one_version(source, group) for _, group in groups]

    def _choose_one_version(self, source: TDRSourceName, versioned_items: BigQueryRows) -> BigQueryRow:
        if source.is_snapshot:
            return one(versioned_items)
        else:
            return max(versioned_items, key=itemgetter('version'))

    def _emulate_bundle(self, bundle_fqid: SourcedBundleFQID) -> Bundle:
        bundle = TDRBundle(fqid=bundle_fqid,
                           manifest=[],
                           metadata_files={})
        entities, links_jsons = self._stitch_bundles(bundle)
        bundle.add_entity('links.json', 'links', self._merge_links(links_jsons))

        with ThreadPoolExecutor(max_workers=config.num_tdr_workers) as executor:
            futures = {
                entity_type: executor.submit(self._retrieve_entities,
                                             bundle.fqid.source.name,
                                             entity_type,
                                             entity_ids)
                for entity_type, entity_ids in entities.items()
            }
            for entity_type, future in futures.items():
                e = future.exception()
                if e is None:
                    rows = future.result()
                    rows.sort(key=itemgetter(entity_type + '_id'))
                    for i, row in enumerate(rows):
                        bundle.add_entity(f'{entity_type}_{i}.json', entity_type, row)
                else:
                    log.error('TDR worker failed to retrieve entities of type %r',
                              entity_type, exc_info=e)
                    raise e
        bundle.manifest.sort(key=itemgetter('uuid'))
        return bundle

    def _stitch_bundles(self,
                        root_bundle: 'TDRBundle'
                        ) -> Tuple[EntitiesByType, List[JSON]]:
        """
        Recursively follow dangling inputs to collect entities from upstream
        bundles, ensuring that no bundle is processed more than once.
        """
        source = root_bundle.fqid.source
        entities: EntitiesByType = defaultdict(set)
        unprocessed: Set[SourcedBundleFQID] = {root_bundle.fqid}
        processed: Set[SourcedBundleFQID] = set()
        stitched_links: List[JSON] = []
        while unprocessed:
            bundle = unprocessed.pop()
            processed.add(bundle)
            links = self._retrieve_links(bundle)
            stitched_links.append(links)
            project = EntityReference(entity_type='project',
                                      entity_id=links['project_id'])
            links = Links.from_json(project, links['content'])
            for entity in links.all_entities():
                entities[entity.entity_type].add(entity.entity_id)

            dangling_inputs = links.dangling_inputs()
            if dangling_inputs:
                if log.isEnabledFor(logging.DEBUG):
                    log.debug('Bundle %r has dangling inputs: %r', bundle, dangling_inputs)
                else:
                    log.info('Bundle %r has %i dangling inputs', bundle, len(dangling_inputs))
                upstream = self._find_upstream_bundles(source, dangling_inputs)
                unprocessed |= upstream - processed
            else:
                log.debug('Bundle %r is self-contained', bundle)
        if log.isEnabledFor(logging.DEBUG):
            log.debug('Stitched together bundles: %r', processed)
        else:
            log.info('Stitched together %i bundles', len(processed))
        return entities, stitched_links

    def _retrieve_links(self, links_id: SourcedBundleFQID) -> JSON:
        """
        Retrieve a links entity from BigQuery and parse the `content` column.

        :param links_id: Which links entity to retrieve.
        """
        links_columns = ', '.join(
            TDRBundle.metadata_columns | {'project_id', 'links_id'}
        )
        source = links_id.source.name
        links = one(self._run_sql(f'''
            SELECT {links_columns}
            FROM {self._full_table_name(source, 'links')}
            WHERE links_id = '{links_id.uuid}'
                AND version = TIMESTAMP('{links_id.version}')
        '''))
        links = dict(links)  # Enable item assignment to pre-parse content JSON
        links['content'] = json.loads(links['content'])
        return links

    def _retrieve_entities(self,
                           source: TDRSourceName,
                           entity_type: EntityType,
                           entity_ids: Set[EntityID]
                           ) -> BigQueryRows:
        pk_column = entity_type + '_id'
        non_pk_columns = (TDRBundle.data_columns
                          if entity_type.endswith('_file')
                          else TDRBundle.metadata_columns)
        columns = ', '.join({pk_column, *non_pk_columns})
        uuid_in_list = ' OR '.join(
            f'{pk_column} = "{entity_id}"' for entity_id in entity_ids
        )
        table_name = self._full_table_name(source, entity_type)
        log.debug('Retrieving %i entities of type %r ...', len(entity_ids), entity_type)
        rows = self._query_latest_version(source, f'''
                       SELECT {columns}
                       FROM {table_name}
                       WHERE {uuid_in_list}
                   ''', group_by=pk_column)
        log.debug('Retrieved %i entities of type %r', len(rows), entity_type)
        missing = entity_ids - {row[pk_column] for row in rows}
        require(not missing,
                f'Required entities not found in {table_name}: {missing}')
        return rows

    def _find_upstream_bundles(self,
                               source: TDRSourceRef,
                               outputs: Entities) -> Set[SourcedBundleFQID]:
        """
        Search for bundles containing processes that produce the specified output
        entities.
        """
        output_ids = [output.entity_id for output in outputs]
        output_id = 'JSON_EXTRACT_SCALAR(link_output, "$.output_id")'
        rows = self._run_sql(f'''
            SELECT links_id, version, {output_id} AS output_id
            FROM {self._full_table_name(source.name, 'links')} AS links
                JOIN UNNEST(JSON_EXTRACT_ARRAY(links.content, '$.links')) AS content_links
                    ON JSON_EXTRACT_SCALAR(content_links, '$.link_type') = 'process_link'
                JOIN UNNEST(JSON_EXTRACT_ARRAY(content_links, '$.outputs')) AS link_output
                    ON {output_id} IN UNNEST({output_ids})
        ''')
        bundles = set()
        outputs_found = set()
        for row in rows:
            bundles.add(SourcedBundleFQID(source=source,
                                          uuid=row['links_id'],
                                          version=self.format_version(row['version'])))
            outputs_found.add(row['output_id'])
        missing = set(output_ids) - outputs_found
        require(not missing,
                f'Dangling inputs not found in any bundle: {missing}')
        return bundles

    def _merge_links(self, links_jsons: JSONs) -> JSON:
        """
        Merge the links.json documents from multiple stitched bundles into a
        single document.
        """
        root, *stitched = links_jsons
        if stitched:
            merged = {'links_id': root['links_id'],
                      'version': root['version']}
            for common_key in ('project_id', 'schema_type'):
                merged[common_key] = one({row[common_key] for row in links_jsons})
            merged_content = {}
            source_contents = [row['content'] for row in links_jsons]
            for common_key in ('describedBy', 'schema_type', 'schema_version'):
                merged_content[common_key] = one({sc[common_key] for sc in source_contents})
            merged_content['links'] = sum((sc['links'] for sc in source_contents),
                                          start=[])
            merged['content'] = merged_content  # Keep result of parsed JSON for reuse
            merged['content_size'] = len(json.dumps(merged_content))
            assert merged.keys() == one({
                frozenset(row.keys()) for row in links_jsons
            }), merged
            assert merged_content.keys() == one({
                frozenset(sc.keys()) for sc in source_contents
            }), merged_content
            return merged
        else:
            return root

    def drs_client(self) -> DRSClient:
        return TerraDRSClient()

    def file_download_class(self) -> Type[RepositoryFileDownload]:
        return TDRFileDownload


class TDRFileDownload(RepositoryFileDownload):

    def _get_blob(self, bucket_name: str, blob_name: str) -> gcs.Blob:
        """
        Get a Blob object by name.
        """
        with aws.service_account_credentials():
            client = gcs.Client()
        bucket = gcs.Bucket(client, bucket_name)
        return bucket.get_blob(blob_name)

    _location: Optional[str] = None

    def update(self, plugin: RepositoryPlugin) -> None:
        require(self.replica is None or self.replica == 'gcp')
        assert self.drs_path is not None
        drs_uri = plugin.drs_uri(self.drs_path)
        drs_client = plugin.drs_client()
        access = drs_client.get_object(drs_uri, access_method=AccessMethod.gs)
        assert access.headers is None
        url = furl(access.url)
        blob_name = '/'.join(url.path.segments)
        # https://github.com/databiosphere/azul/issues/2479#issuecomment-733410253
        if url.fragmentstr:
            blob_name += '#' + unquote(url.fragmentstr)
        else:
            # furl does not differentiate between no fragment and empty
            # fragment
            if access.url.endswith('#'):
                blob_name += '#'
        blob = self._get_blob(bucket_name=url.netloc, blob_name=blob_name)
        expiration = int(time.time() + 3600)
        file_name = self.file_name.replace('"', r'\"')
        assert all(0x1f < ord(c) < 0x80 for c in file_name)
        disposition = f"attachment; filename={file_name}"
        signed_url = blob.generate_signed_url(expiration=expiration,
                                              response_disposition=disposition)
        self._location = signed_url

    @property
    def location(self) -> Optional[str]:
        return self._location

    @property
    def retry_after(self) -> Optional[int]:
        return None


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class Checksums:
    crc32c: str
    sha1: Optional[str] = None
    sha256: str
    s3_etag: Optional[str] = None

    def to_json(self) -> Dict[str, str]:
        """
        >>> Checksums(crc32c='a', sha1='b', sha256='c', s3_etag=None).to_json()
        {'crc32c': 'a', 'sha1': 'b', 'sha256': 'c'}
        """
        return {k: v for k, v in attr.asdict(self).items() if v is not None}

    @classmethod
    def from_json(cls, json: JSON) -> 'Checksums':
        """
        >>> Checksums.from_json({'crc32c': 'a', 'sha256': 'c'})
        Checksums(crc32c='a', sha1=None, sha256='c', s3_etag=None)

        >>> Checksums.from_json({'crc32c': 'a', 'sha1':'b', 'sha256': 'c', 's3_etag': 'd'})
        Checksums(crc32c='a', sha1='b', sha256='c', s3_etag='d')

        >>> Checksums.from_json({'crc32c': 'a'})
        Traceback (most recent call last):
            ...
        ValueError: ('JSON property cannot be absent or null', 'sha256')
        """

        def extract_field(field: attr.Attribute) -> Tuple[str, Any]:
            value = json.get(field.name)
            if value is None and not is_optional(field.type):
                raise ValueError('JSON property cannot be absent or null', field.name)
            return field.name, value

        return cls(**dict(map(extract_field, attr.fields(cls))))


class TDRBundle(Bundle[TDRSourceRef]):

    def add_entity(self, entity_key: str, entity_type: EntityType, entity_row: BigQueryRow) -> None:
        entity_id = entity_row[entity_type + '_id']
        self._add_manifest_entry(name=entity_key,
                                 uuid=entity_id,
                                 version=Plugin.format_version(entity_row['version']),
                                 size=entity_row['content_size'],
                                 content_type='application/json',
                                 dcp_type=f'"metadata/{entity_row["schema_type"]}"')
        if entity_type.endswith('_file'):
            descriptor = json.loads(entity_row['descriptor'])
            self._add_manifest_entry(name=entity_row['file_name'],
                                     uuid=descriptor['file_id'],
                                     version=descriptor['file_version'],
                                     size=descriptor['size'],
                                     content_type=descriptor['content_type'],
                                     dcp_type='data',
                                     checksums=Checksums.from_json(descriptor),
                                     drs_path=self._parse_file_id_column(entity_row['file_id']))
        content = entity_row['content']
        self.metadata_files[entity_key] = (json.loads(content)
                                           if isinstance(content, str)
                                           else content)

    metadata_columns: ClassVar[Set[str]] = {
        'version',
        'JSON_EXTRACT_SCALAR(content, "$.schema_type") AS schema_type',
        'BYTE_LENGTH(content) AS content_size',
        'content'
    }

    data_columns: ClassVar[Set[str]] = metadata_columns | {
        'descriptor',
        'JSON_EXTRACT_SCALAR(content, "$.file_core.file_name") AS file_name',
        'file_id'
    }

    def drs_path(self, manifest_entry: JSON) -> Optional[str]:
        return manifest_entry.get('drs_path')

    def _add_manifest_entry(self,
                            *,
                            name: str,
                            uuid: str,
                            version: str,
                            size: int,
                            content_type: str,
                            dcp_type: str,
                            checksums: Optional[Checksums] = None,
                            drs_path: Optional[str] = None) -> None:
        self.manifest.append({
            'name': name,
            'uuid': uuid,
            'version': version,
            'content-type': f'{content_type}; dcp-type={dcp_type}',
            'size': size,
            **(
                {
                    'indexed': True,
                    'crc32c': '',
                    'sha256': ''
                } if checksums is None else {
                    'indexed': False,
                    'drs_path': drs_path,
                    **checksums.to_json()
                }
            )
        })

    def _parse_file_id_column(self, file_id: Optional[str]) -> Optional[str]:
        # The file_id column is present for datasets, but is usually null, may
        # contain unexpected/unusable values, and NEVER produces usable DRS URLs,
        # so we avoid parsing the column altogether for datasets.
        if self.fqid.source.name.is_snapshot:
            reject(file_id is None)
            # TDR stores the complete DRS URI in the file_id column, but we only
            # index the path component. These requirements prevent mismatches in
            # the DRS domain, and ensure that changes to the column syntax don't
            # go undetected.
            file_id = furl(file_id)
            require(file_id.scheme == 'drs')
            require(file_id.netloc == furl(config.tdr_service_url).netloc)
            return str(file_id.path).strip('/')
        else:
            return None
