from collections import (
    defaultdict,
)
from concurrent.futures.thread import (
    ThreadPoolExecutor,
)
import itertools
import json
import logging
from operator import (
    itemgetter,
)
import time
from typing import (
    Dict,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Set,
)

from deprecated import (
    deprecated,
)
from furl import (
    furl,
)
from more_itertools import (
    one,
)

from azul import (
    RequirementError,
    cached_property,
    config,
    require,
)
from azul.bigquery import (
    AbstractBigQueryAdapter,
    BigQueryAdapter,
    BigQueryRow,
    BigQueryRows,
)
from azul.dss import (
    shared_credentials,
)
from azul.indexer import (
    Bundle,
    BundleFQID,
)
from azul.plugins import (
    RepositoryPlugin,
)
from azul.tdr import (
    TDRClient,
    TDRSource,
)
from azul.types import (
    JSON,
    MutableJSONs,
)
from azul.uuids import (
    validate_uuid_prefix,
)

log = logging.getLogger(__name__)


class Plugin(RepositoryPlugin):

    def __init__(self, source: Optional[TDRSource] = None) -> None:
        super().__init__()
        if source is None:
            source = TDRSource.parse(config.tdr_source)
        self._source = source

    @property
    def source(self) -> str:
        return str(self._source)

    @cached_property
    def api_client(self):
        return TDRClient()

    def list_bundles(self, prefix: str) -> List[BundleFQID]:
        log.info('Listing bundles in prefix %s.', prefix)
        bundle_ids = self.list_links_ids(prefix)
        log.info('Prefix %s contains %i bundle(s).', prefix, len(bundle_ids))
        return bundle_ids

    @deprecated
    def fetch_bundle_manifest(self, bundle_fqid: BundleFQID) -> MutableJSONs:
        raise NotImplementedError()

    def fetch_bundle(self, bundle_fqid: BundleFQID) -> Bundle:
        now = time.time()
        bundle = self.emulate_bundle(bundle_fqid)
        log.info("It took %.003fs to download bundle %s.%s",
                 time.time() - now, bundle.uuid, bundle.version)
        self._stash_source_id(bundle.manifest)
        return bundle

    def portal_db(self) -> Sequence[JSON]:
        return []

    def dss_deletion_subscription_query(self, prefix: str) -> JSON:
        return {}

    def dss_subscription_query(self, prefix: str) -> JSON:
        return {}

    def drs_path(self, manifest_entry: JSON, metadata: JSON) -> str:
        return f"v1_{manifest_entry['source_id']}_{manifest_entry['uuid']}"

    def drs_netloc(self) -> str:
        return furl(config.tdr_service_url).netloc

    def _stash_source_id(self, manifest_entries: MutableJSONs):
        source_id = self.api_client.get_source_id(self._source)
        for entry in manifest_entries:
            entry['source_id'] = source_id

    timestamp_format = '%Y-%m-%dT%H:%M:%S.%fZ'

    @cached_property
    def big_query_adapter(self) -> AbstractBigQueryAdapter:
        with shared_credentials():
            return BigQueryAdapter(self._source.project)

    def list_links_ids(self, prefix: str) -> List[BundleFQID]:
        validate_uuid_prefix(prefix)
        current_bundles = self._query_latest_version(f'''
            SELECT links_id, version
            FROM {self._source.bq_name}.links
            WHERE STARTS_WITH(links_id, '{prefix}')
        ''', group_by='links_id')
        return [BundleFQID(uuid=row['links_id'],
                           version=row['version'].strftime(self.timestamp_format))
                for row in current_bundles]

    def _query_latest_version(self, query: str, group_by: str) -> List[BigQueryRow]:
        iter_rows = self.big_query_adapter.run_sql(query)
        key = itemgetter(group_by)
        groups = itertools.groupby(sorted(iter_rows, key=key), key=key)
        return [self._choose_one_version(group) for _, group in groups]

    def _choose_one_version(self, versioned_items: BigQueryRows) -> BigQueryRow:
        # FIXME: Reenable uniqueness constraint
        #        https://github.com/DataBiosphere/azul/issues/2146
        if False and self._source.is_snapshot:
            return one(versioned_items)
        else:
            return max(versioned_items, key=itemgetter('version'))

    def emulate_bundle(self,
                       bundle_fqid: BundleFQID,
                       manifest_only: bool = False) -> Bundle:
        bundler = (ManifestBundler if manifest_only else ManifestAndMetadataBundler)(bundle_fqid)

        links_columns = ', '.join(bundler.metadata_columns | {'content', 'project_id', 'links_id'})
        links_row = one(self.big_query_adapter.run_sql(f'''
            SELECT {links_columns}
            FROM {self._source.bq_name}.links
            WHERE links_id = '{bundle_fqid.uuid}'
                AND version = TIMESTAMP('{bundle_fqid.version}')
        '''))
        links_json = json.loads(links_row['content'])
        bundle_project_id = links_row['project_id']
        log.info('Retrieved links content, %s top-level links', len(links_json['links']))
        bundler.add_entity('links.json', 'links', links_row)

        entities = defaultdict(set)
        entities['project'].add(bundle_project_id)
        for link in links_json['links']:
            link_type = link['link_type']
            if link_type == 'process_link':
                entities[link['process_type']].add(link['process_id'])
                for category in ('input', 'output', 'protocol'):
                    for entity_ref in link[category + 's']:
                        entities[entity_ref[category + '_type']].add(entity_ref[category + '_id'])
            elif link_type == 'supplementary_file_link':
                # For MVP, only project entities can have associated supplementary files.
                associate_type = link['entity']['entity_type']
                associate_id = link['entity']['entity_id']
                require(associate_type == 'project',
                        f'Supplementary file must be associated with entity of type "project", '
                        f'not "{associate_type}"')
                require(associate_id == bundle_project_id,
                        f'Supplementary file must be associated with the current project '
                        f'({bundle_project_id}, not {associate_id})')
                for supp_file in link['files']:
                    entities['supplementary_file'].add(supp_file['file_id'])
            else:
                raise RequirementError(f'Unexpected link_type: {link_type}')

        def retrieve_rows(entity_type: str, entity_ids: Set[str]):
            pk_column = entity_type + '_id'
            non_pk_columns = bundler.data_columns if entity_type.endswith('_file') else bundler.metadata_columns
            columns = ', '.join(non_pk_columns | {pk_column})
            uuid_in_list = ' OR '.join(f'{pk_column} = "{entity_id}"' for entity_id in entity_ids)
            rows = self._query_latest_version(f'''
                SELECT {columns}
                FROM {self._source.bq_name}.{entity_type}
                WHERE {uuid_in_list}
            ''', group_by=pk_column)
            log.info('Retrieved %s %s entities', len(rows), entity_type)
            return rows

        with ThreadPoolExecutor(max_workers=config.num_repo_workers) as executor:
            futures = {
                entity_type: executor.submit(retrieve_rows, entity_type, entity_ids)
                for entity_type, entity_ids in entities.items()
            }
        for entity_type, future in futures.items():
            rows = future.result()
            entity_ids = entities[entity_type]
            for i, row in enumerate(rows):
                bundler.add_entity(f'{entity_type}_{i}.json', entity_type, row)
                entity_ids.remove(row[entity_type + '_id'])
            if entity_ids:
                raise RuntimeError(f'Required entities not found in {self._source.bq_name}.{entity_type}: {entity_ids}')
        return bundler.result()


class Checksums(NamedTuple):
    crc32c: str
    sha1: str
    sha256: str
    s3_etag: str

    def asdict(self) -> Dict[str, str]:
        return self._asdict()

    @classmethod
    def without_values(cls) -> Dict[str, str]:
        return {f: None for f in cls._fields}

    @classmethod
    def extract(cls, json: JSON) -> 'Checksums':
        return cls(**{f: json[f] for f in cls._fields})


class ManifestEntry(NamedTuple):
    name: str
    uuid: str
    version: str
    size: int
    content_type: str
    dcp_type: str
    checksums: Optional[Checksums]

    @property
    def entry(self):
        return {
            'name': self.name,
            'uuid': self.uuid,
            'version': self.version,
            'content-type': f'{self.content_type}; dcp-type={self.dcp_type}',
            'size': self.size,
            **(
                {
                    'indexed': False,
                    **self.checksums.asdict()
                } if self.dcp_type == 'data' else {
                    'indexed': True,
                    **Checksums.without_values()
                }
            )
        }


class ManifestBundler:

    def __init__(self, bundle_fquid: BundleFQID):
        self.bundle_fqid = bundle_fquid
        self.metadata = {}
        self.manifest = []

    def add_entity(self, key: str, entity_type: str, entity_row: BigQueryRow) -> None:
        content_type = 'links' if entity_type == 'links' else entity_row['content_type']
        self.manifest.append(ManifestEntry(name=key,
                                           uuid=entity_row[entity_type + '_id'],
                                           version=entity_row['version'].strftime(Plugin.timestamp_format),
                                           size=entity_row['content_size'],
                                           content_type='application/json',
                                           dcp_type=f'"metadata/{content_type}"',
                                           checksums=None).entry)
        if entity_type.endswith('_file'):
            descriptor = json.loads(entity_row['descriptor'])
            self.manifest.append(ManifestEntry(name=entity_row['file_name'],
                                               uuid=descriptor['file_id'],
                                               version=descriptor['file_version'],
                                               size=descriptor['size'],
                                               content_type=descriptor['content_type'],
                                               dcp_type='data',
                                               checksums=Checksums.extract(descriptor)).entry)

    @property
    def metadata_columns(self) -> Set[str]:
        return {'version',
                'JSON_EXTRACT_SCALAR(content, "$.schema_type") AS content_type',
                'BYTE_LENGTH(content) AS content_size'}

    @property
    def data_columns(self) -> Set[str]:
        return self.metadata_columns | {'descriptor',
                                        'JSON_EXTRACT_SCALAR(content, "$.file_core.file_name") AS file_name'}

    def result(self):
        return Bundle.for_fqid(self.bundle_fqid, self.manifest, self.metadata)


class ManifestAndMetadataBundler(ManifestBundler):

    @property
    def metadata_columns(self) -> Set[str]:
        return super().metadata_columns | {'content'}

    def add_entity(self, entity_key, entity_type: str, entity_row: BigQueryRow) -> None:
        super().add_entity(entity_key, entity_type, entity_row)
        self.metadata[entity_key] = json.loads(entity_row['content'])
