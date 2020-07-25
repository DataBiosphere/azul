from collections import defaultdict
import itertools
import json
import logging
from operator import (
    itemgetter,
)
from typing import (
    Dict,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Set,
)

import attr
from more_itertools import one

from azul import cached_property
from azul.bigquery import (
    AbstractBigQueryAdapter,
    BigQueryAdapter,
)
from azul.indexer import (
    Bundle,
    BundleFQID,
)
from azul.types import (
    JSON,
    JSONs,
)
from azul.uuids import validate_uuid_prefix

log = logging.getLogger(__name__)


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class BigQueryDataset:
    project: str
    name: str
    is_snapshot: bool

    @classmethod
    def parse(cls, dataset: str) -> 'BigQueryDataset':
        # BigQuery (and by extension the TDR) does not allow : or / in dataset names
        service, project, target = dataset.split(':')
        target_type, target_name = target.split('/')
        assert service == 'tdr'
        if target_type == 'snapshot':
            return cls(project=project, name=target_name, is_snapshot=True)
        elif target_type == 'dataset':
            return cls(project=project, name=f'datarepo_{target_name}', is_snapshot=False)
        else:
            assert False, target_type


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
            "name": self.name,
            "uuid": self.uuid,
            "version": self.version,
            "content-type": f"{self.content_type}; dcp-type={self.dcp_type}",
            "size": self.size,
            **(
                {
                    "indexed": False,
                    **self.checksums.asdict()
                } if self.dcp_type == 'data' else {
                    "indexed": True,
                    **Checksums.without_values()
                }
            )
        }


class ManifestBundler:

    def __init__(self, bundle_fquid: BundleFQID):
        self.bundle_fqid = bundle_fquid
        self.metadata = {}
        self.manifest = []

    def add_entity(self, key: str, entity_type: str, entity_row: JSON) -> None:
        content_type = 'links' if entity_type == 'links' else entity_row['content_type']
        self.manifest.append(ManifestEntry(name=key,
                                           uuid=entity_row[entity_type + '_id'],
                                           version=entity_row['version'].strftime(AzulTDRClient.timestamp_format),
                                           size=entity_row['content_size'],
                                           content_type='application/json',
                                           dcp_type=f'\"metadata/{content_type}\"',
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

    def add_entity(self, entity_key, entity_type: str, entity_row: JSON) -> None:
        super().add_entity(entity_key, entity_type, entity_row)
        self.metadata[entity_key] = json.loads(entity_row['content'])


class AzulTDRClient:
    timestamp_format = '%Y-%m-%dT%H:%M:%S.%fZ'

    def __init__(self, dataset: BigQueryDataset):
        self.target = dataset
        self.big_query_adapter.assert_table_exists(dataset.name, 'links')

    @cached_property
    def big_query_adapter(self) -> AbstractBigQueryAdapter:
        return BigQueryAdapter(self.target.project)

    def list_links_ids(self, prefix: str) -> List[BundleFQID]:
        validate_uuid_prefix(prefix)
        current_bundles = self._query_latest_version(f'''
            SELECT links_id, version
            FROM {self.target.name}.links
            WHERE STARTS_WITH(links_id, "{prefix}")
        ''', group_by='links_id')
        return [BundleFQID(uuid=row['links_id'],
                           version=row['version'].strftime(self.timestamp_format))
                for row in current_bundles]

    def _query_latest_version(self, query: str, group_by: str) -> JSONs:
        iter_rows = self.big_query_adapter.run_sql(query)
        key = itemgetter(group_by)
        groups = itertools.groupby(sorted(iter_rows, key=key), key=key)
        return [self._choose_one_version(group) for _, group in groups]

    def _choose_one_version(self, versioned_items: Iterable[JSON]) -> JSON:
        if self.target.is_snapshot:
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
            FROM {self.target.name}.links
            WHERE links_id = "{bundle_fqid.uuid}"
                AND version = TIMESTAMP("{bundle_fqid.version}")
        '''))
        links_json = json.loads(links_row['content'])
        log.info('Retrieved links content, %s top-level links', len(links_json['links']))
        bundler.add_entity('links.json', 'links', links_row)

        entities = defaultdict(set)
        entities['project'].add(links_row['project_id'])
        for link in links_json['links']:
            link_type = link['link_type']
            if link_type == 'process_link':
                entities[link['process_type']].add(link['process_id'])
                for catgeory in ('input', 'output', 'protocol'):
                    for entity_ref in link[catgeory + 's']:
                        entities[entity_ref[catgeory + '_type']].add(entity_ref[catgeory + '_id'])
            elif link_type == 'supplementary_file_link':
                # For MVP, only project entities can have associated supplementary files.
                entity = link['entity']
                if entity['entity_type'] != 'project' or entity['entity_id'] != links_row['project_id']:
                    raise ValueError(f'Supplementary file not associated with bundle project: {entity}')
                for supp_file in link['files']:
                    entities['supplementary_file'].add(supp_file['file_id'])
            else:
                raise ValueError(f'Unexpected link_type: {link_type}')

        for entity_type, entity_ids in entities.items():
            pk_column = entity_type + '_id'
            non_pk_columns = bundler.data_columns if entity_type.endswith('_file') else bundler.metadata_columns
            columns = ', '.join(non_pk_columns | {pk_column})
            # It's ~32% faster to consolidate queries than to fetch entities separately.
            # Better way to write this in standardSQL would be
            # `id IN UNNEST([id1, id2...])` but IDK how to write an UNNEST
            # function in TinyQuery.
            uuid_in_list = ' OR '.join(f'{pk_column} = "{entity_id}"' for entity_id in entity_ids)
            rows = self._query_latest_version(f'''
                SELECT {columns}
                FROM {self.target.name}.{entity_type}
                WHERE {uuid_in_list}
            ''', group_by=pk_column)
            log.info('Retrieved %s %s entities', len(rows), entity_type)
            for i, row in enumerate(rows):
                bundler.add_entity(f'{entity_type}_{i}.json', entity_type, row)
                entity_ids.remove(row[pk_column])
            if entity_ids:
                raise ValueError(f"Required entities not found in {self.target.name}.{entity_type}: "
                                 f"{entity_ids}")

        return bundler.result()
