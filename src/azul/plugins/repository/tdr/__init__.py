from collections import (
    defaultdict,
)
from concurrent.futures.thread import (
    ThreadPoolExecutor,
)
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
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
)

import attr
from furl import (
    furl,
)
from google.api_core.exceptions import (
    Forbidden,
)
from google.cloud import (
    bigquery,
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
    BundleFQID,
)
from azul.plugins import (
    RepositoryFileDownload,
    RepositoryPlugin,
)
from azul.terra import (
    TDRClient,
    TDRSource,
    TerraDRSClient,
)
from azul.types import (
    JSON,
    is_optional,
)
from azul.uuids import (
    validate_uuid_prefix,
)

log = logging.getLogger(__name__)


class Plugin(RepositoryPlugin):

    @classmethod
    def create(cls, catalog: CatalogName) -> 'RepositoryPlugin':
        source = TDRSource.parse(config.tdr_source(catalog))
        return cls(source)

    def __init__(self, source: TDRSource) -> None:
        super().__init__()
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

    def fetch_bundle(self, bundle_fqid: BundleFQID) -> Bundle:
        now = time.time()
        bundle = self.emulate_bundle(bundle_fqid)
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

    timestamp_format = '%Y-%m-%dT%H:%M:%S.%fZ'

    @cached_property
    def _bigquery(self) -> bigquery.Client:
        with aws.service_account_credentials():
            return bigquery.Client(project=self._source.project)

    def _run_sql(self, query: str) -> BigQueryRows:
        delays = (10, 20, 40, 80)
        assert sum(delays) < config.contribution_lambda_timeout
        for attempt, delay in enumerate((*delays, None)):
            job = self._bigquery.query(query)
            try:
                return job.result()
            except Forbidden as e:
                if 'Exceeded rate limits' in e.message and delay is not None:
                    log.warning('Exceeded BigQuery rate limit during attempt %i/%i. Retrying in %is.',
                                attempt + 1, len(delays) + 1, delay, exc_info=e)
                    time.sleep(delay)
                else:
                    raise e
        assert False

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
        iter_rows = self._run_sql(query)
        key = itemgetter(group_by)
        groups = groupby(sorted(iter_rows, key=key), key=key)
        return [self._choose_one_version(group) for _, group in groups]

    def _choose_one_version(self, versioned_items: BigQueryRows) -> BigQueryRow:
        if self._source.is_snapshot:
            return one(versioned_items)
        else:
            return max(versioned_items, key=itemgetter('version'))

    def emulate_bundle(self, bundle_fqid: BundleFQID) -> Bundle:
        bundle = TDRBundle(source=self._source,
                           uuid=bundle_fqid.uuid,
                           version=bundle_fqid.version,
                           manifest=[],
                           metadata_files={})

        links_columns = ', '.join(bundle.metadata_columns | {'content', 'project_id', 'links_id'})
        links_row = one(self._run_sql(f'''
            SELECT {links_columns}
            FROM {self._source.bq_name}.links
            WHERE links_id = '{bundle_fqid.uuid}'
                AND version = TIMESTAMP('{bundle_fqid.version}')
        '''))
        links_json = json.loads(links_row['content'])
        bundle_project_id = links_row['project_id']
        log.info('Retrieved links content, %s top-level links', len(links_json['links']))
        bundle.add_entity('links.json', 'links', links_row)

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
            non_pk_columns = bundle.data_columns if entity_type.endswith('_file') else bundle.metadata_columns
            columns = ', '.join(non_pk_columns | {pk_column})
            uuid_in_list = ' OR '.join(f'{pk_column} = "{entity_id}"' for entity_id in entity_ids)
            log.info('Retrieving %i entities of type %r ...', len(entity_ids), entity_type)
            rows = self._query_latest_version(f'''
                SELECT {columns}
                FROM {self._source.bq_name}.{entity_type}
                WHERE {uuid_in_list}
            ''', group_by=pk_column)
            return rows

        with ThreadPoolExecutor(max_workers=config.num_tdr_workers) as executor:
            futures = {
                entity_type: executor.submit(retrieve_rows, entity_type, entity_ids)
                for entity_type, entity_ids in entities.items()
            }
        for entity_type, future in futures.items():
            e = future.exception()
            if e is None:
                rows = future.result()
                entity_ids = entities[entity_type]
                for i, row in enumerate(rows):
                    bundle.add_entity(f'{entity_type}_{i}.json', entity_type, row)
                    entity_ids.remove(row[entity_type + '_id'])
                reject(bool(entity_ids),
                       f'Required entities not found in {self._source.bq_name}.{entity_type}: '
                       f'{entity_ids}')
            else:
                log.error('TDR worker failed to retrieve entities of type %r',
                          entity_type, exc_info=e)
                raise e
        return bundle

    def verify_authorization(self):
        """
        Verify that the current service account is authorized to read from the
        TDR BigQuery tables.
        """
        try:
            one(self._run_sql(f'SELECT * FROM {self._source.bq_name}.links LIMIT 1'))
        except Forbidden:
            self.api_client.on_auth_failure(bigquery=True)
        else:
            log.info('Google service account is authorized for TDR BigQuery access.')

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
        blob = self._get_blob(bucket_name=url.netloc,
                              blob_name='/'.join(url.path.segments))
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


@attr.s(auto_attribs=True, kw_only=True)
class TDRBundle(Bundle):
    source: TDRSource

    def add_entity(self, entity_key: str, entity_type: str, entity_row: BigQueryRow) -> None:
        content_type = 'links' if entity_type == 'links' else entity_row['content_type']
        self._add_manifest_entry(name=entity_key,
                                 uuid=entity_row[entity_type + '_id'],
                                 version=entity_row['version'].strftime(Plugin.timestamp_format),
                                 size=entity_row['content_size'],
                                 content_type='application/json',
                                 dcp_type=f'"metadata/{content_type}"')
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
        self.metadata_files[entity_key] = json.loads(entity_row['content'])

    @property
    def metadata_columns(self) -> Set[str]:
        return {
            'version',
            'JSON_EXTRACT_SCALAR(content, "$.schema_type") AS content_type',
            'BYTE_LENGTH(content) AS content_size',
            'content'
        }

    @property
    def data_columns(self) -> Set[str]:
        return self.metadata_columns | {
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
                    'indexed': False,
                    'drs_path': drs_path,
                    **checksums.to_json()
                } if dcp_type == 'data' else {
                    'indexed': True,
                    'crc32c': '',
                    'sha256': ''
                }
            )
        })

    def _parse_file_id_column(self, file_id: str) -> Optional[str]:
        # The file_id column is present for datasets, but is usually null, may
        # contain unexpected/unusable values, and NEVER produces usable DRS URLs,
        # so we avoid parsing the column altogether for datasets.
        # Some developmental snapshots also expose null file_ids.
        if self.source.is_snapshot and file_id is not None:
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
