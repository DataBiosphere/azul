from abc import (
    ABCMeta,
    abstractmethod,
)
from collections import defaultdict
from copy import deepcopy
import csv
from datetime import (
    datetime,
    timedelta,
    timezone,
)
import email.utils
from enum import (
    Enum,
)
from io import (
    StringIO,
    TextIOWrapper,
)
from itertools import chain
import logging
import os
import re
from tempfile import (
    TemporaryDirectory,
    mkstemp,
)
import time
from typing import (
    Any,
    IO,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Tuple,
    cast,
)
import uuid

from bdbag import bdbag_api
from elasticsearch_dsl import Search
from elasticsearch_dsl.response import Hit
from more_itertools import one
import unicodedata
from werkzeug.http import parse_dict_header

from azul import (
    CatalogName,
    cached_property,
    config,
    drs,
)
from azul.json_freeze import (
    freeze,
    sort_frozen,
)
from azul.plugins import (
    ColumnMapping,
    ManifestConfig,
    MutableManifestConfig,
)
from azul.service import Filters
from azul.service.buffer import FlushableBuffer
from azul.service.elasticsearch_service import (
    ElasticsearchService,
    SourceFilters,
)
from azul.service.storage_service import (
    AWS_S3_DEFAULT_MINIMUM_PART_SIZE,
    StorageService,
)
from azul.types import (
    JSON
)

logger = logging.getLogger(__name__)


class ManifestFormat(Enum):
    compact = 'compact'
    full = 'full'
    terra_bdbag = 'terra.bdbag'


class ManifestService(ElasticsearchService):

    def __init__(self, storage_service: StorageService):
        super().__init__()
        self.storage_service = storage_service

    def get_manifest(self,
                     format_: ManifestFormat,
                     catalog: CatalogName,
                     filters: Filters,
                     object_key: Optional[str] = None) -> Tuple[str, bool]:
        """
        Returns a tuple in which the first element is pre-signed URL to a
        manifest in the given format and of the file entities matching the given
        filter. If a suitable manifest already exists, its location will be
        returned. Otherwise, a new manifest will be generated and its location
        returned. Subsequent invocations of this method with the same arguments
        are likely to reuse that manifest, skipping the time-consuming manifest
        generation.

        The second element of the returned tuple is True if an existing
        manifest was reused and False if a new manifest was generated.

        :param format_: The desired format of the manifest.

        :param catalog: The name of the catalog to generate the manifest from.

        :param filters: The filters by which to restrict the contents of the
                        manifest.

        :param object_key: An optional S3 object key of the cached manifest. If
                           None, the key will be computed dynamically. This may
                           take a few seconds. If a valid cached manifest exists
                           under that key, it will be used. Otherwise, a new
                           manifest will be created and stored at the given key.
        """
        generator = ManifestGenerator.for_format(format_=format_,
                                                 service=self,
                                                 catalog=catalog,
                                                 filters=filters)
        if object_key is None:
            object_key = self._compute_object_key(generator=generator,
                                                  format_=format_,
                                                  catalog=catalog,
                                                  filters=filters)
        presigned_url = self._get_cached_manifest(generator, object_key)
        if presigned_url is None:
            file_name = self._generate_manifest(generator, object_key)
            presigned_url = self.storage_service.get_presigned_url(object_key, file_name=file_name)
            return presigned_url, False
        else:
            return presigned_url, True

    def get_cached_manifest(self,
                            format_: ManifestFormat,
                            catalog: CatalogName,
                            filters: Filters
                            ) -> Tuple[str, Optional[str]]:
        generator = ManifestGenerator.for_format(format_, self, catalog, filters)
        object_key = self._compute_object_key(generator, format_, catalog, filters)
        presigned_url = self._get_cached_manifest(generator, object_key)
        return object_key, presigned_url

    def _compute_object_key(self,
                            generator: 'ManifestGenerator',
                            format_: ManifestFormat,
                            catalog: CatalogName,
                            filters: Filters) -> str:
        manifest_key = self._derive_manifest_key(format_, catalog, filters, generator.manifest_content_hash)
        object_key = f'manifests/{manifest_key}.{generator.file_name_extension}'
        return object_key

    def _get_cached_manifest(self,
                             generator: 'ManifestGenerator',
                             object_key: str
                             ) -> Optional[str]:
        if self._can_use_cached_manifest(object_key):
            file_name = self._use_cached_manifest(generator, object_key)
            return self.storage_service.get_presigned_url(object_key, file_name=file_name)
        else:
            return None

    file_name_tag = 'azul_file_name'

    def _generate_manifest(self, generator: 'ManifestGenerator', object_key: str) -> Optional[str]:
        """
        Generate the manifest and return the desired content disposition file
        name if necessary.
        """

        def file_name_(base_name: str) -> str:
            if base_name:
                assert generator.use_content_disposition_file_name
                file_name_prefix = unicodedata.normalize('NFKD', base_name)
                file_name_prefix = re.sub(r'[^\w ,.@%&\-_()\\[\]/{}]', '_', file_name_prefix).strip()
                timestamp = datetime.now().strftime("%Y-%m-%d %H.%M")
                file_name = f'{file_name_prefix} {timestamp}.{generator.file_name_extension}'
            else:
                if generator.use_content_disposition_file_name:
                    file_name = 'hca-manifest-' + object_key.rsplit('/', )[-1]
                else:
                    file_name = None
            return file_name

        def tagging_(file_name: str) -> Optional[Mapping[str, str]]:
            return None if file_name is None else {self.file_name_tag: file_name}

        content_type = generator.content_type
        if isinstance(generator, FileBasedManifestGenerator):
            file_path, base_name = generator.create_file()
            file_name = file_name_(base_name)
            try:
                self.storage_service.upload(file_path,
                                            object_key,
                                            content_type=content_type,
                                            tagging=tagging_(file_name))
            finally:
                os.remove(file_path)
        elif isinstance(generator, StreamingManifestGenerator):
            if config.disable_multipart_manifests:
                output = StringIO()
                base_name = generator.write_to(output)
                file_name = file_name_(base_name)
                self.storage_service.put(object_key,
                                         data=output.getvalue().encode(),
                                         content_type=content_type,
                                         tagging=tagging_(file_name))
            else:
                with self.storage_service.put_multipart(object_key, content_type=content_type) as upload:
                    with FlushableBuffer(AWS_S3_DEFAULT_MINIMUM_PART_SIZE, upload.push) as buffer:
                        text_buffer = TextIOWrapper(buffer, encoding='utf-8', write_through=True)
                        base_name = generator.write_to(text_buffer)
                file_name = file_name_(base_name)
                if file_name is not None:
                    self.storage_service.put_object_tagging(object_key, tagging_(file_name))
        else:
            raise NotImplementedError('Unsupported generator type', type(generator))
        return file_name

    def _use_cached_manifest(self, generator: 'ManifestGenerator', object_key: str) -> Optional[str]:
        """
        Return the content disposition file name of the exiting cached manifest.
        """
        if generator.use_content_disposition_file_name:
            tagging = self.storage_service.get_object_tagging(object_key)
            file_name = tagging.get(self.file_name_tag)
            if file_name is None:
                logger.warning("Manifest object '%s' doesn't have the '%s' tag."
                               "Generating pre-signed URL without Content-Disposition header.",
                               object_key, self.file_name_tag)
        else:
            file_name = None
        return file_name

    def _derive_manifest_key(self,
                             format_: ManifestFormat,
                             catalog: CatalogName,
                             filters: Filters,
                             content_hash: int
                             ) -> str:
        """
        Return a manifest key deterministically derived from the arguments and
        the current commit hash. The same arguments will always produce the same
        return value in one revision of this code. Different arguments should,
        with a very high probability, produce different return values.
        """
        git_commit = config.lambda_git_status['commit']
        manifest_namespace = uuid.UUID('ca1df635-b42c-4671-9322-b0a7209f0235')
        filter_string = repr(sort_frozen(freeze(filters)))
        content_hash = str(content_hash)
        disable_multipart = str(config.disable_multipart_manifests)
        manifest_key_params = (
            git_commit,
            catalog,
            format_.value,
            content_hash,
            disable_multipart,
            filter_string
        )
        assert not any(',' in param for param in manifest_key_params[:-1])
        return str(uuid.uuid5(manifest_namespace, ','.join(manifest_key_params)))

    _date_diff_margin = 10  # seconds

    @classmethod
    def _get_seconds_until_expire(cls, head_response: Mapping[str, Any]) -> float:
        """
        Get the number of seconds before a cached manifest is past its expiration.

        :param head_response: A storage service object header dict
        :return: time to expiration in seconds
        """
        # example Expiration: 'expiry-date="Fri, 21 Dec 2012 00:00:00 GMT", rule-id="Rule for testfile.txt"'
        now = datetime.now(timezone.utc)
        expiration = parse_dict_header(head_response['Expiration'])
        expiry_datetime = email.utils.parsedate_to_datetime(expiration['expiry-date'])
        expiry_seconds = (expiry_datetime - now).total_seconds()
        # Verify that 'Expiration' matches value calculated from 'LastModified'
        last_modified = head_response['LastModified']
        expected_expiry_date: datetime = last_modified + timedelta(days=config.manifest_expiration)
        expected_expiry_seconds = (expected_expiry_date - now).total_seconds()
        if abs(expiry_seconds - expected_expiry_seconds) > cls._date_diff_margin:
            logger.error('The actual object expiration (%s) does not match expected value (%s)',
                         expiration, expected_expiry_date)
        else:
            logger.debug('Manifest object expires in %s seconds, on %s', expiry_seconds, expiry_datetime)
        return expiry_seconds

    def _can_use_cached_manifest(self, object_key: str) -> bool:
        """
        Check if the manifest was previously created, still exists in the bucket and won't be expiring soon.

        :param object_key: S3 object key (eg. 'manifests/e0fabf97-7abb-5111-af97-810f1e736c71.tsv'
        """
        try:
            response = self.storage_service.head(object_key)
        except self.storage_service.client.exceptions.ClientError as e:
            if int(e.response['Error']['Code']) == 404:
                logger.info('Cached manifest not found: %s', object_key)
                return False
            else:
                raise e
        else:
            seconds_until_expire = self._get_seconds_until_expire(response)
            if seconds_until_expire > config.manifest_expiration_margin:
                return True
            else:
                logger.info('Cached manifest about to expire: %s', object_key)
                return False


Cells = MutableMapping[str, str]


class ManifestGenerator(metaclass=ABCMeta):
    """
    A generator for manifests. A manifest is an exhaustive representation of
    the documents in the aggregate index for a particular entity type. The
    generator queries that index for documents that match a given filter and
    transforms the result.
    """

    # Note to implementors: all property getters in this class and its
    # descendants must be inexpensive. If a property getter performs and
    # expensive computation or I/O, it should cache its return value.

    @property
    @abstractmethod
    def file_name_extension(self) -> str:
        """
        The file name extension to use when persisting the output of this
        generator to a file system or an object store.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def content_type(self) -> str:
        """
        The MIME type to use when describing the output of this generator.
        """
        raise NotImplementedError()

    @property
    def use_content_disposition_file_name(self) -> bool:
        """
        True if the manifest output produced by the generator should use a custom
        file name when stored on a file system.
        """
        return True

    @property
    @abstractmethod
    def entity_type(self) -> str:
        """
        The type of the index entities this generator consumes. This controls
        which aggregate Elasticsearch index is queried to fetch the aggregate
        entity documents that this generator consumes when generating the
        output manifest.
        """
        raise NotImplementedError()

    @cached_property
    def manifest_config(self) -> ManifestConfig:
        """
        The manifest config this generator uses. A manifest config is a mapping
        from document properties to manifest fields.
        """
        return self.service.service_config(self.catalog).manifest

    @cached_property
    def source_filter(self) -> SourceFilters:
        """
        A list of document paths or path patterns to be included when requesting
        entity documents from the index. Exclusions are not supported.

        https://www.elastic.co/guide/en/elasticsearch/reference/5.5/search-request-source-filtering.html
        """
        return [
            field_path_prefix + '.' + field_name
            for field_path_prefix, field_mapping in self.manifest_config.items()
            for field_name in field_mapping.values()
        ]

    @classmethod
    def for_format(cls,
                   format_: ManifestFormat,
                   service: ManifestService,
                   catalog: CatalogName,
                   filters: Filters) -> 'ManifestGenerator':
        """
        Return a generator instance for the given format and filters.

        :param format_: format specifying which generator to use

        :param catalog: the name of the catalog to use when querying the index
                        for the documents to be transformed into the manifest

        :param filters: the filter to use when querying the index for the
                        documents to be transformed into the manifest

        :param service: the service to use when querying the index

        :return: a ManifestGenerator instance. Note that the protocol used for
                 consuming the generator output is defined in subclasses.
        """
        if format_ is ManifestFormat.compact:
            return CompactManifestGenerator(service, catalog, filters)
        elif format_ is ManifestFormat.full:
            return FullManifestGenerator(service, catalog, filters)
        elif format_ is ManifestFormat.terra_bdbag:
            return BDBagManifestGenerator(service, catalog, filters)
        else:
            assert False, format_

    def __init__(self,
                 service: ManifestService,
                 catalog: CatalogName,
                 filters: Filters
                 ) -> None:
        super().__init__()
        self.service = service
        self.catalog = catalog
        self.filters = filters

    def _create_request(self) -> Search:
        # We consider this class a friend of the manifest service
        # noinspection PyProtectedMember
        return self.service._create_request(catalog=self.catalog,
                                            filters=self.filters,
                                            post_filter=False,
                                            source_filter=self.source_filter,
                                            enable_aggregation=False,
                                            entity_type=self.entity_type)

    def _hit_to_doc(self, hit: Hit) -> JSON:
        return self.service.translate_fields(self.catalog, hit.to_dict(), forward=False)

    column_joiner = ' || '

    def _extract_fields(self,
                        entities: List[JSON],
                        column_mapping: ColumnMapping,
                        row: Cells):
        stripped_joiner = self.column_joiner.strip()

        def validate(s: str) -> str:
            assert stripped_joiner not in s
            return s

        for column_name, field_name in column_mapping.items():
            assert column_name not in row, f'Column mapping defines {column_name} twice'
            column_value = []
            for entity in entities:
                try:
                    field_value = entity[field_name]
                except KeyError:
                    pass
                else:
                    if isinstance(field_value, list):
                        column_value += [validate(str(v)) for v in field_value if v is not None]
                    else:
                        column_value.append(validate(str(field_value)))
            column_value = self.column_joiner.join(sorted(set(column_value)))
            row[column_name] = column_value

    def _get_entities(self, path: str, doc: JSON) -> List[JSON]:
        """
        Given a document and a dotted path into that document, return the list
        of entities designated by that path.
        """
        path = path.split('.')
        assert path
        d = doc
        for key in path[:-1]:
            d = d.get(key, {})
        entities = d.get(path[-1], [])
        return entities

    def _drs_url(self, file):
        file_uuid = file['uuid']
        file_version = file['version']
        drs_url = drs.object_url(file_uuid, file_version)
        return drs_url

    def _dss_url(self, file):
        file_uuid = file['uuid']
        file_version = file['version']
        replica = 'gcp'
        path = f'files/{file_uuid}?version={file_version}&replica={replica}'
        dss_url = config.dss_endpoint + '/' + path
        return dss_url

    @cached_property
    def manifest_content_hash(self) -> int:
        logger.debug('Computing content hash for manifest using filters %r ...', self.filters)
        start_time = time.time()
        es_search = self._create_request()
        es_search.aggs.metric(
            'hash',
            'scripted_metric',
            init_script='''
                params._agg.fields = 0
            ''',
            map_script='''
                for (bundle in params._source.bundles) {
                    params._agg.fields += (bundle.uuid + bundle.version).hashCode()
                }
            ''',
            combine_script='''
                return params._agg.fields.hashCode()
            ''',
            reduce_script='''
                int result = 0;
                for (agg in params._aggs) {
                    result += agg
                }
                return result
          ''')
        es_search = es_search.extra(size=0)
        response = es_search.execute()
        assert len(response.hits) == 0
        hash_value = response.aggregations.hash.value
        logger.info('Manifest content hash %i was computed in %.3fs using filters %r.',
                    hash_value, time.time() - start_time, self.filters)
        return hash_value


class StreamingManifestGenerator(ManifestGenerator):
    """
    A manifest generator that writes its output to an IO stream.
    """

    @abstractmethod
    def write_to(self, output: IO[str]) -> Optional[str]:
        """
        Write the entire generator output to the given stream and return an
        optional string that should be used to name the output when persisting
        it to an object store or file system.

        :param output: the stream to write to
        """
        raise NotImplementedError()


class FileBasedManifestGenerator(ManifestGenerator):
    """
    A manifest generator that writes its output to a file.

    :return: the path to the file containing the output of the generator and an
             optional string that should be used to name the output when
             persisting it to an object store or another file system
    """

    @abstractmethod
    def create_file(self) -> Tuple[str, Optional[str]]:
        raise NotImplementedError()


class CompactManifestGenerator(StreamingManifestGenerator):

    @property
    def content_type(self) -> str:
        return 'text/tab-separated-values'

    @property
    def file_name_extension(self):
        return 'tsv'

    @property
    def entity_type(self) -> str:
        return 'files'

    @cached_property
    def source_filter(self) -> SourceFilters:
        return [
            *super().source_filter,
            'contents.files.related_files'
        ]

    def write_to(self, output: IO[str]) -> Optional[str]:
        sources = list(self.manifest_config.keys())
        ordered_column_names = [field_name
                                for source in sources
                                for field_name in self.manifest_config[source]]
        writer = csv.DictWriter(output, ordered_column_names, dialect='excel-tab')
        writer.writeheader()
        for hit in self._create_request().scan():
            doc = self._hit_to_doc(hit)
            assert isinstance(doc, dict)
            for bundle in list(doc['bundles']):  # iterate over copy …
                doc['bundles'] = [bundle]  # … to facilitate this in-place modification
                row = {}
                for doc_path, column_mapping in self.manifest_config.items():
                    entities = self._get_entities(doc_path, doc)
                    self._extract_fields(entities, column_mapping, row)
                writer.writerow(row)
                writer.writerows(self._get_related_rows(doc, row))
        return None

    def _get_related_rows(self, doc: dict, row: dict) -> Iterable[dict]:
        file_ = one(doc['contents']['files'])
        for related in file_['related_files']:
            new_row = row.copy()
            new_row.update({'file_' + k: v for k, v in related.items()})
            yield new_row


class FullManifestGenerator(StreamingManifestGenerator):

    @property
    def content_type(self) -> str:
        return 'text/tab-separated-values'

    @property
    def file_name_extension(self):
        return 'tsv'

    @property
    def entity_type(self) -> str:
        return 'bundles'

    @property
    def source_filter(self) -> SourceFilters:
        return ['contents.metadata.*']

    def write_to(self, output: IO[str]) -> Optional[str]:
        sources = list(self.manifest_config['contents'].keys())
        writer = csv.DictWriter(output, sources, dialect='excel-tab')
        writer.writeheader()
        project_short_names = set()

        # Setting 'size' to 500 prevents memory exhaustion in AWS Lambda.
        for hit in self._create_request().params(size=500).scan():
            doc = hit['contents'].to_dict()
            for metadata in list(doc['metadata']):
                if len(project_short_names) < 2:
                    project_short_names.add(metadata['project.project_core.project_short_name'])
                row = dict.fromkeys(sources)
                row.update(metadata)
                writer.writerow(row)
        return project_short_names.pop() if len(project_short_names) == 1 else None

    @cached_property
    def manifest_config(self) -> ManifestConfig:
        es_search = self._create_request()
        map_script = '''
                for (row in params._source.contents.metadata) {
                    for (f in row.keySet()) {
                        params._agg.fields.add(f);
                    }
                }
            '''
        reduce_script = '''
                Set fields = new HashSet();
                for (agg in params._aggs) {
                    fields.addAll(agg);
                }
                return new ArrayList(fields);
            '''
        es_search.aggs.metric('fields', 'scripted_metric',
                              init_script='params._agg.fields = new HashSet()',
                              map_script=map_script,
                              combine_script='return new ArrayList(params._agg.fields)',
                              reduce_script=reduce_script)
        es_search = es_search.extra(size=0)
        response = es_search.execute()
        assert len(response.hits) == 0
        return {
            'contents': {
                value: value.split('.')[-1]
                for value in sorted(response.aggregations.fields.value)
            }
        }


FQID = Tuple[str, str]
Qualifier = str

Group = Mapping[str, Cells]
Groups = List[Group]
Bundle = MutableMapping[Qualifier, Groups]
Bundles = MutableMapping[FQID, Bundle]


class BDBagManifestGenerator(FileBasedManifestGenerator):

    @property
    def file_name_extension(self) -> str:
        return 'zip'

    @property
    def content_type(self) -> str:
        return 'application/octet-stream'

    @property
    def entity_type(self) -> str:
        return 'files'

    @property
    def use_content_disposition_file_name(self) -> bool:
        # Apparently, Terra does not like the content disposition header
        return False

    @cached_property
    def manifest_config(self) -> ManifestConfig:
        return {
            path: {
                column_name.replace('.', self.column_path_separator): field_name
                for column_name, field_name in mapping.items()
            }
            for path, mapping in super().manifest_config.items()
        }

    def create_file(self) -> Tuple[str, Optional[str]]:
        with TemporaryDirectory() as temp_path:
            bag_path = os.path.join(temp_path, 'manifest')
            os.makedirs(bag_path)
            bdbag_api.make_bag(bag_path)
            with open(os.path.join(bag_path, 'data', 'participants.tsv'), 'w') as samples_tsv:
                self._samples_tsv(samples_tsv)
            bag = bdbag_api.make_bag(bag_path, update=True)  # update TSV checksums
            assert bdbag_api.is_bag(bag_path)
            bdbag_api.validate_bag(bag_path)
            assert bdbag_api.check_payload_consistency(bag)
            temp, temp_path = mkstemp()
            os.close(temp)
            archive_path = bdbag_api.archive_bag(bag_path, 'zip')
            # Moves the bdbag archive out of the temporary directory. This prevents
            # the archive from being deleted when the temporary directory self-destructs.
            os.rename(archive_path, temp_path)
            return temp_path, None

    column_path_separator = '__'

    @classmethod
    def _remove_redundant_entries(cls, bundles: Bundles) -> None:
        """
        Remove bundle entries from dict that are redundant based on the set of
        files it contains (eg. a primary bundle is made redundant by its derived
        analysis bundle if the primary only has a subset of files that the
        analysis bundle contains or if they both have the same files).
        """
        redundant_keys = set()
        # Get a forward mapping of bundle FQID to a set of file uuid
        bundle_to_file = defaultdict(set)
        for bundle_fqid, file_types in bundles.items():
            for groups in file_types.values():
                for group in groups:
                    bundle_to_file[bundle_fqid].add(group['file']['file_uuid'])
        # Get a reverse mapping of file uuid to set of bundle fqid
        file_to_bundle = defaultdict(set)
        for fqid, files in bundle_to_file.items():
            for file in files:
                file_to_bundle[file].add(fqid)
        # Find any file sets that are subset or equal to another
        for fqid_a, files_a in bundle_to_file.items():
            if fqid_a in redundant_keys:
                continue
            related_bundles = set(fqid_b for file in files_a for fqid_b in file_to_bundle[file]
                                  if fqid_b != fqid_a and fqid_b not in redundant_keys)
            for fqid_b in related_bundles:
                files_b = bundle_to_file[fqid_b]
                # If sets are equal remove the one with a lesser bundle version
                if files_a == files_b:
                    redundant_keys.add(fqid_a if fqid_a[1] < fqid_b[1] else fqid_b)
                    break
                # If set is a subset of another remove the subset
                elif files_a.issubset(files_b):
                    redundant_keys.add(fqid_a)
                    break
        # remove the redundant entries
        for fqid in redundant_keys:
            del bundles[fqid]

    def _samples_tsv(self, bundle_tsv: IO[str]) -> None:
        """
        Write `samples.tsv` to the given stream.
        """
        # The cast is safe because deepcopy makes a copy that we *can* modify
        other_column_mappings = cast(MutableManifestConfig, deepcopy(self.manifest_config))
        bundle_column_mapping = other_column_mappings.pop('bundles')
        file_column_mapping = other_column_mappings.pop('contents.files')

        bundles: Bundles = defaultdict(lambda: defaultdict(list))

        # For each outer file entity_type in the response …
        for hit in self._create_request().scan():
            doc = self._hit_to_doc(hit)

            # Extract fields from inner entities other than bundles or files
            other_cells = {}
            for doc_path, column_mapping in other_column_mappings.items():
                entities = self._get_entities(doc_path, doc)
                self._extract_fields(entities, column_mapping, other_cells)

            # Extract fields from the sole inner file entity_type
            file = one(doc['contents']['files'])
            file_cells = dict(file_url=self._dss_url(file),
                              drs_url=self._drs_url(file))
            self._extract_fields([file], file_column_mapping, file_cells)

            # Determine the column qualifier. The qualifier will be used to
            # prefix the names of file-specific columns in the TSV
            qualifier: Qualifier = file['file_format']
            if qualifier in ('fastq.gz', 'fastq'):
                qualifier = f"fastq_{file['read_index']}"

            # For each bundle containing the current file …
            doc_bundle: JSON
            for doc_bundle in doc['bundles']:
                bundle_fqid: FQID = (doc_bundle['uuid'], doc_bundle['version'])

                bundle_cells = {'entity:participant_id': '.'.join(bundle_fqid)}
                self._extract_fields([doc_bundle], bundle_column_mapping, bundle_cells)

                # Register the three extracted sets of fields as a group for this bundle and qualifier
                group = {
                    'file': file_cells,
                    'bundle': bundle_cells,
                    'other': other_cells
                }
                bundles[bundle_fqid][qualifier].append(group)

        self._remove_redundant_entries(bundles)

        # Return a complete column name by adding a qualifier and optionally a
        # numeric index. The index is necessary to distinguish between more than
        # one file per file format
        def qualify(qualifier, column_name, index=None):
            if index is not None:
                qualifier = f"{qualifier}_{index}"
            return f"{self.column_path_separator}{qualifier}{self.column_path_separator}{column_name}"

        num_groups_per_qualifier = defaultdict(int)

        # Track the max number of groups for each qualifier in any bundle
        for bundle in bundles.values():
            for qualifier, groups in bundle.items():
                # Sort the groups by reversed file name. This essentially sorts
                # by file extension and any other more general suffixes
                # preceding the extension. It ensure that `patient1_qc.bam` and
                # `patient2_qc.bam` always end up in qualifier `bam[0]` while
                # `patient1_metric.bam` and `patient2_metric.bam` end up in
                # qualifier `bam[1]`.
                groups.sort(key=lambda group: group['file']['file_name'][::-1])
                if len(groups) > num_groups_per_qualifier[qualifier]:
                    num_groups_per_qualifier[qualifier] = len(groups)

        # Compute the column names in deterministic order, bundle_columns first
        # followed by other columns
        column_names = dict.fromkeys(chain(
            ['entity:participant_id'],
            bundle_column_mapping.keys(),
            *(column_mapping.keys() for column_mapping in other_column_mappings.values())))

        # Add file columns for each qualifier and group
        for qualifier, num_groups in sorted(num_groups_per_qualifier.items()):
            for index in range(num_groups):
                for column_name in chain(file_column_mapping.keys(), ('drs_url', 'file_url')):
                    index = None if num_groups == 1 else index
                    column_names[qualify(qualifier, column_name, index=index)] = None

        # Write the TSV header
        bundle_tsv_writer = csv.DictWriter(bundle_tsv, column_names, dialect='excel-tab')
        bundle_tsv_writer.writeheader()

        # Write the actual rows of the TSV
        for bundle in bundles.values():
            row = {}
            for qualifier, groups in bundle.items():
                for i, group in enumerate(groups):
                    for entity, cells in group.items():
                        if entity == 'bundle':
                            # The bundle-specific cells should be consistent across all files in a bundle
                            if row:
                                row.update(cells)
                            else:
                                assert cells.items() <= row.items()
                        elif entity == 'other':
                            # Cells from other entities need to be concatenated. Note that for fields that differ
                            # between the files in a bundle this algorithm retains the values but loses the
                            # association between each individual value and the respective file.
                            for column_name, cell_value in cells.items():
                                row.setdefault(column_name, set()).update(cell_value.split(self.column_joiner))
                        elif entity == 'file':
                            # Since file-specific cells are placed into qualified columns, no concatenation is necessary
                            index = None if num_groups_per_qualifier[qualifier] == 1 else i
                            row.update((qualify(qualifier, column_name, index=index), cell)
                                       for column_name, cell in cells.items())
                        else:
                            assert False
            # Join concatenated values using the joiner
            row = {k: self.column_joiner.join(sorted(v)) if isinstance(v, set) else v for k, v in row.items()}
            bundle_tsv_writer.writerow(row)
