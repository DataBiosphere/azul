from abc import (
    ABCMeta,
    abstractmethod,
)
import base64
from collections import (
    defaultdict,
)
from copy import (
    copy,
    deepcopy,
)
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
from inspect import (
    isabstract,
)
from io import (
    BytesIO,
    TextIOWrapper,
)
import itertools
from itertools import (
    chain,
)
import logging
from operator import (
    itemgetter,
)
import os
import re
import shlex
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
    Protocol,
    Tuple,
    Type,
    Union,
    cast,
)
import unicodedata
import uuid

import attr
from bdbag import (
    bdbag_api,
)
from elasticsearch_dsl import (
    Search,
)
from elasticsearch_dsl.response import (
    Hit,
)
from furl import (
    furl,
)
from more_itertools import (
    one,
)
from werkzeug.http import (
    parse_dict_header,
)

from azul import (
    CatalogName,
    RequirementError,
    cached_property,
    config,
)
from azul.auth import (
    Authentication,
)
from azul.indexer.document import (
    FieldTypes,
    null_str,
)
from azul.json_freeze import (
    freeze,
    sort_frozen,
)
from azul.plugins import (
    ColumnMapping,
    DocumentSlice,
    FieldGlobs,
    FieldPath,
    ManifestConfig,
    MutableManifestConfig,
    RepositoryPlugin,
)
from azul.service import (
    FileUrlFunc,
    Filters,
    avro_pfb,
)
from azul.service.buffer import (
    FlushableBuffer,
)
from azul.service.elasticsearch_service import (
    ElasticsearchService,
    Pagination,
    PaginationStage,
    ToDictStage,
)
from azul.service.storage_service import (
    AWS_S3_DEFAULT_MINIMUM_PART_SIZE,
    StorageService,
)
from azul.types import (
    JSON,
    JSONs,
    MutableJSON,
)
from azul.vendored.frozendict import (
    frozendict,
)

logger = logging.getLogger(__name__)


class ManifestFormat(Enum):
    compact = 'compact'
    terra_bdbag = 'terra.bdbag'
    terra_pfb = 'terra.pfb'
    curl = 'curl'


class ManifestUrlFunc(Protocol):

    def __call__(self,
                 *,
                 fetch: bool = True,
                 catalog: CatalogName,
                 format_: ManifestFormat,
                 **params: str) -> str: ...


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class Manifest:
    """
    Contains the details of a prepared manifest.
    """
    #: The URL of the manifest file.
    location: str

    #: True if an existing manifest was reused or False if a new manifest was
    #: generated.
    was_cached: bool

    #: The format of the manifest
    format_: ManifestFormat

    #: The catalog used to generate the manifest
    catalog: CatalogName

    #: The filters used to generate the manifest
    filters: Filters

    #: The object_key associated with the manifest
    object_key: str

    #: The proposed file name of the manifest when downloading it to a user's
    #: system
    file_name: str

    def to_json(self) -> JSON:
        return {
            'location': self.location,
            'was_cached': self.was_cached,
            'format_': self.format_.value,
            'catalog': self.catalog,
            'filters': self.filters.to_json(),
            'object_key': self.object_key,
            'file_name': self.file_name
        }

    @classmethod
    def from_json(cls, json: JSON) -> 'Manifest':
        return cls(location=json['location'],
                   was_cached=json['was_cached'],
                   format_=ManifestFormat(json['format_']),
                   catalog=json['catalog'],
                   filters=Filters.from_json(json['filters']),
                   object_key=json['object_key'],
                   file_name=json['file_name'])


def tuple_or_none(v):
    return v if v is None else tuple(v)


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class ManifestPartition:
    """
    A partial manifest. An instance of this class encapsulates the state that
    might need to be tracked while a manifest is populated, in increments of
    partitions, or even pages within partitions. The simplest of manifests
    consist of just one big partition that's not split into pages. These
    monolithic manifests come at a price: the size of the manifest must be no
    more than what fits into memory at once.
    """
    #: The 0-based index of the partition
    index: int

    #: True if this is the last partition
    is_last: bool

    #: The file name to use for a manifest that contains this partition. While
    #: this attribute may seem misplaced, the file name is derived from the
    #: contents of the ES hits that make up the manifest rows. If a manifest is
    #: partitioned, we need to track the state of that derivation somewhere.
    #: On the last partition, this attribute is not None and represents the file
    #: name to be used. On the other partitions this attribute may be None, if
    #: it isn't, it represents the base name, the manifest content-dependent
    #: portion of the file name. If all pages of all partitions yield the same
    #: base name, the file name on the last partition will incorporate the base
    #: name. Otherwise, a generic, content-independent file name will be used.
    file_name: Optional[str] = None

    #: The cached configuration of the manifest that contains this partition.
    #: Manifest generators whose `manifest_config` property is expensive should
    #: cache the returned value here for subsequent partitions to reuse.
    config: Optional[ManifestConfig] = None

    #: The ID of the S3 multi-part upload this partition is a part of. If a
    #: manifest consists of just one partition, this may be None, but it doesn't
    #: have to be.
    multipart_upload_id: Optional[str] = None

    #: The S3 ETag of each partition; the current one and all the ones before it
    part_etags: Optional[Tuple[str, ...]] = attr.ib(converter=tuple_or_none,
                                                    default=None)

    #: The index of the current page. The index is zero-based and global. For
    #: example, if the first partition contains five pages, the index of the
    #: first page in the second partition is 5. This is None for manifests whose
    #: partitions aren't split into pages.
    page_index: Optional[int] = None

    #: True if the current page is the last page of the entire manifest. This is
    #: None for manifests whose partitions aren't split into pages.
    is_last_page: Optional[bool] = None

    #: The `sort` value of the first hit of the current page in this partition,
    #: or None if there is no current page.
    search_after: Optional[Tuple[str, str]] = None

    @classmethod
    def from_json(cls, partition: JSON) -> 'ManifestPartition':
        return cls(**{
            k: tuple(v) if isinstance(v, list) else v
            for k, v in partition.items()
        })

    def to_json(self) -> MutableJSON:
        return attr.asdict(self)

    @classmethod
    def first(cls) -> 'ManifestPartition':
        return cls(index=0,
                   is_last=False)

    def with_config(self, config: ManifestConfig):
        return attr.evolve(self, config=config)

    def with_upload(self, multipart_upload_id) -> 'ManifestPartition':
        return attr.evolve(self,
                           multipart_upload_id=multipart_upload_id,
                           part_etags=())

    def first_page(self) -> 'ManifestPartition':
        assert self.index == 0, self
        return attr.evolve(self,
                           page_index=0,
                           is_last_page=False)

    def next_page(self,
                  file_name: Optional[str],
                  search_after: Tuple[str, str]
                  ) -> 'ManifestPartition':
        assert self.page_index is not None, self
        # If different pages yield different file names, use default file name
        if self.page_index > 0:
            if file_name != self.file_name:
                file_name = None
        return attr.evolve(self,
                           page_index=self.page_index + 1,
                           file_name=file_name,
                           search_after=search_after)

    def last_page(self):
        return attr.evolve(self, is_last_page=True)

    def next(self, part_etag: str) -> 'ManifestPartition':
        return attr.evolve(self,
                           index=self.index + 1,
                           part_etags=(*self.part_etags, part_etag))

    def last(self, file_name: str) -> 'ManifestPartition':
        return attr.evolve(self,
                           file_name=file_name,
                           is_last=True)


class CachedManifestNotFound(Exception):
    pass


class CachedManifestSourcesChanged(Exception):
    pass


class ManifestService(ElasticsearchService):

    def __init__(self, storage_service: StorageService, file_url_func: FileUrlFunc):
        super().__init__()
        self.storage_service = storage_service
        self.file_url_func = file_url_func

    def get_manifest(self,
                     *,
                     format_: ManifestFormat,
                     catalog: CatalogName,
                     filters: Filters,
                     partition: ManifestPartition,
                     authentication: Optional[Authentication],
                     object_key: Optional[str] = None
                     ) -> Union[Manifest, ManifestPartition]:
        """
        Return a fully populated manifest that ends with the given partition or
        the next partition if the given partition isn't the last.

        If a manifest is returned, its 'location' attribute contains the
        pre-signed URL of a manifest in the given format, and containing file
        entities matching the given filter.

        If a suitable manifest already exists, it will be used and returned
        immediately. Otherwise, a new manifest will be generated. Subsequent
        invocations of this method with the same arguments are likely to reuse
        that manifest, skipping the time-consuming manifest generation.

        If a manifest needs to be generated and the generation involves multiple
        partitions, this method will only generate one partition and return
        the next one. Repeat calling this method with the returned partition
        until the return value is a Manifest instance.

        :param format_: The desired format of the manifest.

        :param catalog: The name of the catalog to generate the manifest from.

        :param filters: The filters by which to restrict the contents of the
                        manifest.

        :param partition: The manifest partition to generate. Not all manifests
                          involve multiple partitions. If they don't, a Manifest
                          instance will be returned. Otherwise, the next
                          ManifestPartition instance will be returned.

        :param authentication: The authentication accompanying the manifest
                               request

        :param object_key: An optional S3 object key of the cached manifest. If
                           None, the key will be computed dynamically. This may
                           take a few seconds. If a valid cached manifest exists
                           with the given key, it will be used. Otherwise, a new
                           manifest will be created and stored at the given key.
        """
        generator = ManifestGenerator.for_format(format_=format_,
                                                 service=self,
                                                 catalog=catalog,
                                                 filters=filters,
                                                 authentication=authentication)
        if object_key is None:
            object_key = generator.compute_object_key()
        file_name = self._get_cached_manifest_file_name(generator, object_key)
        if file_name is None:
            partition = generator.write(object_key, partition)
            if partition.is_last:
                file_name = partition.file_name
                was_cached = False
            else:
                return partition
        else:
            was_cached = True
        presigned_url = self._presign_url(generator, object_key, file_name)
        return Manifest(location=presigned_url,
                        was_cached=was_cached,
                        format_=format_,
                        catalog=catalog,
                        filters=filters,
                        object_key=object_key,
                        file_name=file_name)

    def _presign_url(self,
                     generator: 'ManifestGenerator',
                     object_key: str,
                     file_name: Optional[str]) -> str:
        if not generator.use_content_disposition_file_name:
            file_name = None
        return self.storage_service.get_presigned_url(object_key,
                                                      file_name=file_name)

    def get_cached_manifest(self,
                            format_: ManifestFormat,
                            catalog: CatalogName,
                            filters: Filters,
                            authentication: Optional[Authentication]
                            ) -> Tuple[str, Optional[Manifest]]:
        generator = ManifestGenerator.for_format(format_,
                                                 self,
                                                 catalog,
                                                 filters,
                                                 authentication)
        object_key = generator.compute_object_key()
        file_name = self._get_cached_manifest_file_name(generator, object_key)
        if file_name is None:
            return object_key, None
        else:
            presigned_url = self._presign_url(generator, object_key, file_name)
            return object_key, Manifest(location=presigned_url,
                                        was_cached=True,
                                        format_=format_,
                                        catalog=catalog,
                                        filters=filters,
                                        object_key=object_key,
                                        file_name=file_name)

    def get_cached_manifest_with_object_key(self,
                                            format_: ManifestFormat,
                                            catalog: CatalogName,
                                            filters: Filters,
                                            object_key: str,
                                            authentication: Optional[Authentication]
                                            ) -> Manifest:
        generator = ManifestGenerator.for_format(format_,
                                                 self,
                                                 catalog,
                                                 filters,
                                                 authentication)
        # FIXME: Add support for long-lived API tokens
        #        https://github.com/DataBiosphere/azul/issues/3328
        if False:
            current_source_key = generator.compute_source_key()
            # FIXME: Consolidate parsing of manifest object key
            #        https://github.com/DataBiosphere/azul/issues/4050
            manifest_key, source_key, extension = object_key.rsplit('/', 1)[-1].split('.')
            if source_key != current_source_key:
                raise CachedManifestSourcesChanged
        file_name = self._get_cached_manifest_file_name(generator, object_key)
        if file_name is None:
            raise CachedManifestNotFound
        else:
            presigned_url = self._presign_url(generator, object_key, file_name)
            return Manifest(location=presigned_url,
                            was_cached=True,
                            format_=format_,
                            catalog=catalog,
                            filters=filters,
                            object_key=object_key,
                            file_name=file_name)

    file_name_tag = 'azul_file_name'

    def _get_cached_manifest_file_name(self,
                                       generator: 'ManifestGenerator',
                                       object_key: str
                                       ) -> Optional[str]:
        """
        Return the proposed local file name of the manifest with the given
        object key if it was previously created, still exists in the bucket, and
        won't be expiring soon. Otherwise return None.

        :param generator: The generator of the manifest
        :param object_key: The object key of the cached manifest
        """
        try:
            response = self.storage_service.head(object_key)
        except self.storage_service.client.exceptions.ClientError as e:
            if int(e.response['Error']['Code']) == 404:
                logger.info('Cached manifest not found: %s', object_key)
                return None
            else:
                raise e
        else:
            seconds_until_expire = self._get_seconds_until_expire(response)
            if seconds_until_expire > config.manifest_expiration_margin:
                tagging = self.storage_service.get_object_tagging(object_key)
                try:
                    encoded_file_name = tagging[self.file_name_tag]
                except KeyError:
                    # FIXME: Can't be absent under S3's strong consistency
                    #        https://github.com/DataBiosphere/azul/issues/3255
                    logger.warning('Manifest object %r does not have the %r tag.',
                                   object_key, self.file_name_tag)
                    return generator.file_name(object_key, base_name=None)
                else:
                    encoded_file_name = encoded_file_name.encode('ascii')
                    return base64.urlsafe_b64decode(encoded_file_name).decode('utf-8')
            else:
                logger.info('Cached manifest is about to expire: %s', object_key)
                return None

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
        # Verify the 'Expiration' value is what is expected given the
        # 'LastModified' value, the number of days before expiration, and that
        # AWS rounds the expiration up to midnight UTC.
        last_modified = head_response['LastModified']
        last_modified_floor = last_modified.replace(hour=0,
                                                    minute=0,
                                                    second=0,
                                                    microsecond=0)
        expiration_in_days = config.manifest_expiration
        if not last_modified == last_modified_floor:
            expiration_in_days += 1
        expected_date = last_modified_floor + timedelta(days=expiration_in_days)
        if expiry_datetime == expected_date:
            logger.debug('Manifest object expires in %s seconds, on %s', expiry_seconds, expiry_datetime)
        else:
            logger.error('The actual object expiration (%s) does not match expected value (%s)',
                         expiration, expected_date)
        return expiry_seconds

    def command_lines(self,
                      manifest: Optional[Manifest],
                      url: str,
                      authentication: Optional[Authentication]
                      ) -> Optional[JSON]:
        format = None if manifest is None else manifest.format_
        generator_cls = ManifestGenerator.cls_for_format(format)
        file_name = None if manifest is None else manifest.file_name
        return generator_cls.command_lines(url, file_name, authentication)


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

    @classmethod
    @abstractmethod
    def format(cls) -> ManifestFormat:
        """
        Returns the manifest format implemented by this generator class.
        """
        raise NotImplementedError

    @cached_property
    def repository_plugin(self) -> RepositoryPlugin:
        catalog = self.catalog
        return RepositoryPlugin.load(catalog).create(catalog)

    @property
    @abstractmethod
    def file_name_extension(self) -> str:
        """
        The file name extension to use when persisting the output of this
        generator to a file system or an object store.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def content_type(self) -> str:
        """
        The MIME type to use when describing the output of this generator.
        """
        raise NotImplementedError

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
        raise NotImplementedError

    @cached_property
    def manifest_config(self) -> ManifestConfig:
        """
        The manifest config this generator uses. A manifest config is a mapping
        from document properties to manifest fields.
        """
        return self.service.metadata_plugin(self.catalog).manifest

    @cached_property
    def field_globs(self) -> FieldGlobs:
        """
        A list of field paths or path patterns to be included when requesting
        entity documents from the index.

        https://www.elastic.co/guide/en/elasticsearch/reference/7.10/search-fields.html#source-filtering
        """
        return [
            '.'.join(chain(field_path, (field_name,)))
            for field_path, column_mapping in self.manifest_config.items()
            for field_name in column_mapping.keys()
        ]

    @classmethod
    def for_format(cls,
                   format_: ManifestFormat,
                   service: ManifestService,
                   catalog: CatalogName,
                   filters: Filters,
                   authentication: Optional[Authentication]) -> 'ManifestGenerator':
        """
        Return a generator instance for the given format and filters.

        :param format_: format specifying which generator to use

        :param catalog: the name of the catalog to use when querying the index
                        for the documents to be transformed into the manifest

        :param filters: the filter to use when querying the index for the
                        documents to be transformed into the manifest

        :param service: the service to use when querying the index

        :param authentication: the authentication accompanying the manifest
                               request

        :return: a ManifestGenerator instance. Note that the protocol used for
                 consuming the generator output is defined in subclasses.
        """
        sub_cls = cls._cls_for_format[format_]
        return sub_cls(service, catalog, filters, authentication)

    _cls_for_format: MutableMapping[ManifestFormat, Type['ManifestGenerator']] = {}

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        if not isabstract(cls):
            format = cls.format()
            assert format not in cls._cls_for_format
            cls._cls_for_format[format] = cls

    @classmethod
    def cls_for_format(cls,
                       format: Optional[ManifestFormat]
                       ) -> Type['ManifestGenerator']:
        if format is None:
            return cls
        else:
            return cls._cls_for_format[format]

    @classmethod
    def _cmd_exe_quote(cls, s: str) -> str:
        """
        Escape a string for insertion into a `cmd.exe` command line
        """
        assert '"' not in s, s
        assert '\\' not in s, s
        return f'"{s}"'

    @classmethod
    def command_lines(cls,
                      url: str,
                      file_name: Optional[str],
                      authentication: Optional[Authentication]
                      ) -> JSON:
        # Normally we would have used --remote-name and --remote-header-name
        # which gets the file name from the content-disposition header. However,
        # URLs longer than 255 characters trigger a bug in curl.exe's
        # implementation of --remote-name on Windows. This is especially
        # surprising because --remote-name doesn't need to parse the URL when
        # --remote-header-name is also passed. To circumvent the URL parsing
        # bug we provide the file name explicitly with --output.

        # Normally, curl writes the response body and returns 0 (success),
        # even on server errors. With --fail, it writes an error message
        # containing the HTTP status code and exits with 22 in those cases.
        def options(quote_func):
            return [] if file_name is None else [
                '--location',
                '--fail',
                '--output',
                quote_func(file_name)
            ]

        return {
            'cmd.exe': ' '.join([
                'curl.exe',
                *options(cls._cmd_exe_quote),
                cls._cmd_exe_quote(url)
            ]),
            'bash': ' '.join([
                'curl',
                *options(shlex.quote),
                shlex.quote(url)
            ])
        }

    def __init__(self,
                 service: ManifestService,
                 catalog: CatalogName,
                 filters: Filters,
                 authentication: Optional[Authentication]
                 ) -> None:
        super().__init__()
        self.service = service
        self.catalog = catalog
        self.filters = filters
        self.file_url_func = service.file_url_func
        self.authentication = authentication

    def compute_object_key(self) -> str:
        """
        Return a manifest object key deterministically derived from this
        generator's parameters (its concrete type and the arguments passed to
        its constructor) and the current commit hash. The same parameters will
        always produce the same return value in one revision of this code.
        Different parameters should, with a very high probability, produce
        different return values.
        """
        git_commit = config.lambda_git_status['commit']
        manifest_namespace = uuid.UUID('ca1df635-b42c-4671-9322-b0a7209f0235')
        filter_string = repr(sort_frozen(freeze(self.filters.explicit)))
        content_hash = str(self.manifest_content_hash)
        manifest_key_params = (
            git_commit,
            self.catalog,
            self.format().value,
            content_hash,
            filter_string
        )
        assert not any(',' in param for param in manifest_key_params[:-1])
        manifest_key = str(uuid.uuid5(manifest_namespace, ','.join(manifest_key_params)))
        source_key = self.compute_source_key()
        for part in manifest_key, source_key:
            assert '.' not in part, part
        object_key = f'manifests/{manifest_key}.{source_key}.{self.file_name_extension}'
        return object_key

    source_namespace = uuid.UUID('6540b139-ea49-4e36-8f19-17c309b5fa76')

    def compute_source_key(self) -> str:
        source_ids = sorted(self.filters.source_ids)
        joiner = ','
        assert not any(joiner in source_id for source_id in source_ids), source_ids
        return str(uuid.uuid5(self.source_namespace, joiner.join(source_ids)))

    def _create_request(self) -> Search:
        pipeline = self._create_pipeline()
        request = self.service.create_request(self.catalog, self.entity_type)
        request = pipeline.prepare_request(request)
        # The response is processed by the generator, not the pipeline
        return request

    def _create_pipeline(self):
        document_slice = DocumentSlice(includes=self.field_globs)
        pipeline = self.service.create_chain(catalog=self.catalog,
                                             entity_type=self.entity_type,
                                             filters=self.filters,
                                             post_filter=False,
                                             document_slice=document_slice)
        return pipeline

    def _hit_to_doc(self, hit: Hit) -> MutableJSON:
        return self.service.translate_fields(self.catalog, hit.to_dict(), forward=False)

    column_joiner = ' || '

    @cached_property
    def _field_types(self) -> FieldTypes:
        return self.service.field_types(self.catalog)

    def _extract_fields(self,
                        *,
                        field_path: FieldPath,
                        entities: List[JSON],
                        column_mapping: ColumnMapping,
                        row: Cells) -> None:
        """
        Extract columns in `column_mapping` from `entities` and insert values
        into `row`.
        """
        field_types = self._field_types
        for field in field_path:
            field_types = field_types[field]

        stripped_joiner = self.column_joiner.strip()

        def convert(field_name, field_value):
            try:
                field_type = field_types[field_name]
            except KeyError:
                if field_name == 'file_url':
                    field_type = null_str
                else:
                    raise
            else:
                if field_name == 'drs_path':
                    field_value = self.repository_plugin.drs_uri(field_value)
                    field_type = null_str
                elif isinstance(field_type, list):
                    field_type = one(field_type)
            return field_type.to_tsv(field_value)

        def validate(field_value: str) -> str:
            # FIXME: Re-enable, once indexer rejects joiners in metadata
            #        https://github.com/DataBiosphere/azul/issues/3911
            if False:
                assert stripped_joiner not in field_value
            return field_value

        for field_name, column_name in column_mapping.items():
            assert column_name not in row, f'Column mapping defines {column_name} twice'
            column_value = []
            for entity in entities:
                try:
                    field_value = entity[field_name]
                except KeyError:
                    pass
                else:
                    if isinstance(field_value, list):
                        column_value += [
                            validate(convert(field_name, field_sub_value))
                            for field_sub_value in field_value
                            if field_sub_value is not None
                        ]
                    else:
                        column_value.append(validate(convert(field_name, field_value)))
            # FIXME: The slice is a hotfix. Reconsider.
            #        https://github.com/DataBiosphere/azul/issues/2649
            column_value = self.column_joiner.join(sorted(set(column_value))[:100])
            row[column_name] = column_value

    def _get_entities(self, field_path: FieldPath, doc: JSON) -> List[JSON]:
        """
        Given a document and a dotted path into that document, return the list
        of entities designated by that path.
        """
        assert field_path, field_path
        d = doc
        for key in field_path[:-1]:
            d = d.get(key, {})
        entities = d.get(field_path[-1], [])
        return entities

    def _repository_file_url(self, file: JSON) -> Optional[str]:
        replica = 'gcp'  # BDBag is for Terra and Terra is GCP
        return self.repository_plugin.direct_file_url(file_uuid=file['uuid'],
                                                      file_version=file['version'],
                                                      replica=replica)

    def _azul_file_url(self, file: JSON, args: Mapping = frozendict()) -> str:
        return self.file_url_func(catalog=self.catalog,
                                  file_uuid=file['uuid'],
                                  version=file['version'],
                                  fetch=False,
                                  **args)

    @cached_property
    def manifest_content_hash(self) -> int:
        logger.debug('Computing content hash for manifest using filters %r ...', self.filters)
        start_time = time.time()
        request = self._create_request()
        request.aggs.metric(
            'hash',
            'scripted_metric',
            init_script='''
                state.fields = 0
            ''',
            map_script='''
                for (bundle in params._source.bundles) {
                    state.fields += (bundle.uuid + bundle.version).hashCode()
                }
            ''',
            combine_script='''
                return state.fields.hashCode()
            ''',
            reduce_script='''
                int result = 0;
                for (state in states) {
                    result += state
                }
                return result
          ''')
        request = request.extra(size=0)
        response = request.execute()
        assert len(response.hits) == 0
        hash_value = response.aggregations.hash.value
        logger.info('Manifest content hash %i was computed in %.3fs using filters %r.',
                    hash_value, time.time() - start_time, self.filters)
        return hash_value

    def file_name(self, object_key, base_name: Optional[str] = None) -> str:
        if base_name:
            file_name_prefix = unicodedata.normalize('NFKD', base_name)
            file_name_prefix = re.sub(r'[^\w ,.@%&\-_()\\[\]/{}]', '_', file_name_prefix).strip()
            timestamp = datetime.now().strftime("%Y-%m-%d %H.%M")
            file_name = f'{file_name_prefix} {timestamp}.{self.file_name_extension}'
        else:
            # FIXME: Consolidate parsing of manifest object key
            #        https://github.com/DataBiosphere/azul/issues/4050
            file_name = 'hca-manifest-' + object_key.rsplit('/', )[-1]
        return file_name

    def tagging(self, file_name: Optional[str]) -> Optional[Mapping[str, str]]:
        if file_name is None:
            return None
        else:
            encoded_file_name = base64.urlsafe_b64encode(file_name.encode('utf-8'))
            return {self.service.file_name_tag: encoded_file_name.decode('ascii')}

    @abstractmethod
    def write(self,
              object_key: str,
              partition: ManifestPartition,
              ) -> ManifestPartition:
        """
        Write the given partition of the manifest to the specified object in S3
        storage and return the next partition to be written. Unless the returned
        partition is the last one, this method will soon be invoked again,
        passing the partition returned by the previous invocation.

        A minimal implementation of this method would write the entire manifest
        in just one large partition and return that partition with the is_last
        flag set.

        :param object_key: The S3 object key under which to store the manifest
                           partition.

        :param partition: The partition to write.
        """
        raise NotImplementedError

    @property
    def storage(self):
        return self.service.storage_service


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
        raise NotImplementedError

    def write(self,
              object_key: str,
              partition: ManifestPartition,
              ) -> ManifestPartition:
        assert partition.index == 0 and partition.page_index is None, partition
        with self.storage.put_multipart(object_key, content_type=self.content_type) as upload:
            with FlushableBuffer(AWS_S3_DEFAULT_MINIMUM_PART_SIZE, upload.push) as buffer:
                text_buffer = TextIOWrapper(buffer, encoding='utf-8', write_through=True)
                base_name = self.write_to(text_buffer)
        file_name = self.file_name(object_key, base_name)
        tagging = self.tagging(file_name)
        if tagging is not None:
            self.storage.put_object_tagging(object_key, tagging)
        return partition.last(file_name)


class PagedManifestGenerator(ManifestGenerator):
    """
    A manifest generator whose output can be split over multiple concatenable
    IO streams.
    """

    @abstractmethod
    def write_page_to(self,
                      partition: ManifestPartition,
                      output: IO[str]
                      ) -> ManifestPartition:
        """
        Write the generator output for the current page of the given partition
        to the given stream and return an updated partition object that
        represents the next page of the given partition.

        :param partition: the current partition

        :param output: the stream to write to
        """
        raise NotImplementedError

    # With the minimum part size of 5 MiB I've observed a running time of only
    # 5s per partition so to minimize step function churn we'll go with 50 MiB
    # instead.

    part_size = 50 * 1024 * 1024

    assert part_size >= AWS_S3_DEFAULT_MINIMUM_PART_SIZE

    def write(self,
              object_key: str,
              partition: ManifestPartition,
              ) -> ManifestPartition:
        assert not partition.is_last, partition
        if partition.config is None:
            partition = partition.with_config(self.manifest_config)
        else:
            type(self).manifest_config.fset(self, partition.config)
        if partition.multipart_upload_id is None:
            upload = self.storage.create_multipart_upload(object_key)
            partition = partition.with_upload(upload.id)
        else:
            upload = self.storage.load_multipart_upload(object_key=object_key,
                                                        upload_id=partition.multipart_upload_id)
        if partition.page_index is None:
            partition = partition.first_page()
        buffer = BytesIO()
        text_buffer = TextIOWrapper(buffer, encoding='utf-8', write_through=True)
        while True:
            partition = self.write_page_to(partition, output=text_buffer)
            if partition.is_last_page or buffer.tell() > self.part_size:
                break

        def upload_part():
            buffer.seek(0)
            return self.storage.upload_multipart_part(buffer, partition.index + 1, upload)

        if partition.is_last_page:
            if buffer.tell() > 0:
                partition = partition.next(part_etag=upload_part())
            self.storage.complete_multipart_upload(upload, partition.part_etags)
            file_name = self.file_name(object_key, partition.file_name)
            tagging = self.tagging(file_name)
            if tagging is not None:
                self.storage.put_object_tagging(object_key, tagging)
            return partition.last(file_name)
        else:
            return partition.next(part_etag=upload_part())

    page_size = 500

    def _create_paged_request(self, partition: ManifestPartition) -> Search:
        pagination = Pagination(sort='entryId',
                                order='asc',
                                size=self.page_size,
                                search_after=partition.search_after)
        pipeline = self._create_pipeline()
        # Only needs this to satisfy the type constraints
        pipeline = ToDictStage(service=self.service,
                               catalog=self.catalog,
                               entity_type=self.entity_type).wrap(pipeline)
        pipeline = PaginationStage(service=self.service,
                                   catalog=self.catalog,
                                   entity_type=self.entity_type,
                                   pagination=pagination,
                                   filters=self.filters,
                                   peek_ahead=False).wrap(pipeline)
        request = self.service.create_request(catalog=self.catalog,
                                              entity_type=self.entity_type)
        # The response is processed by the generator, not the pipeline
        request = pipeline.prepare_request(request)
        return request


class FileBasedManifestGenerator(ManifestGenerator):
    """
    A manifest generator that writes its output to a file.

    :return: the path to the file containing the output of the generator and an
             optional string that should be used to name the output when
             persisting it to an object store or another file system
    """

    @abstractmethod
    def create_file(self) -> Tuple[str, Optional[str]]:
        raise NotImplementedError

    def write(self,
              object_key: str,
              partition: ManifestPartition,
              ) -> ManifestPartition:
        """
        Generate the manifest and return the desired content disposition file
        name if necessary.
        """
        assert partition.index == 0 and partition.page_index is None, partition
        file_path, base_name = self.create_file()
        file_name = self.file_name(object_key, base_name)
        try:
            self.storage.upload(file_path,
                                object_key,
                                content_type=self.content_type,
                                tagging=self.tagging(file_name))
        finally:
            os.remove(file_path)
        partition = partition.last(file_name)
        return partition


class CurlManifestGenerator(PagedManifestGenerator):

    @classmethod
    def format(cls) -> ManifestFormat:
        return ManifestFormat.curl

    @property
    def content_type(self) -> str:
        return 'text/plain'

    @property
    def file_name_extension(self):
        return 'curlrc'

    @property
    def entity_type(self) -> str:
        return 'files'

    @cached_property
    def field_globs(self) -> FieldGlobs:
        return [
            *super().field_globs,
            'contents.files.related_files'
        ]

    @classmethod
    def command_lines(cls,
                      url: str,
                      file_name: Optional[str],
                      authentication: Optional[Authentication]
                      ) -> JSON:
        authentication_option = [] if authentication is None else [
            '--header',
            cls._option(authentication.as_http_header())
        ]
        manifest_options = [
            '--location',
            '--fail',
            # The non-fetch endpoint provides a pre-authenticated signed S3 URL
            *(
                authentication_option
                if furl(url).netloc == furl(config.service_endpoint()).netloc
                else ()
            )
        ]
        return {
            'cmd.exe': ' '.join([
                'curl.exe',
                *manifest_options,
                cls._cmd_exe_quote(url),
                '|',
                'curl.exe',
                *authentication_option,
                '--config',
                '-'
            ]),
            'bash': ' '.join([
                'curl',
                *manifest_options,
                shlex.quote(url),
                '|',
                'curl',
                *authentication_option,
                '--config',
                '-'
            ])
        }

    @classmethod
    def _option(cls, s: str):
        """
        >>> f = CurlManifestGenerator._option
        >>> f('')
        '""'

        >>> f('abc')
        '"abc"'

        >>> list(map(ord, f('"')))
        [34, 92, 34, 34]

        >>> list(map(ord, f(f('"'))))
        [34, 92, 34, 92, 92, 92, 34, 92, 34, 34]

        """
        return '"' + s.replace('\\', '\\\\').replace('"', '\\"') + '"'

    def write_page_to(self,
                      partition: ManifestPartition,
                      output: IO[str]
                      ) -> ManifestPartition:

        def _write(file: JSON, is_related_file: bool = False):
            name = file['name']
            # Related files are indexed differently than normal files (they
            # don't have their own document but are listed inside the main
            # file's document), so to ensure that the /repository/files
            # endpoint can resolve them correctly, their endpoint URLs
            # contain additional parameters, so that the endpoint does not
            # need to query the index for that information.
            args = {
                'requestIndex': 1,
                'fileName': name,
                'drsPath': file['drs_path']
            } if is_related_file else {}

            url = self._azul_file_url(file, args)
            # To prevent overwriting one file with another one of the same name
            # but different content we nest each file in a folder using the
            # bundle UUID. Because a file can belong to multiple bundles we use
            # the one with the most recent version.
            bundle = max(cast(JSONs, doc['bundles']), key=itemgetter('version', 'uuid'))
            output_name = self._sanitize_path(bundle['uuid'] + '/' + name)
            output.write(f'url={self._option(url)}\n'
                         f'output={self._option(output_name)}\n\n')

        if partition.page_index == 0:
            curl_options = [
                '--create-dirs',  # Allow curl to create folders
                '--compressed',  # Request a compressed response
                '--location',  # Follow redirects
                '--globoff',  # Prevent '#' in file names from being interpreted as output variables
                '--fail',  # Upon server error don't save the error message to the file
                '--fail-early',  # Exit curl with error on the first failure encountered
                '--continue-at -',  # Resume partially downloaded files
                '--write-out "Downloading to: %{filename_effective}\\n\\n"'
            ]
            output.write('\n\n'.join(curl_options))
            output.write('\n\n')

        request = self._create_paged_request(partition)
        response = request.execute()
        if response.hits:
            hit = None
            for hit in response.hits:
                doc = self._hit_to_doc(hit)
                file = one(doc['contents']['files'])
                _write(file)
                for related_file in file['related_files']:
                    _write(related_file, is_related_file=True)
            assert hit is not None
            search_after = tuple(hit.meta.sort)
            return partition.next_page(file_name=None,
                                       search_after=search_after)
        else:
            return partition.last_page()

    # Disallow control characters and backslash as they likely indicate an
    # injection attack. No useful file name should contain them
    #
    _malicious_chars = re.compile(r'[\x00-\x1f\\]')

    # Benign occurrences of potentially problematic characters
    #
    _problematic_chars = re.compile(r'[<>:"|?*]')

    # Disallow slashes anywhere in a path component. Allow a single dot at the
    # beginning as long as it's followed by a something other than space or dot.
    # Disallow space or dot at the end. Within the path component (anywhere but
    # the beginning or end), dots and spaces are allowed, even consecutive ones
    #
    _valid_path_component = r'\.?[^./ ]([^/]*[^./ ])?'

    # Allow single slashes between path components
    #
    _valid_path = re.compile(rf'{_valid_path_component}(/{_valid_path_component})*')

    # Reject path components that are special on Windows, courtesy of DOS
    #
    special_dos_files = {
        'CON', 'PRN', 'AUX', 'NUL',
        *(f'{cmd}{i}' for cmd in ['COM', 'LPT'] for i in range(1, 10))
    }

    @classmethod
    def _sanitize_path(cls, path: str) -> str:
        """
        >>> f = CurlManifestGenerator._sanitize_path
        >>> f('foo/bar/\\x1F/file') # doctest: +NORMALIZE_WHITESPACE
        Traceback (most recent call last):
        ...
        azul.RequirementError: ('Invalid file path', 'foo/bar/\\x1f/file',
                                'Control character or backslash at position', 8)

        >>> f('foo/bar/COM6/file') # doctest: +NORMALIZE_WHITESPACE
        Traceback (most recent call last):
        ...
        azul.RequirementError: ('Invalid file path', 'foo/bar/COM6/file',
                                'Use of reserved path component for Windows', {'COM6'})

        >>> f('foo/bar/ / baz/file') # doctest: +NORMALIZE_WHITESPACE
        Traceback (most recent call last):
        ...
        azul.RequirementError: ('Invalid file path', 'foo/bar/ / baz/file')

        Substitutions:

        >>> f('<>:"|?*<>:"|?*')
        '______________'

        Pass-through:

        >>> f('foo/bar/file.fastq.gz')
        'foo/bar/file.fastq.gz'

        Invalid paths:

        >>> s: str  # work around false `Unresolved reference` warning by PyCharm

        >>> all(
        ...     CurlManifestGenerator._valid_path.fullmatch(s) is None
        ...     for s in ('', '.', '..', ' ', ' x', 'x ', 'x ', '/', 'x/', '/x', 'x//x')
        ... )
        True

        Valid paths:

        >>> all(
        ...     CurlManifestGenerator._valid_path.fullmatch(s) is not None
        ...     for s in ('x', '.x', '.x. y', 'x/x', '.x/.y')
        ... )
        True
        """
        match = cls._malicious_chars.search(path)
        if match is not None:
            raise RequirementError('Invalid file path', path,
                                   'Control character or backslash at position', match.start())

        path = cls._problematic_chars.sub('_', path)

        if cls._valid_path.fullmatch(path) is None:
            raise RequirementError('Invalid file path', path)

        components = set(path.split('/')) & cls.special_dos_files
        if components:
            raise RequirementError('Invalid file path', path,
                                   'Use of reserved path component for Windows', components)

        return path


class CompactManifestGenerator(PagedManifestGenerator):

    @classmethod
    def format(cls) -> ManifestFormat:
        return ManifestFormat.compact

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
    def field_globs(self) -> FieldGlobs:
        return [
            *super().field_globs,
            'contents.files.related_files'
        ]

    def write_page_to(self,
                      partition: ManifestPartition,
                      output: IO[str]
                      ) -> ManifestPartition:
        column_mappings = self.manifest_config.values()
        column_names = list(chain.from_iterable(map(dict.values, column_mappings)))
        writer = csv.DictWriter(output, column_names, dialect='excel-tab')

        if partition.page_index == 0:
            writer.writeheader()

        request = self._create_paged_request(partition)
        response = request.execute()
        if response.hits:
            project_short_names = set()
            hit = None
            for hit in response.hits:
                doc = self._hit_to_doc(hit)
                assert isinstance(doc, dict)
                if len(project_short_names) < 2:
                    project = one(doc['contents']['projects'])
                    short_names = project['project_short_name']
                    project_short_names.update(short_names)
                row = {}
                related_rows = []
                for field_path, column_mapping in self.manifest_config.items():
                    entities = self._get_entities(field_path, doc)
                    if field_path == ('contents', 'files'):
                        file = copy(one(entities))
                        file['file_url'] = self._azul_file_url(file)
                        entities = [file]
                    self._extract_fields(field_path=field_path,
                                         entities=entities,
                                         column_mapping=column_mapping,
                                         row=row)
                    if field_path == ('contents', 'files'):
                        file = copy(one(entities))
                        if 'related_files' in file:
                            field_path = (*field_path, 'related_files')
                            for related_file in file['related_files']:
                                related_row = {}
                                file.update(related_file)
                                file['file_url'] = self._azul_file_url(file)
                                self._extract_fields(field_path=field_path,
                                                     entities=[file],
                                                     column_mapping=column_mapping,
                                                     row=related_row)
                                related_rows.append(related_row)
                writer.writerow(row)
                for related in related_rows:
                    row.update(related)
                    writer.writerow(row)
            assert hit is not None
            search_after = tuple(hit.meta.sort)
            file_name = project_short_names.pop() if len(project_short_names) == 1 else None
            return partition.next_page(file_name=file_name,
                                       search_after=search_after)
        else:
            return partition.last_page()


FQID = Tuple[str, str]
Qualifier = str

Group = Mapping[str, Cells]
Groups = List[Group]
Bundle = MutableMapping[Qualifier, Groups]
Bundles = MutableMapping[FQID, Bundle]


class PFBManifestGenerator(FileBasedManifestGenerator):

    @classmethod
    def format(cls) -> ManifestFormat:
        return ManifestFormat.terra_pfb

    @property
    def file_name_extension(self) -> str:
        return 'avro'

    @property
    def content_type(self) -> str:
        return 'application/octet-stream'

    @property
    def entity_type(self) -> str:
        return 'files'

    @property
    def field_globs(self) -> FieldGlobs:
        """
        We want all of the metadata because then we can use the field_types()
        to generate the complete schema.
        """
        return []

    def _all_docs_sorted(self) -> Iterable[JSON]:
        request = self._create_request()
        request = request.params(preserve_order=True).sort('entity_id.keyword')
        for hit in request.scan():
            doc = self._hit_to_doc(hit)
            yield doc

    def create_file(self) -> Tuple[str, Optional[str]]:
        transformers = self.service.transformer_types(self.catalog)
        transformer = one(t for t in transformers if t.entity_type() == 'files')
        field_types = transformer.field_types()
        entity = avro_pfb.pfb_metadata_entity(field_types)
        pfb_schema = avro_pfb.pfb_schema_from_field_types(field_types)

        converter = avro_pfb.PFBConverter(pfb_schema, self.repository_plugin)
        for doc in self._all_docs_sorted():
            converter.add_doc(doc)

        entities = itertools.chain([entity], converter.entities())

        fd, path = mkstemp(suffix='.avro')
        os.close(fd)
        avro_pfb.write_pfb_entities(entities, pfb_schema, path)
        return path, None


class BDBagManifestGenerator(FileBasedManifestGenerator):

    @classmethod
    def format(cls) -> ManifestFormat:
        return ManifestFormat.terra_bdbag

    @property
    def file_name_extension(self) -> str:
        return 'zip'

    @property
    def content_type(self) -> str:
        return 'application/octet-stream'

    @property
    def entity_type(self) -> str:
        return 'files'

    @cached_property
    def field_globs(self) -> FieldGlobs:
        return [
            *super().field_globs,
            'contents.files.drs_path'
        ]

    @property
    def use_content_disposition_file_name(self) -> bool:
        # Apparently, Terra does not like the content disposition header
        return False

    @cached_property
    def manifest_config(self) -> ManifestConfig:
        return {
            field_path: {
                field_name: column_name.replace('.', self.column_path_separator)
                for field_name, column_name in column_mapping.items()
                if field_name != 'file_url'
            }
            for field_path, column_mapping in super().manifest_config.items()
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
        bundle_column_mapping = other_column_mappings.pop(('bundles',))
        file_column_mapping = other_column_mappings.pop(('contents', 'files'))

        bundles: Bundles = defaultdict(lambda: defaultdict(list))

        # For each outer file entity_type in the response …
        for hit in self._create_request().scan():
            doc = self._hit_to_doc(hit)
            # Extract fields from inner entities other than bundles or files
            other_cells = {}
            for field_path, column_mapping in other_column_mappings.items():
                entities = self._get_entities(field_path, doc)
                self._extract_fields(field_path=field_path,
                                     entities=entities,
                                     column_mapping=column_mapping,
                                     row=other_cells)

            # Extract fields from the sole inner file entity_type
            file = one(doc['contents']['files'])
            file_cells = dict(file_url=self._repository_file_url(file))
            self._extract_fields(field_path=('contents', 'files'),
                                 entities=[file],
                                 column_mapping=file_column_mapping,
                                 row=file_cells)

            # Determine the column qualifier. The qualifier will be used to
            # prefix the names of file-specific columns in the TSV
            qualifier: Qualifier = file['file_format']
            if qualifier in ('fastq.gz', 'fastq'):
                qualifier = f"fastq_{file['read_index']}"
            # Terra requires column headers only contain alphanumeric
            # characters, underscores, and dashes.
            # See https://github.com/DataBiosphere/azul/issues/2182
            qualifier = re.sub(r'[^A-Za-z0-9_-]', '-', qualifier)

            # For each bundle containing the current file …
            doc_bundle: JSON
            for doc_bundle in doc['bundles']:
                # Versions indexed by TDR contain ':', but Terra won't allow ':'
                # in the 'entity:participant_id' field
                bundle_fqid: FQID = doc_bundle['uuid'], doc_bundle['version'].replace(':', '')

                bundle_cells = {'entity:participant_id': '.'.join(bundle_fqid)}
                self._extract_fields(field_path=('bundles',),
                                     entities=[doc_bundle],
                                     column_mapping=bundle_column_mapping,
                                     row=bundle_cells)

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
            bundle_column_mapping.values(),
            *map(dict.values, other_column_mappings.values())))

        # Add file columns for each qualifier and group
        for qualifier, num_groups in sorted(num_groups_per_qualifier.items()):
            for index in range(num_groups):
                for column_name in chain(file_column_mapping.values(),
                                         ('file_drs_uri', 'file_url')):
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
