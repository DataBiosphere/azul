from abc import (
    ABCMeta,
    abstractmethod,
)
import base64
from bisect import (
    insort,
)
from collections.abc import (
    Iterable,
    Mapping,
    Sequence,
)
import csv
from datetime import (
    datetime,
)
from functools import (
    partial,
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
import json
import logging
from math import (
    ceil,
)
from operator import (
    itemgetter,
)
import os
import re
import shlex
from tempfile import (
    mkstemp,
)
import time
from typing import (
    Any,
    Callable,
    ClassVar,
    IO,
    Protocol,
    Self,
)
import unicodedata
from uuid import (
    UUID,
    uuid5,
)

import attrs
from furl import (
    furl,
)
from more_itertools import (
    always_iterable,
    chunked,
    one,
)
import msgpack
from opensearchpy import (
    Q,
    Search,
)
from opensearchpy.helpers.response import (
    Hit,
    Response,
)

from azul import (
    CatalogName,
    config,
)
from azul.auth import (
    Authentication,
)
from azul.deployment import (
    aws,
)
from azul.field_type import (
    FieldType,
    FieldTypes,
    null_str,
)
from azul.filters import (
    Filters,
    FiltersJSON,
)
from azul.indexer.document import (
    DocumentType,
    EntityType,
    FieldPath,
)
from azul.indexer.mirror_service import (
    MirrorService,
)
from azul.lib import (
    R,
    cache,
    cached_property,
    mutable_furl,
)
from azul.lib.attrs import (
    SerializableAttrs,
    is_uuid,
    serializable,
    serializable_uuid,
    strict_auto,
)
from azul.lib.bytes import (
    azul_urlsafe_b64decode,
    azul_urlsafe_b64encode,
)
from azul.lib.collections import (
    getitem,
)
from azul.lib.functions import (
    compose,
)
from azul.lib.json import (
    copy_json,
)
from azul.lib.strings import (
    double_quote as dq,
)
from azul.lib.types import (
    FlatJSON,
    JSON,
    JSONs,
    MutableJSON,
    json_dict,
    json_element_dicts,
    json_element_mappings,
    json_element_strings,
    json_elements_are_mappings,
    json_int,
    json_list_of_dicts,
    json_mapping,
    json_sequence,
    json_sequence_of_mappings,
    json_str,
    not_none,
    optional,
)
from azul.lib.uuids import (
    uuid5_for_bytes,
)
from azul.plugins import (
    ColumnMapping,
    DocumentSlice,
    ManifestConfig,
    ManifestFormat,
    MetadataPlugin,
    SpecialField,
    dotted,
    manifest_config_from_json,
    manifest_config_to_json,
)
from azul.service import (
    FileUrlFunc,
    avro_pfb,
)
from azul.service.avro_pfb import (
    PFBRelation,
)
from azul.service.query_service import (
    OpenSearchChain,
    Pagination,
    PaginationStage,
    QueryService,
    SortKey,
    ToDictStage,
    sort_key_from_json,
    sort_key_to_json,
)
from azul.service.storage_service import (
    StorageObjectNotFound,
    StorageService,
)
from azul.source import (
    Prefix,
    SourceRef,
)
from azul.vendored.frozendict import (
    frozendict,
)

log = logging.getLogger(__name__)


class ManifestUrlFunc(Protocol):

    def __call__(self,
                 *,
                 fetch: bool = True,
                 token_or_key: str | None = None,
                 **params: str
                 ) -> mutable_furl: ...


@attrs.frozen
class InvalidManifestKey(Exception):
    value: str


class AbstractManifestKey(metaclass=ABCMeta):
    """
    The root of the manifest key class hierarchy. The hierarchy expresses the
    basic security constraints on manifest keys as they are sent through
    potentially insecure channels. This class defines the methods for
    (de)serializing a manifest key using a somewhat space-efficient
    binary "packed" representation.
    """

    @abstractmethod
    def pack(self) -> bytes:
        raise NotImplementedError

    def encode(self) -> str:
        return azul_urlsafe_b64encode(self.pack())

    @classmethod
    @abstractmethod
    def unpack(cls, pack: bytes) -> Self:
        raise NotImplementedError

    @classmethod
    def decode(cls, value: str) -> Self:
        try:
            return cls.unpack(azul_urlsafe_b64decode(value))
        except Exception as e:
            raise InvalidManifestKey(value) from e


@attrs.frozen(kw_only=True)
class BareManifestKey(AbstractManifestKey, SerializableAttrs):
    """
    An untrusted manifest key. Instances can be freely serialized and
    deserialized but the service won't accept them. To obtain a key the service
    trusts, use an instance of :class:`ManifestKey` that was returned by the
    service.

    To send a manifest key through an an untrusted channel, it must first be
    signed using :meth:`ManifestService.verify_manifest_key_signature`. After
    reading it from the untrusted channel the signature must be verified using
    :meth:`ManifestService.verify_manifest_key_signature`.

    >>> manifest_key = BareManifestKey(catalog='foo',
    ...                                format=ManifestFormat.curl,
    ...                                manifest_hash=UUID('d2b0ce3c-46f0-57fe-b9d4-2e38d8934fd4'),
    ...                                source_hash=UUID('77936747-5968-588e-809f-af842d6be9e0'))

    >>> manifest_key.encode()
    'lKNmb2-kY3VybMQQ0rDOPEbwV_651C442JNP1MQQd5NnR1loWI6An6-ELWvp4A'

    The encode() method is the inverse of decode():

    >>> BareManifestKey.decode(manifest_key.encode()) == manifest_key
    True

    Invalid base64:

    >>> BareManifestKey.decode(manifest_key.encode()[:-1])
    ... # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    azul.service.manifest_service.InvalidManifestKey:
    lKNmb2-kY3VybMQQ0rDOPEbwV_651C442JNP1MQQd5NnR1loWI6An6-ELWvp4

    Valid base64 encoding and msgpack format, but value of wrong type for
    `catalog` atrribute

    >>> with attrs.validators.disabled():
    ...     # noinspection PyTypeChecker
    ...     bad_key = attrs.evolve(manifest_key, catalog=123).encode()
    >>> bad_key
    'lHukY3VybMQQ0rDOPEbwV_651C442JNP1MQQd5NnR1loWI6An6-ELWvp4A'

    >>> BareManifestKey.decode(bad_key)
    ... # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    azul.service.manifest_service.InvalidManifestKey:
    lHukY3VybMQQ0rDOPEbwV_651C442JNP1MQQd5NnR1loWI6An6-ELWvp4A

    >>> bad_key = base64.b64encode(manifest_key.pack() + b'123').decode()
    >>> BareManifestKey.decode(bad_key)
    ... # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    azul.service.manifest_service.InvalidManifestKey:
    lKNmb2+kY3VybMQQ0rDOPEbwV/651C442JNP1MQQd5NnR1loWI6An6+ELWvp4DEyMw==

    >>> bad_key = base64.b64encode(manifest_key.pack()[:-1]).decode()
    >>> BareManifestKey.decode(bad_key)
    ... # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    azul.service.manifest_service.InvalidManifestKey:
    lKNmb2+kY3VybMQQ0rDOPEbwV/651C442JNP1MQQd5NnR1loWI6An6+ELWvp

    Manifest keys contain the catalog name which can be quite long, extending
    the length of the encoded manifest key proportionally by 4 characters for
    every 3 catalog name characters.

    >>> manifest_key = BareManifestKey(catalog='a' * 64,
    ...                                format=ManifestFormat.terra_pfb,
    ...                                manifest_hash=UUID('d2b0ce3c-46f0-57fe-b9d4-2e38d8934fd4'),
    ...                                source_hash=UUID('77936747-5968-588e-809f-af842d6be9e0'))
    >>> len(manifest_key.encode())
    151
    """
    catalog: CatalogName = strict_auto()
    format: ManifestFormat = strict_auto()
    manifest_hash: UUID = serializable_uuid(attrs.field(validator=is_uuid(5)))
    source_hash: UUID = serializable_uuid(attrs.field(validator=is_uuid(5)))

    def pack(self) -> bytes:
        return msgpack.packb([
            self.catalog,
            self.format.value,
            self.manifest_hash.bytes,
            self.source_hash.bytes,
        ])

    @classmethod
    def unpack(cls, pack: bytes) -> Self:
        i = iter(msgpack.unpackb(pack))
        return cls(catalog=next(i),
                   format=ManifestFormat(next(i)),
                   manifest_hash=UUID(bytes=next(i)),
                   source_hash=UUID(bytes=next(i)))


@attrs.frozen(kw_only=True)
class SignedManifestKey(AbstractManifestKey):
    """
    A bare manifest key and its signature.

    >>> bare_manifest_key = BareManifestKey(catalog='foo',
    ...                                     format=ManifestFormat.curl,
    ...                                     manifest_hash=UUID('d2b0ce3c-46f0-57fe-b9d4-2e38d8934fd4'),
    ...                                     source_hash=UUID('77936747-5968-588e-809f-af842d6be9e0'))
    >>> manifest_key = SignedManifestKey(value=bare_manifest_key,
    ...                                  signature=b'123')

    >>> manifest_key.encode()
    'ksQulKNmb2-kY3VybMQQ0rDOPEbwV_651C442JNP1MQQd5NnR1loWI6An6-ELWvp4MQDMTIz'

    >>> SignedManifestKey.decode(manifest_key.encode()) == manifest_key
    True
    """
    value: BareManifestKey = strict_auto()
    signature: bytes = strict_auto()

    def pack(self) -> bytes:
        return msgpack.packb([
            self.value.pack(),
            self.signature
        ])

    @classmethod
    def unpack(cls, pack: bytes) -> Self:
        i = iter(msgpack.unpackb(pack))
        return cls(value=BareManifestKey.unpack(next(i)),
                   signature=next(i))


class ManifestKey(BareManifestKey):
    """
    A manifest key that the service trusts implicitly. It is assumed to have
    either been instantiated by the service itself and transmitted exclusively
    over secure channels, or to have been extracted from a signed manifest key
    after signature verification.

    >>> manifest_key = ManifestKey(catalog='foo',
    ...                            format=ManifestFormat.curl,
    ...                            manifest_hash=UUID('d2b0ce3c-46f0-57fe-b9d4-2e38d8934fd4'),
    ...                            source_hash=UUID('77936747-5968-588e-809f-af842d6be9e0'))

    Encoded representation is short:

    >>> manifest_key.encode()
    'lKNmb2-kY3VybMQQ0rDOPEbwV_651C442JNP1MQQd5NnR1loWI6An6-ELWvp4A'

    It shouldn't be possible to deserialize a ManifestKey instance.

    >>> ManifestKey.decode(manifest_key.encode())
    ... # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    azul.service.manifest_service.InvalidManifestKey:
    lKNmb2-kY3VybMQQ0rDOPEbwV_651C442JNP1MQQd5NnR1loWI6An6-ELWvp4A

    The from_json() method is the inverse of to_json():

    >>> ManifestKey.from_json(manifest_key.to_json()) == manifest_key
    True
    """

    @classmethod
    def unpack(cls, pack: bytes) -> Self:
        """
        Do not call this method. It is unsafe to deserialize an instance of
        this class. Instead, deserialize a :class:`SignedManifestKey` and use
        :meth:`ManifestService.verify_manifest_key_signature`.
        """
        assert False

    _uuid_namespace: ClassVar[UUID] = UUID('c5a0cd95-44f7-4216-972f-623f00f8fd22')

    @property
    def uuid(self) -> UUID:
        return uuid5_for_bytes(self._uuid_namespace, self.pack())


@attrs.frozen
class InvalidManifestKeySignature(Exception):
    value: SignedManifestKey


@attrs.frozen(kw_only=True)
class Manifest(SerializableAttrs):
    """
    Contains the details of a prepared manifest.
    """
    #: The S3 object key under which the manifest is stored in the storage
    #: bucket
    object_key: str

    #: True if an existing manifest was reused or False if a new manifest was
    #: generated.
    was_cached: bool

    #: The format of the manifest
    format: ManifestFormat

    #: Uniquely identifies this manifest
    manifest_key: ManifestKey

    #: The proposed file name of the manifest when downloading it to a user's
    #: system
    file_name: str | None


@attrs.frozen(kw_only=True)
class ManifestPartition(SerializableAttrs):
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
    file_name: str | None = None

    #: The cached configuration of the manifest that contains this partition.
    #: Manifest generators whose `manifest_config` property is expensive should
    #: cache the returned value here for subsequent partitions to reuse.
    config: ManifestConfig | None = serializable(attrs.field(default=None),
                                                 from_json=partial(optional, manifest_config_from_json),
                                                 to_json=partial(optional, manifest_config_to_json))

    #: The ID of the S3 multi-part upload this partition is a part of. If a
    #: manifest consists of just one partition, this may be None, but it doesn't
    #: have to be.
    multipart_upload_id: str | None = None

    #: The S3 ETag of each partition; the current one and all the ones before it
    part_etags: tuple[str, ...] | None = serializable(attrs.field(default=None),
                                                      from_json=partial(optional, compose(tuple, json_element_strings)),
                                                      to_json=partial(optional, list))

    #: The index of the current page. The index is zero-based and global. For
    #: example, if the first partition contains five pages, the index of the
    #: first page in the second partition is 5. This is None for manifests whose
    #: partitions aren't split into pages.
    page_index: int | None = None

    #: True if the current page is the last page of the entire manifest. This is
    #: None for manifests whose partitions aren't split into pages.
    is_last_page: bool | None = None

    #: The `sort` value of the first hit of the current page in this partition,
    #: or None if there is no current page.
    search_after: SortKey | None = serializable(attrs.field(default=None),
                                                from_json=partial(optional, sort_key_from_json),
                                                to_json=partial(optional, sort_key_to_json))

    @classmethod
    def first(cls) -> Self:
        return cls(index=0,
                   is_last=False)

    @property
    def is_first(self) -> bool:
        return not (self.index or self.page_index)

    def with_config(self, config: ManifestConfig) -> Self:
        return attrs.evolve(self, config=config)

    def with_upload(self, multipart_upload_id) -> Self:
        return attrs.evolve(self,
                            multipart_upload_id=multipart_upload_id,
                            part_etags=())

    def first_page(self) -> Self:
        assert self.index == 0, self
        return attrs.evolve(self,
                            page_index=0,
                            is_last_page=False)

    def next_page(self,
                  file_name: str | None,
                  search_after: SortKey | None
                  ) -> Self:
        assert self.page_index is not None, self
        # If different pages yield different file names, use default file name
        if self.page_index > 0:
            if file_name != self.file_name:
                file_name = None
        return attrs.evolve(self,
                            page_index=self.page_index + 1,
                            file_name=file_name,
                            search_after=search_after)

    def last_page(self) -> Self:
        return attrs.evolve(self, is_last_page=True)

    def next(self, part_etag: str) -> Self:
        return attrs.evolve(self,
                            index=self.index + 1,
                            part_etags=(*not_none(self.part_etags), part_etag))

    def last(self, file_name: str) -> Self:
        return attrs.evolve(self,
                            file_name=file_name,
                            is_last=True)


@attrs.frozen
class CachedManifestNotFound(Exception):
    manifest_key: ManifestKey


@attrs.frozen(kw_only=True)
class ManifestService(QueryService):
    file_url_func: FileUrlFunc

    @cached_property
    def storage_service(self) -> StorageService:
        return StorageService()

    def get_manifest(self,
                     *,
                     format: ManifestFormat,
                     catalog: CatalogName,
                     filters: Filters,
                     partition: ManifestPartition,
                     manifest_key: ManifestKey | None = None
                     ) -> Manifest | ManifestPartition:
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

        :param format: The desired format of the manifest.

        :param catalog: The name of the catalog to generate the manifest from.

        :param filters: The filters by which to restrict the contents of the
                        manifest.

        :param partition: The manifest partition to generate. Not all manifests
                          involve multiple partitions. If they don't, a Manifest
                          instance will be returned. Otherwise, the next
                          ManifestPartition instance will be returned.

        :param manifest_key: An optional key identifying the cached manifest. If
                             None, the key will be computed dynamically. This
                             may take a few seconds. If a valid cached manifest
                             exists under the given key, it will be used.
                             Otherwise, a new manifest will be created and
                             stored under the given key.
        """
        generator_cls = ManifestGenerator.cls_for_format(format)
        generator = generator_cls(self, catalog, filters)
        if manifest_key is None:
            manifest_key = generator.manifest_key()
        if partition.is_first:
            try:
                return self._get_cached_manifest(generator_cls, manifest_key)
            except CachedManifestNotFound:
                return self._generate_manifest(generator, manifest_key, partition)
        else:
            return self._generate_manifest(generator, manifest_key, partition)

    def _generate_manifest(self,
                           generator: ManifestGenerator,
                           manifest_key: ManifestKey,
                           partition: ManifestPartition
                           ) -> Manifest | ManifestPartition:
        partition = generator.write(manifest_key, partition)
        if partition.is_last:
            return self._make_manifest(generator_cls=type(generator),
                                       manifest_key=manifest_key,
                                       file_name=partition.file_name,
                                       was_cached=False)
        else:
            return partition

    def get_cached_manifest(self,
                            format: ManifestFormat,
                            catalog: CatalogName,
                            filters: Filters
                            ) -> Manifest:
        generator_cls = ManifestGenerator.cls_for_format(format)
        generator = generator_cls(self, catalog, filters)
        manifest_key = generator.manifest_key()
        return self._get_cached_manifest(generator_cls, manifest_key)

    @classmethod
    def sign_manifest_key(cls, manifest_key: ManifestKey) -> SignedManifestKey:
        """
        Sign the given manifest key with a secret so that it can later be
        verified to have not been tamplered with.
        """
        response = aws.kms.generate_mac(Message=manifest_key.pack(),
                                        KeyId=config.manifest_kms_key.alias,
                                        MacAlgorithm='HMAC_SHA_256')
        return SignedManifestKey(value=manifest_key,
                                 signature=response['Mac'])

    @classmethod
    def verify_manifest_key(cls, manifest_key: SignedManifestKey) -> ManifestKey:
        """
        Verify a manifest key against its signature. If either the key or the
        signature have been tampered with, an exception will be raised.
        """
        try:
            response = aws.kms.verify_mac(KeyId=config.manifest_kms_key.alias,
                                          MacAlgorithm='HMAC_SHA_256',
                                          Message=manifest_key.value.pack(),
                                          Mac=manifest_key.signature)
        except aws.kms.exceptions.KMSInvalidMacException:
            raise InvalidManifestKeySignature(manifest_key)
        else:
            assert response['MacValid']
            return ManifestKey(**attrs.asdict(manifest_key.value))

    def get_cached_manifest_with_key(self, manifest_key: ManifestKey) -> Manifest:
        generator_cls = ManifestGenerator.cls_for_format(manifest_key.format)
        return self._get_cached_manifest(generator_cls, manifest_key)

    def _get_cached_manifest(self,
                             generator_cls: type[ManifestGenerator],
                             manifest_key: ManifestKey
                             ) -> Manifest:
        file_name = self._get_cached_manifest_file_name(generator_cls, manifest_key)
        if file_name is None:
            raise CachedManifestNotFound(manifest_key)
        else:
            return self._make_manifest(generator_cls=generator_cls,
                                       manifest_key=manifest_key,
                                       file_name=file_name,
                                       was_cached=True)

    def _make_manifest(self,
                       generator_cls: type[ManifestGenerator],
                       manifest_key: ManifestKey,
                       file_name: str | None,
                       was_cached: bool
                       ) -> Manifest:
        if not generator_cls.use_content_disposition_file_name():
            file_name = None
        object_key = generator_cls.s3_object_key(manifest_key)
        return Manifest(object_key=object_key,
                        was_cached=was_cached,
                        format=generator_cls.format(),
                        manifest_key=manifest_key,
                        file_name=file_name)

    def get_manifest_url(self, manifest: Manifest) -> str:
        return self.storage_service.get_presigned_url(object_key=manifest.object_key,
                                                      file_name=manifest.file_name)

    file_name_tag = 'azul_file_name'

    def _get_cached_manifest_file_name(self,
                                       generator_cls: type[ManifestGenerator],
                                       manifest_key: ManifestKey
                                       ) -> str | None:
        """
        Return the proposed local file name of the manifest with the given
        object key if it was previously created, still exists in the bucket, and
        won't be expiring soon. Otherwise return None.

        :param generator_cls: The generator class of the manifest

        :param manifest_key: The key of the cached manifest
        """
        object_key = generator_cls.s3_object_key(manifest_key)
        try:
            time_left = self.storage_service.time_until_object_expires(object_key,
                                                                       expiration=config.manifest_expiration)
        except StorageObjectNotFound:
            log.info('Cached manifest not found: %s', manifest_key)
            return None
        else:
            if time_left > config.manifest_expiration_margin:
                tagging = self.storage_service.get_object_tagging(object_key)
                try:
                    file_name = tagging[self.file_name_tag]
                except KeyError:
                    # While unpaged manifest generators apply the tag *at*
                    # object creation, paged ones do so in a separate request.
                    # Reaching this point for a paged manifest (no name tag)
                    # means that the manifest has been created but not yet
                    # tagged. In this case, we treat the manifest as if it
                    # doesn't yet exist and return None. This assumes that the
                    # caller will then raise a `CachedManifestNotFound`
                    # exception causing a redirect response to the client and
                    # when the client follows the redirect, the tagging should
                    # be complete.
                    return None
                else:
                    encoded_file_name = file_name.encode('ascii')
                    return base64.urlsafe_b64decode(encoded_file_name).decode('utf-8')
            else:
                log.info('Cached manifest is about to expire: %s', object_key)
                return None

    def command_lines(self,
                      manifest: Manifest | None,
                      url: furl,
                      authentication: Authentication | None
                      ) -> FlatJSON:
        format = None if manifest is None else manifest.format
        generator_cls = ManifestGenerator.cls_for_format(format)
        file_name = None if manifest is None else manifest.file_name
        return generator_cls.command_lines(url, file_name, authentication)


type Cells = dict[str, str]


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

    @property
    def metadata_plugin(self) -> MetadataPlugin:
        return self.service.metadata_plugin(self.catalog)

    @cached_property
    def mirror_service(self) -> MirrorService:
        return MirrorService.for_catalog(self.catalog)

    @classmethod
    @abstractmethod
    def file_name_extension(cls) -> str:
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

    @classmethod
    def use_content_disposition_file_name(cls) -> bool:
        """
        True if the manifest output produced by the generator should use a custom
        file name when stored on a file system.
        """
        return True

    @property
    @abstractmethod
    def entity_type(self) -> EntityType:
        """
        The type of the index entities this generator consumes. This controls
        which aggregate OpenSearch index is queried to fetch the aggregate
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
        return self.metadata_plugin.manifest_config

    @cached_property
    def included_fields(self) -> list[FieldPath] | None:
        """
        A list of field paths to be included when requesting entity documents
        from the index or None if all fields should be included.

        https://www.elastic.co/guide/en/elasticsearch/reference/7.10/search-fields.html#source-filtering
        """
        return [
            (*field_path, field_name)
            for field_path, column_mapping in self.manifest_config.items()
            for field_name in column_mapping.keys()
            if field_name is not None
        ]

    _cls_for_format: dict[ManifestFormat, type[ManifestGenerator]] = {}

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        if not isabstract(cls):
            format = cls.format()
            assert format not in cls._cls_for_format
            cls._cls_for_format[format] = cls

    @classmethod
    def cls_for_format(cls,
                       format: ManifestFormat | None
                       ) -> type[ManifestGenerator]:
        """
        Return the generator class  for the given format.

        :param format: format specifying which type of generator to use

        :return: a concrete subclass of ManifestGenerator
        """
        if format is None:
            return cls
        else:
            return cls._cls_for_format[format]

    @classmethod
    def _cmd_exe_quote(cls, s: str) -> str:
        """
        Escape a string for insertion into a `cmd.exe` command line
        """
        assert '\\' not in s, s
        return dq(s)

    @classmethod
    def command_lines(cls,
                      url: furl,
                      file_name: str | None,
                      authentication: Authentication | None
                      ) -> FlatJSON:
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
                cls._cmd_exe_quote(str(url))
            ]),
            'bash': ' '.join([
                'curl',
                *options(shlex.quote),
                shlex.quote(str(url))
            ])
        }

    def __init__(self,
                 service: ManifestService,
                 catalog: CatalogName,
                 filters: Filters
                 ) -> None:
        """
        Construct a generator instance.

        :param catalog: the name of the catalog to use when querying the index
                        for the documents to be transformed into the manifest

        :param filters: the filter to use when querying the index for the
                        documents to be transformed into the manifest

        :param service: the service to use when querying the index
        """
        super().__init__()
        self.service = service
        self.catalog = catalog
        self.filters = filters
        self.file_url_func = service.file_url_func

    manifest_namespace = UUID('ca1df635-b42c-4671-9322-b0a7209f0235')

    source_namespace = UUID('6540b139-ea49-4e36-8f19-17c309b5fa76')

    def manifest_key(self) -> ManifestKey:
        """
        Return a manifest object key deterministically derived from this
        generator's parameters (its concrete type and the arguments passed to
        its constructor) and the current commit hash. The same parameters will
        always produce the same return value in one revision of this code.
        Different parameters should, with a very high probability, produce
        different return values.
        """
        git_commit = config.git_status['commit']
        # The explicit filters are already normalized so we don't to do anything
        # special to desensitize the hash to insignificat differences
        filter_string = json.dumps(self.filters.explicit)
        content_hash = self._content_hash(by_bundle=config.enable_bundle_notifications)
        catalog = self.catalog
        format = self.format()
        manifest_hash_input = [
            git_commit,
            catalog,
            format.value,
            content_hash,
            filter_string
        ]
        joiner = ','
        assert not any(joiner in param for param in manifest_hash_input[:-1])
        manifest_hash = uuid5(self.manifest_namespace, joiner.join(manifest_hash_input))

        source_ids = sorted(self.filters.source_ids)
        assert not any(joiner in source_id for source_id in source_ids), source_ids
        source_hash = uuid5(self.source_namespace, joiner.join(source_ids))

        return ManifestKey(catalog=catalog,
                           format=format,
                           manifest_hash=manifest_hash,
                           source_hash=source_hash)

    @classmethod
    def s3_object_key(cls, manifest_key: ManifestKey) -> str:
        return 'manifests' + '/' + cls.s3_object_key_base(manifest_key)

    @classmethod
    def s3_object_key_base(cls, manifest_key: ManifestKey) -> str:
        manifest_hash = str(manifest_key.manifest_hash)
        source_hash = str(manifest_key.source_hash)
        for part in manifest_hash, source_hash:
            for joiner in '.', '/':
                assert joiner not in part, (joiner, part)
        return '.'.join([manifest_hash, source_hash, cls.file_name_extension()])

    def file_name(self,
                  manifest_key: ManifestKey,
                  base_name: str | None = None
                  ) -> str:
        if base_name:
            file_name_prefix = unicodedata.normalize('NFKD', base_name)
            file_name_prefix = re.sub(r'[^\w ,.@%&\-_()\\[\]/{}]', '_', file_name_prefix).strip()
            timestamp = datetime.now().strftime('%Y-%m-%d %H.%M')
            file_name = f'{file_name_prefix} {timestamp}.{self.file_name_extension()}'
        else:
            atlas = config.catalogs[self.catalog].atlas
            file_name = atlas + '-manifest-' + self.s3_object_key_base(manifest_key)
        return file_name

    def _create_request(self, entity_type: EntityType) -> Search:
        pipeline = self._create_pipeline()
        request = self.service.create_request(self.catalog, entity_type)
        request = pipeline.prepare_request(request)
        # The response is processed by the generator, not the pipeline
        return request

    def _create_pipeline(self) -> OpenSearchChain[Response, Any, Response]:
        if self.included_fields is None:
            document_slice = DocumentSlice()
        else:
            includes = list(map(dotted, self.included_fields))
            # The complete set of source fields is needed to polymorphically
            # deserialize sources from the index. The sources are used to
            # help determine which files might be mirrored.
            includes.append('sources.*')
            document_slice = DocumentSlice(includes=includes)
        pipeline = self.service.create_chain(catalog=self.catalog,
                                             entity_type=self.entity_type,
                                             filters=self.filters,
                                             post_filter=False,
                                             document_slice=document_slice)
        return pipeline

    def _hit_to_doc(self, hit: Hit) -> MutableJSON:
        return self.service.translate_fields(self.catalog,
                                             hit.to_dict(),
                                             forward=False)

    column_joiner = config.manifest_column_joiner
    padded_joiner = ' ' + column_joiner + ' '

    @cached_property
    def _field_types(self) -> FieldTypes:
        return self.service.field_types(self.catalog)

    def _extract_fields(self,
                        *,
                        field_path: FieldPath,
                        entities: JSONs,
                        column_mapping: ColumnMapping,
                        row: Cells) -> None:
        """
        Extract columns in `column_mapping` from `entities` and insert values
        into `row`.
        """
        field_types = self._field_types
        for field in field_path:
            assert isinstance(field_types, dict)
            field_types = field_types[field]

        def convert(field_name, field_value):
            try:
                field_type = field_types[field_name]
            except KeyError:
                if field_name in ('file_url', 'file_mirror_uri'):
                    field_type = null_str
                else:
                    raise
            else:
                if isinstance(field_type, list):
                    field_type = one(field_type)
            assert isinstance(field_type, FieldType)
            return field_type.to_tsv(field_value)

        def validate(field_value: str) -> str:
            assert self.column_joiner not in field_value
            return field_value

        for field_name, column_name in column_mapping.items():
            if column_name is not None:
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
                column_value = self.padded_joiner.join(sorted(set(column_value))[:100])
                row[column_name] = column_value

    def _get_entities(self, field_path: FieldPath, doc: JSON) -> JSONs:
        """
        Given a document and a dotted path into that document, return the list
        of entities designated by that path.
        """
        assert field_path, field_path
        d = doc
        for key in field_path[:-1]:
            d = json_mapping(d.get(key, {}))
        entities = json_sequence(d.get(field_path[-1], []))
        assert json_elements_are_mappings(entities)
        return entities

    def _azul_file_url(self,
                       file: JSON,
                       args: Mapping = frozendict()
                       ) -> str | None:
        if file['drs_uri'] is None:
            # To download a file we need its DRS URI
            return None
        else:
            special_fields = self.metadata_plugin.special_fields
            return str(self.file_url_func(catalog=self.catalog,
                                          file_uuid=json_str(file[special_fields.file_uuid.name_in_hit]),
                                          version=json_str(file['version']),
                                          fetch=False,
                                          **args))

    def _azul_mirror_uri(self, source: SourceRef, file: JSON) -> str | None:
        file_cls = self.metadata_plugin.file_class
        return self.mirror_service.mirror_uri(source, file_cls, file)

    @cache
    def _content_hash(self, *, by_bundle: bool) -> str:
        """
        Return a hash of the input this generator builds the manifest from. The
        input is the set of ES documents from the files index. For two generator
        instances g1 and g2 created at two different points in time, and any
        boolean value b, if

        g1.manifest_hash(by_bundle=b) == g2.manifest_hash(by_bundle=b)

        then there is a high probability that the manifests generated by g1 and
        g2 contain the same set of entries. This test can be used in deciding
        whether g2 can reuse g1's manifest, thereby avoiding an expensive
        operation. A false positive occurs when the hashes are equal but the
        inputs differ. A false negative occurs when the hashes differ, but the
        inputs are equal. False negatives are less problematic because they only
        lead to redundant computations: the manifest is regenerated when it
        could have been reused. False positives are problematic because they
        lead to a manifest being reused erroneously, yielding an incorrect
        manifest that is inconsistent with the input.

        If ``by_bundle`` is True, the hash is computed from the fully-qualified
        identifiers (FQID) of all bundles (subgraphs) containing files that
        match the current filter. The rate of false negatives is low because a
        change to any file entity requires a new bundle or a new bundle version,
        both of which have different FQIDs, leading to a different hash. This
        mode is slower and should be used if the index is changing or is likely
        to change due to the incremental incorporation of bundles.

        If ``by_bundle`` is False, the hash is instead computed from the set of
        identifiers of the sources that contributed files matching the current
        filters. This mode should *not* be used if the index is changing or is
        likely to change due to the incremental incorporation of bundles.
        """
        log.debug('Computing content hash from %s matching %r ...',
                  'bundles' if by_bundle else 'sources', self.filters)
        start_time = time.time()
        if by_bundle:
            entity_type = self.entity_type
        else:
            entity_type = self.metadata_plugin.root_entity_type
        request = self._create_request(entity_type)
        request.aggs.metric(
            'hash',
            'scripted_metric',
            init_script='''
                state.fields = 0
            ''',
            map_script=(
                '''
                    for (bundle in params._source.bundles) {
                        state.fields += (bundle.uuid + bundle.version).hashCode()
                    }
                '''
                if by_bundle else
                '''
                    for (source in params._source.sources) {
                        state.fields += source.id.hashCode()
                    }
                '''
            ),
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
        hash_value = str(response.aggregations.hash.value)
        log.info('Computed content hash %r from %s matching %r',
                 hash_value, time.time() - start_time, self.filters)
        return hash_value

    def tagging(self, file_name: str | None) -> Mapping[str, str] | None:
        if file_name is None:
            return None
        else:
            encoded_file_name = base64.urlsafe_b64encode(file_name.encode('utf-8'))
            return {self.service.file_name_tag: encoded_file_name.decode('ascii')}

    @abstractmethod
    def write(self,
              manifest_key: ManifestKey,
              partition: ManifestPartition,
              ) -> ManifestPartition:
        """
        Write the given partition of a manifest to object storage under the
        specified key and return the next partition to be written. Unless the
        returned partition is the last one, this method will soon be invoked
        again, passing the partition returned by the previous invocation.

        A minimal implementation of this method would write the entire manifest
        in just one large partition and return that partition with the is_last
        flag set.

        :param manifest_key: The manifest key under which to store the manifest
                             partition.

        :param partition: The partition to write.
        """
        raise NotImplementedError

    @property
    def storage(self) -> StorageService:
        return self.service.storage_service


class ClientSidePagingManifestGenerator(ManifestGenerator, metaclass=ABCMeta):
    """
    A manifest generator that uses client-side paging to query OpenSearch.
    """
    page_size = 500

    #: A paginator is a function that, given a ``search_after`` value, returns a
    #: fully populated OpenSearch request for one page worth of sorted hits with
    #: the first hit's sort key matching that ``search_after`` value. If the
    #: ``search_after`` value is None, the request for the first page is
    #: returned.
    #:
    type Paginator = Callable[[SortKey | None], Search]

    def _paginate_hits(self, paginator: Paginator) -> Iterable[Hit]:
        """
        Yield all hits in every page of OpenSearch hits obtained using the
        given paginator.
        """
        search_after = None
        while True:
            request = paginator(search_after)
            response = request.execute()
            if response.hits:
                hit = None
                for hit in response.hits:
                    yield hit
                assert hit is not None
                search_after = self._search_after(hit)
            else:
                break

    def _custom_paginator(self, request: Search, sort: Sequence[str]) -> Paginator:
        """
        Copies the given request, sets up the copy for pagination using the
        given sort, and returns a paginator that uses the copy to produce
        requests for individual pages. The sort is specified as a sequence of
        one or two field names: the primary field to sort by and an optional tie
        breaker. The length of the ``sort`` argument must be equal to the length
        of the ``search_after`` value the returned paginator is called with.

        Note that this method *returns* a paginator.
        """
        request = request.extra(size=self.page_size)
        request = request.sort(*sort)

        def request_factory(search_after: SortKey | None) -> Search:
            if search_after is None:
                return request
            else:
                return request.extra(search_after=search_after)

        return request_factory

    def _default_paginator(self, search_after: SortKey | None) -> Search:
        """
        Creates an OpenSearch request for finding aggregates of the current
        entity type, matching the current filters, sorting the hits by entity ID
        and returning one page of worth of hits, starting at the hit with the
        given key, or, if the key is None, starting at the first hit.

        Note that this method *is* a Paginator.
        """
        pagination = Pagination(sort='entryId',
                                order='asc',
                                size=self.page_size,
                                search_after=search_after)
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

    def _search_after(self, hit: Hit) -> SortKey:
        return sort_key_from_json(list(hit.meta.sort))


class PagedManifestGenerator(ClientSidePagingManifestGenerator):
    """
    A manifest generator whose output is split over several concatenable
    segments, also known as pages.

    In some subclasses, e.g. CompactManifestGenerator and CurlManifestGenerator,
    a manifest page corresponds to a page of hits from a paginated OpenSearch
    request. In others, e.g. JSONLVerbatimManifestGenerator, the relationship
    between manifest pages and OpenSearch pages is more complicated.
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

    assert aws.s3_min_part_size <= part_size <= aws.s3_max_part_size

    def write(self,
              manifest_key: ManifestKey,
              partition: ManifestPartition,
              ) -> ManifestPartition:
        assert not partition.is_last, partition
        if partition.config is None:
            partition = partition.with_config(self.manifest_config)
        else:
            type(self).manifest_config.fset(self, partition.config)
        object_key = self.s3_object_key(manifest_key)
        if partition.multipart_upload_id is None:
            upload_id = self.storage.create_multipart_upload(object_key=object_key)
            partition = partition.with_upload(upload_id)
        else:
            upload_id = partition.multipart_upload_id
        if partition.page_index is None:
            partition = partition.first_page()
        with BytesIO() as buffer:
            with TextIOWrapper(buffer, encoding='utf-8', write_through=True) as text_buffer:
                while True:
                    partition = self.write_page_to(partition, output=text_buffer)
                    # Manifest lambda has 2 GB of memory
                    assert buffer.tell() < 1.5 * 1024 ** 3
                    if partition.is_last_page or buffer.tell() > self.part_size:
                        break
                if buffer.tell() > 0:
                    buffer.seek(0)
                    part_etag = self.storage.upload_multipart_part(object_key=object_key,
                                                                   upload_id=upload_id,
                                                                   part_number=partition.index + 1,
                                                                   buffer=buffer)
                    partition = partition.next(part_etag=part_etag)
                if partition.is_last_page:
                    self.storage.complete_multipart_upload(object_key=object_key,
                                                           upload_id=upload_id,
                                                           etags=not_none(partition.part_etags))
                    file_name = self.file_name(manifest_key, base_name=partition.file_name)
                    tagging = self.tagging(file_name)
                    if tagging is not None:
                        self.storage.put_object_tagging(object_key, tagging)
                    partition = partition.last(file_name)
                return partition


class FileBasedManifestGenerator(ClientSidePagingManifestGenerator):
    """
    A manifest generator that writes its output to a file.

    :return: the path to the file containing the output of the generator and an
             optional string that should be used to name the output when
             persisting it to an object store or another file system
    """

    @abstractmethod
    def create_file(self) -> tuple[str, str | None]:
        raise NotImplementedError

    def write(self,
              manifest_key: ManifestKey,
              partition: ManifestPartition,
              ) -> ManifestPartition:
        """
        Generate the manifest and return the desired content disposition file
        name if necessary.
        """
        assert partition.index == 0 and partition.page_index is None, partition
        file_path, base_name = self.create_file()
        file_name = self.file_name(manifest_key, base_name)
        try:
            self.storage.upload(file_path=file_path,
                                object_key=(self.s3_object_key(manifest_key)),
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

    @classmethod
    def file_name_extension(cls):
        return 'curlrc'

    @property
    def entity_type(self) -> EntityType:
        return 'files'

    @cached_property
    def included_fields(self) -> list[FieldPath] | None:
        return [
            *not_none(super().included_fields),
            ('contents', 'files', 'related_files')
        ]

    @classmethod
    def command_lines(cls,
                      url: furl,
                      file_name: str | None,
                      authentication: Authentication | None
                      ) -> FlatJSON:
        authentication_option = [] if authentication is None else [
            '--header',
            cls._option(authentication.as_http_header())
        ]
        manifest_options = [
            '--location',
            '--fail',
        ]
        rate_limit = config.waf_rate_limit_files
        # Some options are added to the command-line instead of the curl
        # manifest so that the user can more easily customize them.
        file_options = [
            # We want curl to make enough retries so that it waits a total of
            # one and a half times the evaluation window of the WAF rate rule,
            # long enough for the tripped rule to clear.
            f'--retry {ceil(rate_limit.period * 1.5 / rate_limit.retry_after)}',
            # Curl will respect the 'Retry-After' header if given in a response,
            # like the one returned when the WAF rate rule is tripped. Otherwise,
            # curl will wait for the number of seconds specified here.
            '--retry-delay 10',
        ]
        return {
            'cmd.exe': ' '.join([
                'curl.exe',
                *manifest_options,
                cls._cmd_exe_quote(str(url)),
                '|',
                'curl.exe',
                *authentication_option,
                *file_options,
                '--config',
                '-'
            ]),
            'bash': ' '.join([
                'curl',
                *manifest_options,
                shlex.quote(str(url)),
                '|',
                'curl',
                *authentication_option,
                *file_options,
                '--config',
                '-'
            ])
        }

    @classmethod
    def _option(cls, s: str) -> str:
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
            special_fields = self.metadata_plugin.special_fields
            file_name_field = special_fields.file_name.name_in_hit
            file_uuid_field = special_fields.file_uuid.name_in_hit

            file_name = json_str(file[file_name_field])
            # Related files are indexed differently than normal files (they
            # don't have their own document but are listed inside the main
            # file's document), so to ensure that the /repository/files
            # endpoint can resolve them correctly, their endpoint URLs
            # contain additional parameters, so that the endpoint does not
            # need to query the index for that information.
            args = {
                'requestIndex': 1,
                'fileName': file_name,
                'drsUri': file['drs_uri']
            } if is_related_file else {
            }

            file_url = self._azul_file_url(file, args)
            if file_url is None:
                output.write(f"# File {file[file_uuid_field]!r}, version {file['version']!r} "
                             f"is currently not available in catalog {self.catalog!r}.\n\n")
            else:
                # To prevent overwriting one file with another one of the same name
                # but different content we nest each file in a folder using the
                # bundle UUID. Because a file can belong to multiple bundles we use
                # the one with the most recent version.
                bundle = max(json_element_mappings(doc['bundles']),
                             key=itemgetter('version', 'uuid'))
                output_name = json_str(bundle['uuid']) + '/' + file_name
                output_name = self._sanitize_path(output_name)
                output.write(f'url={self._option(file_url)}\n'
                             f'output={self._option(output_name)}\n\n')

        if partition.page_index == 0:
            curl_options = [
                # FIXME: Remove `--http1.1` option
                #        https://github.com/DataBiosphere/azul/issues/7032
                '--http1.1',  # Avoid a bug in curl 8.7.1 where 429s aren't retried with HTTP/2
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

        request = self._default_paginator(partition.search_after)
        response = request.execute()
        if response.hits:
            hit = None
            for hit in response.hits:
                doc = self._hit_to_doc(hit)
                contents = json_mapping(doc['contents'])
                files = json_sequence(contents['files'])
                file = json_mapping(one(files))
                source_json = json_mapping(one(json_sequence(doc['sources'])))
                source: SourceRef = SourceRef.from_json(source_json)

                # On AnVIL, we are only permitted to include mirrored files, in
                # order to limit egress cost against the owner of the originals
                # in GCP. Note that the conditional below indicates that a file
                # will *eventually* be mirrored, not that it already has been.
                #
                if (
                    not config.is_anvil_enabled(self.catalog)
                    or self.mirror_service.will_mirror(source.spec, json_int(file['file_size']))
                ):
                    _write(file)
                    if config.is_hca_enabled(self.catalog):
                        for related_file in json_element_mappings(file['related_files']):
                            _write(related_file, is_related_file=True)
            assert hit is not None
            return partition.next_page(file_name=None,
                                       search_after=self._search_after(hit))
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
        AssertionError: R('Invalid file path', 'foo/bar/\\x1f/file',
                          'Control character or backslash at position', 8)

        >>> f('foo/bar/COM6/file') # doctest: +NORMALIZE_WHITESPACE
        Traceback (most recent call last):
        ...
        AssertionError: R('Invalid file path', 'foo/bar/COM6/file',
                          'Use of reserved path component for Windows', {'COM6'})

        >>> f('foo/bar/ / baz/file') # doctest: +NORMALIZE_WHITESPACE
        Traceback (most recent call last):
        ...
        AssertionError: R('Invalid file path', 'foo/bar/ / baz/file')

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
        assert match is None, R('Invalid file path', path,
                                'Control character or backslash at position', match.start())

        path = cls._problematic_chars.sub('_', path)

        assert cls._valid_path.fullmatch(path) is not None, R('Invalid file path', path)

        components = set(path.split('/')) & cls.special_dos_files
        assert not components, R('Invalid file path', path,
                                 'Use of reserved path component for Windows', components)

        return path


class CompactManifestGenerator(PagedManifestGenerator):

    @classmethod
    def format(cls) -> ManifestFormat:
        return ManifestFormat.compact

    @property
    def content_type(self) -> str:
        return 'text/tab-separated-values'

    @classmethod
    def file_name_extension(cls):
        return 'tsv'

    @property
    def entity_type(self) -> EntityType:
        return 'files'

    @cached_property
    def included_fields(self) -> list[FieldPath] | None:
        return [
            *not_none(super().included_fields),
            ('contents', 'files', 'related_files')
        ]

    def write_page_to(self,
                      partition: ManifestPartition,
                      output: IO[str]
                      ) -> ManifestPartition:
        column_mappings = self.manifest_config.values()
        column_mappings = (d.values() for d in column_mappings)
        column_names = list(filter(None, chain.from_iterable(column_mappings)))
        writer = csv.DictWriter(output, column_names, dialect='excel-tab')

        if partition.page_index == 0:
            writer.writeheader()

        request = self._default_paginator(partition.search_after)
        response = request.execute()
        if response.hits:
            project_short_names: set[str] = set()
            hit = None
            for hit in response.hits:
                doc = self._hit_to_doc(hit)
                assert isinstance(doc, dict)
                contents = json_mapping(doc['contents'])
                sources = json_element_mappings(doc['sources'])
                source: SourceRef = SourceRef.from_json(one(sources))
                if len(project_short_names) < 2 and 'projects' in contents:
                    project = one(json_sequence_of_mappings(contents['projects']))
                    short_names = json_element_strings(project['project_short_name'])
                    project_short_names.update(short_names)
                row: Cells = {}
                related_rows: list[Cells] = []
                for field_path, column_mapping in self.manifest_config.items():
                    entities = self._get_entities(field_path, doc)
                    if field_path == ('contents', 'files'):
                        file = copy_json(one(entities))
                        if 'file_url' in column_mapping:
                            file['file_url'] = self._azul_file_url(file)
                        if 'file_mirror_uri' in column_mapping:
                            file['file_mirror_uri'] = self._azul_mirror_uri(source, file)
                        entities = [file]
                    self._extract_fields(field_path=field_path,
                                         entities=entities,
                                         column_mapping=column_mapping,
                                         row=row)
                    if field_path == ('contents', 'files'):
                        file = copy_json(one(entities))
                        if 'related_files' in file:
                            field_path = (*field_path, 'related_files')
                            for related_file in json_element_dicts(file['related_files']):
                                related_row: Cells = {}
                                file.update(related_file)
                                if 'file_url' in column_mapping:
                                    file['file_url'] = self._azul_file_url(file)
                                if 'file_mirror_uri' in column_mapping:
                                    file['file_mirror_uri'] = self._azul_mirror_uri(source, file)
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
            file_name = project_short_names.pop() if len(project_short_names) == 1 else None
            return partition.next_page(file_name=file_name,
                                       search_after=self._search_after(hit))
        else:
            return partition.last_page()


FQID = tuple[str, str]
Qualifier = str

Group = Mapping[str, Cells]
Groups = list[Group]
Bundle = dict[Qualifier, Groups]
Bundles = dict[FQID, Bundle]


class PFBManifestGenerator(FileBasedManifestGenerator):

    @classmethod
    def format(cls) -> ManifestFormat:
        return ManifestFormat.terra_pfb

    @classmethod
    def file_name_extension(cls):
        return 'avro'

    @property
    def content_type(self) -> str:
        return 'application/octet-stream'

    @property
    def entity_type(self) -> str:
        return 'files'

    @property
    def included_fields(self) -> list[FieldPath] | None:
        """
        We want all of the metadata because then we can use the field_types()
        to generate the complete schema.
        """
        return None

    def _all_docs_sorted(self) -> Iterable[JSON]:
        request = self._create_request(self.entity_type)
        sort = ['entity_id.keyword']
        paginator = self._custom_paginator(request, sort)
        hits = self._paginate_hits(paginator)
        return map(self._hit_to_doc, hits)

    def create_file(self) -> tuple[str, str | None]:
        transformers = self.service.transformer_types(self.catalog)
        transformer = one(t for t in transformers if t.entity_type() == 'files')
        field_types = transformer.field_types()
        pfb_schema = avro_pfb.pfb_schema_from_field_types(field_types)

        converter = avro_pfb.PFBConverter(pfb_schema)
        for doc in self._all_docs_sorted():
            converter.add_doc(doc)

        links = avro_pfb.pfb_links_from_field_types(field_types)
        entity = avro_pfb.pfb_metadata_entity(links)
        entities = itertools.chain([entity], converter.entities())

        fd, path = mkstemp(suffix='.avro')
        os.close(fd)
        avro_pfb.write_pfb_entities(entities, pfb_schema, path)
        return path, None


class VerbatimManifestGenerator(ClientSidePagingManifestGenerator,
                                metaclass=ABCMeta):
    page_size = 5000

    @property
    def entity_type(self) -> EntityType:
        # Orphans only have projects/datasets as hubs, so we need to retrieve
        # aggregates of those types in order to join against orphan replicas
        root_entity_type = self.metadata_plugin.root_entity_type
        return root_entity_type if self.include_orphans else 'files'

    @property
    def included_fields(self) -> list[FieldPath]:
        # This is only used when searching the aggregates, which are only used
        # to perform a "join" on the replicas index. Therefore, we only need the
        # "keys" used for the join.
        return [
            ('entity_id',),
            *(
                ('contents', entity_type, 'document_id')
                for entity_type in self.hot_entity_types
            )
        ]

    @property
    def hot_entity_types(self) -> Iterable[str]:
        return self.metadata_plugin.hot_entity_types

    @property
    def include_orphans(self) -> bool:

        # When filtering exclusively by properties of implicit hubs, e.g.,
        # data sets for AnVIL or projects for HCA, we include replicas of all
        # entities implicitly connected to the matching hubs, even replicas of
        # orphans, i.e., entities that aren't connected to files.
        #
        plugin = self.metadata_plugin
        root_entity_fields = {
            field_name
            for field_name, field_path in plugin.field_mapping.items()
            if field_path[0] == 'contents' and field_path[1] == plugin.root_entity_type
        }

        # For both HCA and AnVIL, these root entities are bijective with the
        # sources used for indexing, and filtering by a specific project
        # or dataset entity should produce the same results as filtering by
        # that entity's source.
        #
        # The verbatim JSONL generator temporarily inserts a source ID condition
        # into its provided filters in order to partition the manifest. If the
        # source ID field were not included below, that insertion would cause
        # orphans to be absent from the manifest, which is incorrect.
        #
        source_fields = {
            plugin.special_fields.source_id.name,
            plugin.special_fields.source_spec.name
        }
        return self.filters.explicit.keys() < (root_entity_fields | source_fields)

    @attrs.frozen(kw_only=True)
    class ReplicaKeys:
        """
        Most replicas contain a list of the entity ID of their hubs, usually
        file entities. However, some low-cardinality entities like HCA projects
        have too many hubs to track within their replica document.

        This class captures the information needed to locate all replicas
        associated with a given a hub entity, either using the hub's entity ID
        or the replica's entity ID.
        """
        hub_id: str
        replica_ids: list[str]

    def _list_replica_keys(self) -> Iterable[ReplicaKeys]:
        paginator = self._default_paginator
        for hit in self._paginate_hits(paginator):
            document_ids = [
                document_id
                for entity_type in self.hot_entity_types
                for inner_entity in getitem(hit['contents'], entity_type, ())
                # `document_id` is a scalar (string) when the inner and outer
                # entity types match, and an array otherwise. `None` should not
                # occur.
                for document_id in always_iterable(inner_entity['document_id'])
            ]
            yield self.ReplicaKeys(hub_id=hit['entity_id'],
                                   replica_ids=document_ids)

    def _list_replicas(self) -> Iterable[MutableJSON]:
        emitted_replica_ids = set()
        for page in chunked(self._list_replica_keys(), self.page_size):
            num_replicas = 0
            num_new_replicas = 0
            for replica in self._join_replicas(page):
                num_replicas += 1
                # A single replica may have many hubs. To prevent replicas from
                # being emitted more than once, we need to keep track of
                # replicas already emitted.
                replica_id = replica.meta.id
                if replica_id not in emitted_replica_ids:
                    num_new_replicas += 1
                    yield copy_json(replica.to_dict())
                    emitted_replica_ids.add(replica_id)
            log.info('Found %d replicas (%d already emitted) from page of %d hubs',
                     num_replicas, num_replicas - num_new_replicas, len(page))

    def _join_replicas(self, keys: Iterable[ReplicaKeys]) -> Iterable[Hit]:
        hub_ids, replica_ids = set(), set()
        for key in keys:
            hub_ids.add(key.hub_id)
            replica_ids.update(key.replica_ids)

        request = self.service.create_request(catalog=self.catalog,
                                              entity_type='replica',
                                              doc_type=DocumentType.replica)
        request = request.query(Q('bool', should=[
            {'terms': {'hub_ids.keyword': list(hub_ids)}},
            {'terms': {'entity_id.keyword': list(replica_ids)}}
        ]))

        # `_id` is currently the only index field that is unique to each replica
        # document (and thus results in an unambiguous total ordering). However,
        # sorting just by `_id` is unacceptably slow, an OpenSearch quirk. To
        # overcome the performance hit, we sort by a field that's *almost*
        # unique to each replica, so that `_id` only needs to be loaded and
        # compared in the infrequent event that it's needed as a tiebreaker.
        #
        # FIXME: ES DeprecationWarning for using _id as sort key
        #        https://github.com/DataBiosphere/azul/issues/7290
        #
        sort = ['entity_id.keyword', '_id']
        paginator = self._custom_paginator(request, sort)
        return self._paginate_hits(paginator)


class JSONLVerbatimManifestGenerator(PagedManifestGenerator,
                                     VerbatimManifestGenerator):

    @property
    def content_type(self) -> str:
        return 'application/jsonl'

    @classmethod
    def file_name_extension(cls) -> str:
        return 'jsonl'

    @classmethod
    def format(cls) -> ManifestFormat:
        return ManifestFormat.verbatim_jsonl

    @property
    def source_id_field(self) -> SpecialField:
        return self.metadata_plugin.special_fields.source_id

    def source_ids(self) -> list[str]:
        # Currently, we process each source that might be included in the
        # manifest. This can be very inefficient since many partitions may be
        # empty for small manifests. A potential optimization is to use a terms
        # aggregation to query for the set of nonempty sources before
        # processing any hits.

        # It's possible that inaccessible sources are included in the explicit
        # sources. If they are, an exception will be raised when the filters are
        # reified, so it's safe to skip that check here.
        sources: Iterable[str]
        try:
            source_filter = self.filters.explicit[self.source_id_field.name]
        except KeyError:
            sources = self.filters.source_ids
        else:
            assert 'is' in source_filter
            sources = json_element_strings(source_filter['is'])
        return sorted(sources)

    def write_page_to(self,
                      partition: ManifestPartition,
                      output: IO[str]
                      ) -> ManifestPartition:
        # All replicas from each source must be held in memory simultaneously to
        # avoid emitting duplicates. Therefore, each "page" of this manifest
        # must retrieve every replica from a given source, using multiple paged
        # requests to OpenSearch if necessary.
        source_ids = self.source_ids()
        page_index = not_none(partition.page_index)
        source_id = source_ids[page_index]
        log.info('Listing replicas from source %r for manifest page %d',
                 source_id, page_index)
        partition_filter: FiltersJSON = {self.source_id_field.name: {'is': [source_id]}}
        original_filters = self.filters
        try:
            self.filters = original_filters.update(partition_filter)
            replicas = self._list_replicas()
            for replica in replicas:
                entry = {
                    'value': replica['contents'],
                    'type': replica['replica_type']
                }
                json.dump(entry, output)
                output.write('\n')
        finally:
            self.filters = original_filters
        last_page = len(source_ids) - 1
        if page_index < last_page:
            return partition.next_page(file_name=None, search_after=None)
        elif page_index == last_page:
            return partition.last_page()
        else:
            assert False, (partition, source_ids)


class PFBVerbatimManifestGenerator(FileBasedManifestGenerator,
                                   VerbatimManifestGenerator):

    @property
    def content_type(self) -> str:
        return 'application/octet-stream'

    @classmethod
    def file_name_extension(cls):
        return 'avro'

    @classmethod
    def format(cls) -> ManifestFormat:
        return ManifestFormat.verbatim_pfb

    def _include_relations(self, replica: JSON) -> bool:
        # Terra will reject the handover if the manifest includes
        # dangling relations, i.e., if any entity references another
        # entity that isn't included in the manifest. There are three
        # known cases where dangling relations can occur (note that
        # currently only the AnVIL plugins support adding relations
        # to the manifest):
        #
        # 1. If an entity occurs in both a replica bundle and a primary
        #    bundle, but only the replica bundle is indexed, its
        #    referenced entities may be missing from the index (and
        #    consequently from the manifest). This can only occur when
        #    the deployment is configured to index snapshots using a
        #    common prefix. See
        #    https://github.com/DataBiosphere/azul/issues/6843
        #
        # 2. When using a filter that matches some but not all of the
        #    files derived from a particular activity, the activity will
        #    be left with dangling relations to the derived files that
        #    didn't match the filter.
        #
        # 3. The `anvil_assayactivity` table includes a foreign key into
        #    the `anvil_antibody` table. We only index replicas from the
        #    latter as orphans, so replicas from the former can include
        #    dangling relations when orphans are not included.
        #    See https://github.com/DataBiosphere/azul/issues/4440
        #
        # (1) can only occur when orphans are included, and (2) and (3)
        # can only occur when orphans are *not* included.
        #
        source = json_mapping(replica['source'])
        prefix = Prefix.parse(json_str(source['prefix']))
        return (
            config.enable_verbatim_relations
            and self.include_orphans
            and not prefix.common
        )

    def create_file(self) -> tuple[str, str | None]:
        replicas = list(self._list_replicas())
        plugin = self.metadata_plugin
        replica_schemas = plugin.verbatim_pfb_schema(replicas)
        # FIXME: Move injection of snapshot ID field to metadata plugin
        #        https://github.com/DataBiosphere/azul/issues/7411
        if config.is_anvil_enabled(self.catalog):
            for replica in replicas:
                source = json_dict(replica['source'])
                source_id = json_str(source['id'])
                contents = json_dict(replica['contents'])
                contents['source_datarepo_snapshot_id'] = source_id
            for schema in replica_schemas:
                schema_from_column = getattr(plugin, '_pfb_schema_from_anvil_column')
                field_schema: MutableJSON = schema_from_column(table_name=schema['name'],
                                                               column_name='source_datarepo_snapshot_id',
                                                               anvil_datatype='string',
                                                               is_optional=False)
                fields = json_list_of_dicts(schema['fields'])
                insort(fields, field_schema, key=itemgetter('name'))
        # Ensure field order is consistent for unit tests
        replica_schemas.sort(key=itemgetter('name'))
        links = {
            replica_type: plugin.verbatim_pfb_links(replica_type)
            for replica_type in ([json_str(s['name']) for s in replica_schemas])
        }
        pfb_metadata_entity = avro_pfb.pfb_metadata_entity(links)
        pfb_schema = avro_pfb.avro_pfb_schema(replica_schemas)

        def pfb_entities():
            yield pfb_metadata_entity
            for replica in replicas:
                id = plugin.verbatim_pfb_entity_id(replica)
                entity = avro_pfb.PFBEntity.for_replica(id, dict(replica))
                # The inclusion of relations is determined on a case-by-case
                # basis for each replica, which may result in inconsistent
                # expression of relations across rows in the same manifest.
                # We chose this approach because scanning all replicas in
                # advance would present another obstacle to our goal of
                # parallelizing the manifest generation.
                if self._include_relations(replica):
                    relations = plugin.verbatim_pfb_relations(replica)
                    entity_relations = [
                        PFBRelation(dst_name=replica_type, dst_id=entity_id)
                        for replica_type, entity_id in relations
                    ]
                else:
                    entity_relations = []
                yield entity.to_json(entity_relations)

        fd, path = mkstemp(suffix=f'.{self.file_name_extension()}')
        os.close(fd)
        avro_pfb.write_pfb_entities(pfb_entities(), pfb_schema, path)
        return path, None
