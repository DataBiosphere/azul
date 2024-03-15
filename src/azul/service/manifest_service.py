from abc import (
    ABCMeta,
    abstractmethod,
)
import base64
from collections import (
    defaultdict,
)
from collections.abc import (
    Iterable,
    Mapping,
)
from copy import (
    deepcopy,
)
import csv
from datetime import (
    datetime,
    timedelta,
    timezone,
)
import email.utils
from hashlib import (
    sha256,
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
    TemporaryDirectory,
    mkstemp,
)
import time
from typing import (
    Any,
    IO,
    Optional,
    Protocol,
    Self,
    Type,
    cast,
)
import unicodedata
from uuid import (
    UUID,
    uuid5,
)

import attrs
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
import msgpack
from werkzeug.http import (
    parse_dict_header,
)

from azul import (
    CatalogName,
    RequirementError,
    cached_property,
    config,
    mutable_furl,
)
from azul.attrs import (
    is_uuid,
    strict_auto,
)
from azul.auth import (
    Authentication,
)
from azul.bytes import (
    azul_urlsafe_b64decode,
    azul_urlsafe_b64encode,
)
from azul.deployment import (
    aws,
)
from azul.indexer.document import (
    FieldPath,
    FieldTypes,
    null_str,
)
from azul.json import (
    copy_json,
)
from azul.json_freeze import (
    freeze,
    sort_frozen,
)
from azul.plugins import (
    ColumnMapping,
    DocumentSlice,
    ManifestConfig,
    ManifestFormat,
    MutableManifestConfig,
    RepositoryPlugin,
    dotted,
)
from azul.service import (
    FileUrlFunc,
    Filters,
    avro_pfb,
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
    AnyJSON,
    FlatJSON,
    JSON,
    JSONs,
    MutableJSON,
)
from azul.vendored.frozendict import (
    frozendict,
)

log = logging.getLogger(__name__)


class ManifestUrlFunc(Protocol):

    def __call__(self,
                 *,
                 fetch: bool = True,
                 token_or_key: Optional[str] = None,
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
class BareManifestKey(AbstractManifestKey):
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
    ...                                format=ManifestFormat.terra_bdbag,
    ...                                manifest_hash=UUID('d2b0ce3c-46f0-57fe-b9d4-2e38d8934fd4'),
    ...                                source_hash=UUID('77936747-5968-588e-809f-af842d6be9e0'))
    >>> len(manifest_key.encode())
    154
    """
    catalog: CatalogName = strict_auto()
    format: ManifestFormat = strict_auto()
    manifest_hash: UUID = attrs.field(validator=is_uuid(5))
    source_hash: UUID = attrs.field(validator=is_uuid(5))

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
    def unpack(cls, pack: bytes) -> None:
        """
        Do not call this method. It is unsafe to deserialize an instance of
        this class. Instead, deserialize a :class:`SignedManifestKey` and use
        :meth:`ManifestService.verify_manifest_key_signature`.
        """
        assert False

    def to_json(self) -> JSON:
        return {
            'catalog': self.catalog,
            'format': self.format.value,
            'manifest_hash': str(self.manifest_hash),
            'source_hash': str(self.source_hash)
        }

    @classmethod
    def from_json(cls, json: JSON) -> 'ManifestKey':
        return cls(catalog=json['catalog'],
                   format=ManifestFormat(json['format']),
                   manifest_hash=UUID(json['manifest_hash']),
                   source_hash=UUID(json['source_hash']))

    @property
    def hash(self) -> bytes:
        return sha256(self.pack()).digest()


@attrs.frozen
class InvalidManifestKeySignature(Exception):
    value: SignedManifestKey


@attrs.frozen(kw_only=True)
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
    format: ManifestFormat

    #: The key under which the manifest is stored
    manifest_key: ManifestKey

    #: The proposed file name of the manifest when downloading it to a user's
    #: system
    file_name: str

    def to_json(self) -> JSON:
        return {
            'location': self.location,
            'was_cached': self.was_cached,
            'format': self.format.value,
            'manifest_key': self.manifest_key.to_json(),
            'file_name': self.file_name
        }

    @classmethod
    def from_json(cls, json: JSON) -> 'Manifest':
        return cls(location=json['location'],
                   was_cached=json['was_cached'],
                   format=ManifestFormat(json['format']),
                   manifest_key=ManifestKey.from_json(json['manifest_key']),
                   file_name=json['file_name'])


def tuple_or_none(v):
    return v if v is None else tuple(v)


@attrs.frozen(kw_only=True)
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
    config: Optional[AnyJSON] = None

    #: The ID of the S3 multi-part upload this partition is a part of. If a
    #: manifest consists of just one partition, this may be None, but it doesn't
    #: have to be.
    multipart_upload_id: Optional[str] = None

    #: The S3 ETag of each partition; the current one and all the ones before it
    part_etags: Optional[tuple[str, ...]] = attrs.field(converter=tuple_or_none,
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
    search_after: Optional[tuple[str, str]] = None

    @classmethod
    def from_json(cls, partition: JSON) -> 'ManifestPartition':
        return cls(**{
            k: tuple(v) if k == 'search_after' and v is not None else v
            for k, v in partition.items()
        })

    def to_json(self) -> MutableJSON:
        return attrs.asdict(self)

    @classmethod
    def first(cls) -> 'ManifestPartition':
        return cls(index=0,
                   is_last=False)

    @property
    def is_first(self):
        return not (self.index or self.page_index)

    def with_config(self, config: AnyJSON):
        return attrs.evolve(self, config=config)

    def with_upload(self, multipart_upload_id) -> 'ManifestPartition':
        return attrs.evolve(self,
                            multipart_upload_id=multipart_upload_id,
                            part_etags=())

    def first_page(self) -> 'ManifestPartition':
        assert self.index == 0, self
        return attrs.evolve(self,
                            page_index=0,
                            is_last_page=False)

    def next_page(self,
                  file_name: Optional[str],
                  search_after: tuple[str, str]
                  ) -> 'ManifestPartition':
        assert self.page_index is not None, self
        # If different pages yield different file names, use default file name
        if self.page_index > 0:
            if file_name != self.file_name:
                file_name = None
        return attrs.evolve(self,
                            page_index=self.page_index + 1,
                            file_name=file_name,
                            search_after=search_after)

    def last_page(self):
        return attrs.evolve(self, is_last_page=True)

    def next(self, part_etag: str) -> 'ManifestPartition':
        return attrs.evolve(self,
                            index=self.index + 1,
                            part_etags=(*self.part_etags, part_etag))

    def last(self, file_name: str) -> 'ManifestPartition':
        return attrs.evolve(self,
                            file_name=file_name,
                            is_last=True)


@attrs.frozen
class CachedManifestNotFound(Exception):
    manifest_key: ManifestKey


class ManifestService(ElasticsearchService):

    def __init__(self, storage_service: StorageService, file_url_func: FileUrlFunc):
        super().__init__()
        self.storage_service = storage_service
        self.file_url_func = file_url_func

    def get_manifest(self,
                     *,
                     format: ManifestFormat,
                     catalog: CatalogName,
                     filters: Filters,
                     partition: ManifestPartition,
                     manifest_key: Optional[ManifestKey] = None
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
                           generator: 'ManifestGenerator',
                           manifest_key: ManifestKey,
                           partition: ManifestPartition
                           ) -> Manifest | ManifestPartition:
        partition = generator.write(manifest_key, partition)
        if partition.is_last:
            return self._presign_manifest(generator_cls=type(generator),
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
                                        KeyId=config.manifest_kms_alias,
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
            response = aws.kms.verify_mac(KeyId=config.manifest_kms_alias,
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
                             generator_cls: Type['ManifestGenerator'],
                             manifest_key: ManifestKey
                             ) -> Manifest:
        file_name = self._get_cached_manifest_file_name(generator_cls, manifest_key)
        if file_name is None:
            raise CachedManifestNotFound(manifest_key)
        else:
            return self._presign_manifest(generator_cls=generator_cls,
                                          manifest_key=manifest_key,
                                          file_name=file_name,
                                          was_cached=True)

    def _presign_manifest(self,
                          generator_cls: Type['ManifestGenerator'],
                          manifest_key: ManifestKey,
                          file_name: Optional[str],
                          was_cached: bool
                          ) -> Manifest:
        if not generator_cls.use_content_disposition_file_name:
            file_name = None
        object_key = generator_cls.s3_object_key(manifest_key)
        presigned_url = self.storage_service.get_presigned_url(object_key, file_name)
        return Manifest(location=presigned_url,
                        was_cached=was_cached,
                        format=generator_cls.format(),
                        manifest_key=manifest_key,
                        file_name=file_name)

    file_name_tag = 'azul_file_name'

    def _get_cached_manifest_file_name(self,
                                       generator_cls: Type['ManifestGenerator'],
                                       manifest_key: ManifestKey
                                       ) -> Optional[str]:
        """
        Return the proposed local file name of the manifest with the given
        object key if it was previously created, still exists in the bucket, and
        won't be expiring soon. Otherwise return None.

        :param generator_cls: The generator class of the manifest

        :param manifest_key: The key of the cached manifest
        """
        object_key = generator_cls.s3_object_key(manifest_key)
        try:
            response = self.storage_service.head(object_key)
        except self.storage_service.client.exceptions.ClientError as e:
            if int(e.response['Error']['Code']) == 404:
                log.info('Cached manifest not found: %s', manifest_key)
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
                    # Can't be absent under S3's strong consistency
                    assert False, (object_key, self.file_name_tag)
                else:
                    encoded_file_name = encoded_file_name.encode('ascii')
                    return base64.urlsafe_b64decode(encoded_file_name).decode('utf-8')
            else:
                log.info('Cached manifest is about to expire: %s', object_key)
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
            log.debug('Manifest object expires in %s seconds, on %s', expiry_seconds, expiry_datetime)
        else:
            log.error('The actual object expiration (%s) does not match expected value (%s)',
                      expiration, expected_date)
        return expiry_seconds

    def command_lines(self,
                      manifest: Optional[Manifest],
                      url: furl,
                      authentication: Optional[Authentication]
                      ) -> FlatJSON:
        format = None if manifest is None else manifest.format
        generator_cls = ManifestGenerator.cls_for_format(format)
        file_name = None if manifest is None else manifest.file_name
        return generator_cls.command_lines(url, file_name, authentication)


Cells = dict[str, str]


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
        ]

    _cls_for_format: dict[ManifestFormat, Type['ManifestGenerator']] = {}

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
        assert '"' not in s, s
        assert '\\' not in s, s
        return f'"{s}"'

    @classmethod
    def command_lines(cls,
                      url: furl,
                      file_name: Optional[str],
                      authentication: Optional[Authentication]
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
        git_commit = config.lambda_git_status['commit']
        filter_string = repr(sort_frozen(freeze(self.filters.explicit)))
        content_hash = str(self.manifest_content_hash)
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

    @classmethod
    def file_name(cls,
                  manifest_key: ManifestKey,
                  base_name: Optional[str] = None
                  ) -> str:
        if base_name:
            file_name_prefix = unicodedata.normalize('NFKD', base_name)
            file_name_prefix = re.sub(r'[^\w ,.@%&\-_()\\[\]/{}]', '_', file_name_prefix).strip()
            timestamp = datetime.now().strftime('%Y-%m-%d %H.%M')
            file_name = f'{file_name_prefix} {timestamp}.{cls.file_name_extension()}'
        else:
            file_name = 'hca-manifest-' + cls.s3_object_key_base(manifest_key)
        return file_name

    def _create_request(self) -> Search:
        pipeline = self._create_pipeline()
        request = self.service.create_request(self.catalog, self.entity_type)
        request = pipeline.prepare_request(request)
        # The response is processed by the generator, not the pipeline
        return request

    def _create_pipeline(self):
        if self.included_fields is None:
            document_slice = DocumentSlice()
        else:
            document_slice = DocumentSlice(includes=list(map(dotted, self.included_fields)))
        pipeline = self.service.create_chain(catalog=self.catalog,
                                             entity_type=self.entity_type,
                                             filters=self.filters,
                                             post_filter=False,
                                             document_slice=document_slice)
        return pipeline

    def _hit_to_doc(self, hit: Hit) -> MutableJSON:
        return self.service.translate_fields(self.catalog,
                                             hit.to_dict(),
                                             forward=False,
                                             allowed_paths=self.included_fields)

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
            field_types = field_types[field]

        def convert(field_name, field_value):
            try:
                field_type = field_types[field_name]
            except KeyError:
                if field_name == 'file_url':
                    field_type = null_str
                else:
                    raise
            else:
                if isinstance(field_type, list):
                    field_type = one(field_type)
            return field_type.to_tsv(field_value)

        def validate(field_value: str) -> str:
            assert self.column_joiner not in field_value
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
            d = d.get(key, {})
        entities = d.get(field_path[-1], [])
        return entities

    def _azul_file_url(self,
                       file: JSON,
                       args: Mapping = frozendict()
                       ) -> Optional[str]:
        download_cls = self.repository_plugin.file_download_class()
        if download_cls.needs_drs_uri and file['drs_uri'] is None:
            return None
        else:
            return str(self.file_url_func(catalog=self.catalog,
                                          file_uuid=file['uuid'],
                                          version=file['version'],
                                          fetch=False,
                                          **args))

    @cached_property
    def manifest_content_hash(self) -> int:
        log.debug('Computing content hash for manifest using filters %r ...', self.filters)
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
        log.info('Manifest content hash %i was computed in %.3fs using filters %r.',
                 hash_value, time.time() - start_time, self.filters)
        return hash_value

    def tagging(self, file_name: Optional[str]) -> Optional[Mapping[str, str]]:
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
    def storage(self):
        return self.service.storage_service


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
              manifest_key: ManifestKey,
              partition: ManifestPartition,
              ) -> ManifestPartition:
        assert not partition.is_last, partition
        if partition.config is None:
            # The keys in manifest config are tuples which aren't allowed in
            # JSON. We convert the outer mapping to a list of entries.
            config = [[list(k), v] for k, v in self.manifest_config.items()]
            partition = partition.with_config(config)
        else:
            config = {tuple(k): v for k, v in partition.config}
            type(self).manifest_config.fset(self, config)
        object_key = self.s3_object_key(manifest_key)
        if partition.multipart_upload_id is None:
            upload = self.storage.create_multipart_upload(object_key)
            partition = partition.with_upload(upload.id)
        else:
            upload = self.storage.load_multipart_upload(object_key=object_key,
                                                        upload_id=partition.multipart_upload_id)
        if partition.page_index is None:
            partition = partition.first_page()
        with BytesIO() as buffer:
            with TextIOWrapper(buffer, encoding='utf-8', write_through=True) as text_buffer:
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
                    file_name = self.file_name(manifest_key, base_name=partition.file_name)
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

    def _search_after(self, hit: Hit) -> tuple[str, str]:
        a, b = hit.meta.sort
        return a, b


class FileBasedManifestGenerator(ManifestGenerator):
    """
    A manifest generator that writes its output to a file.

    :return: the path to the file containing the output of the generator and an
             optional string that should be used to name the output when
             persisting it to an object store or another file system
    """

    @abstractmethod
    def create_file(self) -> tuple[str, Optional[str]]:
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
    def entity_type(self) -> str:
        return 'files'

    @cached_property
    def included_fields(self) -> list[FieldPath] | None:
        return [
            *super().included_fields,
            ('contents', 'files', 'related_files')
        ]

    @classmethod
    def command_lines(cls,
                      url: furl,
                      file_name: Optional[str],
                      authentication: Optional[Authentication]
                      ) -> FlatJSON:
        authentication_option = [] if authentication is None else [
            '--header',
            cls._option(authentication.as_http_header())
        ]
        manifest_options = [
            '--location',
            '--fail',
        ]
        file_options = [
            '--fail-early',  # Exit curl with error on the first failure encountered
            '--continue-at -',  # Resume partially downloaded files
            # We want curl to make enough retries so that it waits a total of
            # one and a half times the evaluation period of the WAF rate rule,
            # long enough for the tripped rule to clear.
            f'--retry {ceil(config.waf_rate_rule_period * 1.5 / config.waf_rate_rule_retry_after)}',
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
                'drsUri': file['drs_uri']
            } if is_related_file else {
            }

            file_url = self._azul_file_url(file, args)
            if file_url is None:
                output.write(f"# File {file['uuid']!r}, version {file['version']!r} is "
                             f"currently not available in catalog {self.catalog!r}.\n\n")
            else:
                # To prevent overwriting one file with another one of the same name
                # but different content we nest each file in a folder using the
                # bundle UUID. Because a file can belong to multiple bundles we use
                # the one with the most recent version.
                bundle = max(cast(JSONs, doc['bundles']), key=itemgetter('version', 'uuid'))
                output_name = self._sanitize_path(bundle['uuid'] + '/' + name)
                output.write(f'url={self._option(file_url)}\n'
                             f'output={self._option(output_name)}\n\n')

        if partition.page_index == 0:
            curl_options = [
                '--create-dirs',  # Allow curl to create folders
                '--compressed',  # Request a compressed response
                '--location',  # Follow redirects
                '--globoff',  # Prevent '#' in file names from being interpreted as output variables
                '--fail',  # Upon server error don't save the error message to the file
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
                file = one(cast(JSONs, doc['contents']['files']))
                _write(file)
                for related_file in file['related_files']:
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

    @classmethod
    def file_name_extension(cls):
        return 'tsv'

    @property
    def entity_type(self) -> str:
        return 'files'

    @cached_property
    def included_fields(self) -> list[FieldPath] | None:
        return [
            *super().included_fields,
            ('contents', 'files', 'related_files')
        ]

    def write_page_to(self,
                      partition: ManifestPartition,
                      output: IO[str]
                      ) -> ManifestPartition:
        column_mappings = self.manifest_config.values()
        column_names = list(chain.from_iterable(d.values() for d in column_mappings))
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
                contents = doc['contents']
                if len(project_short_names) < 2 and 'projects' in contents:
                    project = one(cast(JSONs, contents['projects']))
                    short_names = project['project_short_name']
                    project_short_names.update(short_names)
                row = {}
                related_rows = []
                for field_path, column_mapping in self.manifest_config.items():
                    entities = self._get_entities(field_path, doc)
                    if field_path == ('contents', 'files'):
                        file = copy_json(one(entities))
                        file['file_url'] = self._azul_file_url(file)
                        entities = [file]
                    self._extract_fields(field_path=field_path,
                                         entities=entities,
                                         column_mapping=column_mapping,
                                         row=row)
                    if field_path == ('contents', 'files'):
                        file = copy_json(one(entities))
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
        request = self._create_request()
        request = request.params(preserve_order=True).sort('entity_id.keyword')
        for hit in request.scan():
            doc = self._hit_to_doc(hit)
            yield doc

    def create_file(self) -> tuple[str, Optional[str]]:
        transformers = self.service.transformer_types(self.catalog)
        transformer = one(t for t in transformers if t.entity_type() == 'files')
        field_types = transformer.field_types()
        pfb_schema = avro_pfb.pfb_schema_from_field_types(field_types)

        converter = avro_pfb.PFBConverter(pfb_schema, self.repository_plugin)
        for doc in self._all_docs_sorted():
            converter.add_doc(doc)

        entity = avro_pfb.pfb_metadata_entity(field_types)
        entities = itertools.chain([entity], converter.entities())

        fd, path = mkstemp(suffix='.avro')
        os.close(fd)
        avro_pfb.write_pfb_entities(entities, pfb_schema, path)
        return path, None


class BDBagManifestGenerator(FileBasedManifestGenerator):

    @classmethod
    def format(cls) -> ManifestFormat:
        return ManifestFormat.terra_bdbag

    @classmethod
    def file_name_extension(cls):
        return 'zip'

    @property
    def content_type(self) -> str:
        return 'application/octet-stream'

    @property
    def entity_type(self) -> str:
        return 'files'

    @cached_property
    def included_fields(self) -> list[FieldPath] | None:
        return [
            *super().included_fields,
            ('contents', 'files', 'drs_uri')
        ]

    @classmethod
    def use_content_disposition_file_name(cls) -> bool:
        # Apparently, Terra does not like the content disposition header
        return False

    @cached_property
    def manifest_config(self) -> ManifestConfig:
        return {
            field_path: {
                field_name: column_name.replace('.', self.column_path_separator)
                for field_name, column_name in column_mapping.items()
            }
            for field_path, column_mapping in super().manifest_config.items()
        }

    def create_file(self) -> tuple[str, Optional[str]]:
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
        files it contains (e.g. a primary bundle is made redundant by its derived
        analysis bundle if the primary only has a subset of files that the
        analysis bundle contains or if they both have the same files).
        """
        redundant_keys = set()
        # Get a forward mapping of bundle FQID to a set of file uuid
        bundle_to_file: dict[FQID, set[str]] = defaultdict(set)
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
            related_bundles: set[FQID] = set(fqid_b
                                             for file in files_a
                                             for fqid_b in file_to_bundle[file]
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
            file = copy_json(one(cast(JSONs, doc['contents']['files'])))
            file['file_url'] = self._azul_file_url(file)
            file_cells = {}
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

                # Register the three extracted sets of fields as a group for
                # this bundle and qualifier
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
                qualifier = f'{qualifier}_{index}'
            return f'{self.column_path_separator}{qualifier}{self.column_path_separator}{column_name}'

        num_groups_per_qualifier = defaultdict(int)

        # Track the max number of groups for each qualifier in any bundle
        for bundle in bundles.values():
            for qualifier, groups in bundle.items():
                # Sort the groups by reversed file name. This essentially sorts
                # by file extension and any other more general suffixes
                # preceding the extension. It ensures that `patient1_qc.bam` and
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
            *(d.values() for d in other_column_mappings.values())
        ))

        # Add file columns for each qualifier and group
        for qualifier, num_groups in sorted(num_groups_per_qualifier.items()):
            for index in range(num_groups):
                for column_name in file_column_mapping.values():
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
                            # Cells from other entities need to be concatenated.
                            # Note that for fields that differ between the files
                            # in a bundle this algorithm retains the values but
                            # loses the association between each individual
                            # value and the respective file.
                            for column_name, cell_value in cells.items():
                                row.setdefault(column_name, set()).update(cell_value.split(self.padded_joiner))
                        elif entity == 'file':
                            # Since file-specific cells are placed into
                            # qualified columns, no concatenation is necessary
                            index = None if num_groups_per_qualifier[qualifier] == 1 else i
                            row.update((qualify(qualifier, column_name, index=index), cell)
                                       for column_name, cell in cells.items())
                        else:
                            assert False
            # Join concatenated values using the joiner
            row = {k: self.padded_joiner.join(sorted(v)) if isinstance(v, set) else v for k, v in row.items()}
            bundle_tsv_writer.writerow(row)
