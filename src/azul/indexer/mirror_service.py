from abc import (
    ABCMeta,
    abstractmethod,
)
from functools import (
    singledispatchmethod,
)
import json
import logging
import math
import string
import time
from typing import (
    ClassVar,
    Iterable,
    Iterator,
    Protocol,
    Self,
    final,
)
from uuid import (
    UUID,
    uuid4,
    uuid5,
)

import attr
import attrs
from furl import (
    furl,
)

from azul import (
    CatalogName,
    R,
    cached_property,
    config,
    json_mapping,
    mutable_furl,
)
from azul.attrs import (
    SerializableAttrs,
    devolve,
    serializable,
)
from azul.auth import (
    Authentication,
)
from azul.deployment import (
    aws,
)
from azul.digests import (
    Hasher,
    get_resumable_hasher,
    hasher_from_json,
    hasher_to_json,
)
from azul.drs import (
    AccessMethod,
)
from azul.functions import (
    compose,
)
from azul.http import (
    HasCachedHttpClient,
)
from azul.indexer import (
    SourceConfig,
    SourceRef,
    SourceSpec,
)
from azul.plugins import (
    File,
    RepositoryFileDownload,
    RepositoryPlugin,
)
from azul.queues import (
    Action,
    Queues,
    SQSFifoMessage,
)
from azul.service.source_service import (
    SourceService,
)
from azul.service.storage_service import (
    StorageObjectExists,
    StorageService,
)
from azul.types import (
    JSON,
    json_element_strings,
)

log = logging.getLogger(__name__)


@attrs.frozen(kw_only=True)
class FilePart(SerializableAttrs):
    """
    A part of a mirrored file
    """

    #: The part number, starting at 0 for the first part, unlike S3 API part
    #: numbers, which start at 1.
    #:
    index: int

    #: Offset of the first byte of this part, relative to the start of the file
    #:
    offset: int

    #: The size of this part
    #:
    size: int

    #: Various S3 quotas related to parts and part sizes
    #: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
    #:
    min_size: ClassVar[int] = aws.s3_min_part_size
    max_size: ClassVar[int] = aws.s3_max_part_size
    max_num_parts: ClassVar[int] = aws.s3_max_num_parts

    #: In experiments, we observed a download rate of ~25 MB/s for an AWS Lambda
    #: function downloading from GCS. To leave room for network impairments or
    #: other partial outages, the normal download time for a single part should
    #: be one third of the Lambda function timeout. Also, we heuristically
    #: decided for the part size to not exceed 1 GiB in size in stable
    #: deployments, or 256 MiB in elsewhere.
    #:
    default_size: ClassVar[int] = min(
        1024 ** 3 if config.deployment.is_stable else 256 * 1024 ** 2,
        int(config.mirror_lambda_timeout * 25 * 1024 ** 2 / 3)
    )

    assert min_size <= default_size <= max_size

    @classmethod
    def first(cls, file: File) -> Self:
        """
        The first part of the given file, using the given part size.
        """
        assert file.size is not None, R(
            'File size unknown', file)
        part_count = math.ceil(file.size / cls.default_size)
        assert part_count <= cls.max_num_parts, R(
            'Too many parts', part_count, cls.default_size, file)
        return cls(index=0, offset=0, size=min(cls.default_size, file.size))

    def next(self, file: File) -> Self | None:
        """
        The part following this part in the given file, or None if this is the
        last part.
        """
        assert file.size is not None, R('File size unknown', file)
        next_offset = self.offset + self.size
        if next_offset == file.size:
            return None
        elif 0 < next_offset < file.size:
            next_index = self.index + 1
            next_size = min(self.size, file.size - next_offset)
            return attr.evolve(self, index=next_index, offset=next_offset, size=next_size)
        else:
            assert False, R('Part range exceeds file size', self, file)


@attrs.frozen(kw_only=True)
class MirrorFileDownload(RepositoryFileDownload):
    _location: str

    @property
    def retry_after(self) -> int | None:
        return None

    @property
    def location(self) -> str | None:
        return self._location

    def update(self,
               plugin: RepositoryPlugin,
               authentication: Authentication | None
               ) -> None:
        pass


class SchemaUrlFunc(Protocol):

    def __call__(self, *, schema_name: str, version: int) -> mutable_furl: ...


@attrs.frozen(kw_only=True)
class MirrorAction(Action, metaclass=ABCMeta):
    catalog: CatalogName

    #: When performing a mirror action results in more secondary actions, this
    #: field should be copied from the action being performed to those secondary
    #: actions. When constructing primary actions, this field should be omitted
    #: so that the default is used. The indirection via a class method for the
    #: default factory allows for easy patching during unit tests.
    #:
    operation_id: str = attrs.field(factory=lambda: MirrorAction._operation_id())

    @classmethod
    def _operation_id(cls):
        return str(uuid4())

    @property
    @abstractmethod
    def group_id(self) -> tuple[str, ...]:
        """
        The SQS FIFO message group ID of a message about this action. Messages
        in different groups can be handled in parallel. Messages in the same
        group are handled serially. Use this property to prevent concurrent
        actions against a particular resource, by making the ID of that resource
        the group ID of those actions.
        """
        raise NotImplementedError

    @property
    def dedup_id(self) -> tuple[str, ...]:
        """
        The SQS message deduplication ID to use for a message about this action.
        If two messages with the same deduplication ID are sent within 5 min or
        less of each other, the second message will be discarded silently. Use
        this property to avoid redundant work.
        """
        # Since different catalogs may be configured to handle the same file in
        # different ways, we can't conflate two messages that only differ in
        # the catalog they are targetting.
        return str(type(self)), self.catalog, self.operation_id

    def to_sqs(self) -> SQSFifoMessage:
        return SQSFifoMessage(body=json_mapping(self.to_json()),
                              group_id=self._make_id(self.group_id),
                              dedup_id=self._make_id(self.dedup_id))

    dedup_uuid_namespace = UUID('cb3a5301-5ad4-44f4-9020-cd34e7c61d3e')

    def _make_id(self, id: tuple[str, ...]) -> str:
        joiner = ':'
        assert not any(joiner in s for s in id)
        return str(uuid5(self.dedup_uuid_namespace, joiner.join(id)))


@attrs.frozen(kw_only=True)
class MirrorSourceAction(MirrorAction):
    source: SourceRef

    @property
    def group_id(self) -> tuple[str, ...]:
        return self.source.id,

    @property
    def dedup_id(self) -> tuple[str, ...]:
        return *super().dedup_id, self.source.id


@attrs.frozen(kw_only=True)
class MirrorPartitionAction(MirrorSourceAction):
    prefix: str

    @property
    def group_id(self) -> tuple[str, ...]:
        return *super().group_id, self.prefix

    @property
    def dedup_id(self) -> tuple[str, ...]:
        return *super().dedup_id, self.prefix


@attrs.frozen(kw_only=True)
class MirrorFileAction(MirrorPartitionAction):
    file: File

    @property
    @final
    def group_id(self) -> tuple[str, ...]:
        # This method is final because we need to serialize all actions that
        # target a specific file in the mirror bucket.
        digest = self.file.digest
        return digest.type, digest.value

    @property
    def dedup_id(self) -> tuple[str, ...]:
        return *super().dedup_id, self.file.uuid,


@attrs.frozen(kw_only=True)
class FileUpload(SerializableAttrs):
    upload_id: str
    etags: list[str] = serializable(to_json=list,
                                    from_json=compose(list, json_element_strings))
    hasher: Hasher = serializable(to_json=hasher_to_json,
                                  from_json=hasher_from_json)

    def copy(self) -> Self:
        return attrs.evolve(self,
                            etags=self.etags.copy(),
                            hasher=self.hasher.copy())


@attrs.frozen(kw_only=True)
class MultiPartUploadAction(MirrorFileAction):
    upload: FileUpload


@attrs.frozen(kw_only=True)
class MirrorPartAction(MultiPartUploadAction):
    part: FilePart

    @property
    def dedup_id(self) -> tuple[str, ...]:
        return *super().dedup_id, str(self.part.index)


class FinalizeFileAction(MultiPartUploadAction):
    pass


@attrs.frozen(kw_only=True, slots=False)
class BaseMirrorService:
    """
    Service for queuing mirroring work, e.g., sending action messages, and
    reading mirrored files. The most prominent reader of mirrored files is the
    service app.
    """

    catalog: CatalogName

    @cached_property
    def _queues(self) -> Queues:
        return Queues()

    @property
    def repository_plugin(self) -> RepositoryPlugin:
        return self._source_service.repository_plugin(self.catalog)

    @cached_property
    def _storage(self) -> StorageService:
        bucket = config.mirror_bucket
        if bucket is None or self.catalog in config.integration_test_catalogs:
            bucket = aws.mirror_bucket
        return StorageService(bucket)

    @cached_property
    def _source_service(self) -> SourceService:
        return SourceService()

    def may_mirror_files_from_source(self, source_spec: SourceSpec) -> bool:
        """
        Test whether it makes sense to request the mirroring of files from the
        given source. If this method returns True, files from the source may or
        may not be mirrored. If this method returns False, the service will
        definitely refuse to mirror all files from the source.
        """
        if self.may_mirror():
            plugin = self.repository_plugin
            source_config = plugin.sources[source_spec]
            if source_config.mirror:
                is_public = any(
                    source_spec == source.spec
                    for source in self._source_service.configured_public_sources
                )
                return is_public
            else:
                return False
        else:
            return False

    def may_mirror(self, file_size: int = 0) -> bool:
        """
        Test whether it makes sense to request the mirroring of files from the
        current catalog if they are of the given size or larger. If this method
        returns True, such files may or may not be mirrored. If this method
        returns False, the service will definitely refuse to mirror such files,
        although it may accept smaller files.
        """
        if config.enable_mirroring:
            max_size = config.catalogs[self.catalog].mirror_limit
            return max_size is None or file_size <= max_size
        else:
            return False

    def mirror_sources(self, sources: Iterable[tuple[SourceRef, SourceConfig]]):
        if self.may_mirror():
            def actions():
                for source, source_config in sources:
                    if source_config.mirror:
                        log.info('Mirroring files in source %r from catalog %r',
                                 str(source.spec), self.catalog)
                        yield MirrorSourceAction(catalog=self.catalog, source=source)
                    else:
                        log.info('Not mirroring any files in source %r from catalog %r because '
                                 'mirroring is explicitly disabled',
                                 str(source.spec), self.catalog)

            self._queue_actions(actions())
        else:
            log.info('Not mirroring any files in catalog %r because the file '
                     'size limit is negative', self.catalog)

    def mirror_file(self, source: SourceRef, file: File):
        def actions():
            yield MirrorFileAction(catalog=self.catalog,
                                   source=source,
                                   prefix='',
                                   file=file)

        self._queue_actions(actions())

    def _mirror_queue(self):
        name = config.mirror_queue.name
        return aws.sqs_queue(name)

    def _queue_actions(self, actions: Iterator[MirrorAction]) -> int:
        rate_limit = float(aws.sqs_fifo_rate_limit)
        if config.is_in_lambda:
            rate_limit /= config.mirroring_concurrency
        return self._queues.send_messages(self._mirror_queue(),
                                          map(MirrorAction.to_sqs, actions),
                                          rate_limit=rate_limit)

    #: Since we track the ETags of all parts of a multipart file in SQS
    #: messages, the maximum file size is primarily constrained by the SQS
    #: message size. We observe ETags to be 32-byte hexadecimal strings which,
    #: if represented in a JSON array, take up 35 bytes per item, 36 if the
    #: comma is followed by a space. This limits the number of ETags we can
    #: store in a message, while leaving room for 64 KiB of other information,
    #: and thereby also limits the maximum size of files we can mirror.
    #:
    max_file_size = (
        FilePart.default_size
        * (aws.sqs_max_message_size - 64 * 1024) / 36
    )

    # We should be able to copy files 1.5 TiB in size or larger. Currently, the
    # largest file in the open-access datasets for AnVIL is 1.3 TiB.
    #
    assert 1.5 * 1024 ** 4 <= max_file_size

    def mirror_uri(self,
                   source: SourceSpec,
                   file_cls: type[File],
                   file_json: JSON
                   ) -> str | None:
        """
        Return the the URI of the mirror copy of the given file from the current
        catalog. If this method returns None, the file was not mirrored, and no
        such URI exists. Otherwise, a mirror copy of the file may or may not
        exist under the returned URI.

        :param source: The source of the file

        :param file_cls: The type of the file. This parameter is needed in order
                         to avoid deserializing a file from a source that was
                         configured to not be mirrored because the file metadata
                         in that source is incomplete or broken

        :param file_json: the index representation of the file
        """
        if self.may_mirror_files_from_source(source):
            file = file_cls.from_index(file_json)
            if self.may_mirror(0 if file.size is None else file.size):
                return str(furl(scheme='s3',
                                netloc=self._storage.bucket_name,
                                path=self._file_object_key(file)))
            else:
                return None
        else:
            return None

    def mirror_url(self, file: File) -> str:
        return self._storage.get_presigned_url(object_key=self._file_object_key(file),
                                               file_name=file.name,
                                               content_type=file.content_type)

    def info_exists(self, file: File) -> bool:
        return self._storage.object_exists(self._info_object_key(file))

    def _file_exists(self, file: File) -> bool:
        return self._storage.object_exists(self._file_object_key(file))

    info_prefix, file_prefix = 'info', 'file'

    def _info_object_key(self, file: File) -> str:
        return self._object_key(self.info_prefix, file, extension='.json')

    def _file_object_key(self, file: File) -> str:
        return self._object_key(self.file_prefix, file)

    def _object_key(self, prefix: str, file: File, *, extension: str = '') -> str:
        digest = file.digest
        digest_value = digest.value.lower()
        assert all(c in string.hexdigits for c in digest_value), R(
            'Expected a hexadecimal digest', digest)
        mirror_prefix = self._mirror_prefix
        return f'{mirror_prefix}{prefix}/{digest_value}.{digest.type}{extension}'

    @cached_property
    def _mirror_prefix(self) -> str:
        return '_it/' if self.catalog in config.integration_test_catalogs else ''

    def delete_it_files(self):
        """
        Delete all objects (both file/ and info/) with the given catalog's
        mirror prefix. Currently, the mirror prefix is only used to distinguish
        IT catalogs from non-IT catalogs, so if an IT catalog is specified,
        objects from *all* IT catalogs will be deleted, not just the specified
        catalog.
        """
        assert self.catalog in config.integration_test_catalogs, R(
            'Not an IT catalog', self.catalog)
        prefix = self._mirror_prefix
        assert len(prefix) > 1 and prefix.endswith('/'), prefix
        object_keys = self._storage.list_objects(prefix)
        assert len(object_keys) <= 300, R('Too many objects', len(object_keys))
        self._storage.delete_objects(object_keys, batch_size=100)


@attrs.frozen(kw_only=True, slots=False)
class MirrorService(BaseMirrorService, HasCachedHttpClient):
    """
    Service that carries out mirroring work. Requires a mechanism to compose
    schema URLs. This function is currently offered by the indexer app, so
    another way to view this service class is as an encapsulation of the
    mirroring work done by the indexer app.
    """

    _schema_url_func: SchemaUrlFunc

    # We don't store the mirrored files' actual content type(s) in S3's
    # `Content-Type` metadata because a single file object may store the
    # contents of multiple file metadata entities, which may declare different
    # content types for the same data. When file objects are downloaded from the
    # mirror bucket via Azul, this value will be overridden with the requested
    # file's actual content type via a query parameter in the signed URL.
    #
    # Files mirrored prior to this change may erroneously specify a different
    # value in the `Content-Type` metadata. We haven't found an efficient way to
    # update the content type of an existing object without copying its data.
    #
    _file_object_content_type = 'application/octet-stream'

    def mirror(self, action: MirrorAction):
        assert action.catalog == self.catalog, R(
            'Action references unexpected catalog', action, self.catalog)
        self._queue_actions(self._mirror(action))

    @singledispatchmethod
    def _mirror(self, a: MirrorAction):
        raise NotImplementedError

    @_mirror.register
    def _(self, a: MirrorSourceAction) -> Iterator[MirrorAction]:
        public_sources = self._source_service.list_accessible_source_ids(self.catalog,
                                                                         authentication=None)
        assert a.source.id in public_sources, R(
            'Cannot mirror non-public source', a.source)
        plugin = self.repository_plugin
        # The desired partition size depends on the maximum number of messages
        # we can send in one Lambda invocation, because queueing the individual
        # mirror_file messages turns out to dominate the running time of
        # handling a mirror_source message.
        partition_size = min(plugin.max_partition_size, int(
            aws.sqs_fifo_rate_limit  # max. # of SendMessage calls per second
            * Queues.batch_size  # number of messages per call
            * config.mirror_lambda_timeout  # max. duration of the invocation
            / config.mirroring_concurrency  # number of concurrent invocations
            / 2  # safety margin
        ))
        source = plugin.partition_source_for_mirroring(a.catalog,
                                                       a.source,
                                                       partition_size)
        prefix = source.prefix
        assert prefix is not None, source
        log.info('Queueing %d partitions of source %r in catalog %r',
                 prefix.num_partitions, str(source.spec), a.catalog)

        for partition in prefix.partition_prefixes():
            yield devolve(MirrorPartitionAction, a, source=source, prefix=partition)

    @_mirror.register
    def _(self, a: MirrorPartitionAction) -> Iterator[MirrorAction]:
        plugin = self.repository_plugin
        files = plugin.list_files(a.source, a.prefix)
        for file in files:
            assert file.size is not None, R('File size unknown', file)
            assert file.size <= self.max_file_size, R(
                'File too big', file, self.max_file_size)
            if self.may_mirror(file.size):
                yield devolve(MirrorFileAction, a, file=file)
            else:
                log.info('Not mirroring file to save cost: %r', file)
        log.info('Queued %d files in partition %r of source %r in catalog %r',
                 len(files), a.prefix, str(a.source), a.catalog)

    @_mirror.register
    def _(self, a: MirrorFileAction) -> Iterator[MirrorAction]:
        assert a.file.size is not None, R('File size unknown', a.file)
        if self.info_exists(a.file):
            log.info('File is already mirrored, skipping upload: %r', a.file)
            self._update_info(a.file)
        elif self._file_exists(a.file):
            assert False, R('File object is already present', a.file)
        else:
            part_size = FilePart.default_size
            if a.file.size <= part_size:
                log.info('Mirroring file via standard upload: %r', a.file)
                self._mirror_file(a.file)
                log.info('Successfully mirrored file via standard upload: %r', a.file)
            else:
                log.info('Mirroring file via multi-part upload: %r', a.file)
                upload = self._create_upload(a.file)
                next_part = self._mirror_first_part(a.file, upload)
                yield devolve(MirrorPartAction, a, upload=upload, part=next_part)

    def _mirror_file(self, file: File):
        """
        Upload the file in a single request. For larger files, use
        :meth:`begin_mirroring_file` instead.
        """
        hasher = get_resumable_hasher(file.digest.type)
        file_content = self._download(file)
        hasher.update(file_content)
        self._verify_digest(file, hasher)
        self._storage.put_object(object_key=self._file_object_key(file),
                                 data=file_content,
                                 content_type=self._file_object_content_type,
                                 overwrite=False)
        self._create_info(file)

    def _create_upload(self, file: File) -> FileUpload:
        object_key = self._file_object_key(file)
        content_type = self._file_object_content_type
        upload_id = self._storage.create_multipart_upload(object_key=object_key,
                                                          content_type=content_type)
        return FileUpload(upload_id=upload_id,
                          hasher=get_resumable_hasher(file.digest.type),
                          etags=[])

    def _mirror_first_part(self, file: File, upload: FileUpload) -> FilePart:
        first_part = FilePart.first(file)
        next_part = self._mirror_part(file, upload, first_part)
        # We shouldn't have started an MP upload for only one part
        assert next_part is not None
        return next_part

    def _mirror_part(self,
                     file: File,
                     upload: FileUpload,
                     part: FilePart
                     ) -> FilePart | None:
        log.info('Uploading part #%d of file %r', part.index, file)
        content = self._download(file, part)
        upload.hasher.update(content)
        etag = self._storage.upload_multipart_part(object_key=self._file_object_key(file),
                                                   upload_id=upload.upload_id,
                                                   part_number=part.index + 1,
                                                   buffer=content)
        upload.etags.append(etag)
        next_part = part.next(file)
        return next_part

    @_mirror.register
    def _(self, a: MirrorPartAction) -> Iterator[MirrorAction]:
        # Some upload field values are mutable so we should make a copy
        upload = a.upload.copy()
        next_part = self._mirror_part(a.file, upload, a.part)
        if next_part is None:
            log.info('Uploaded all %d parts for file %r', len(upload.etags), a.file)
            yield devolve(FinalizeFileAction, a, upload=upload)
        else:
            yield devolve(MirrorPartAction, a, upload=upload, part=next_part)

    @_mirror.register
    def _(self, a: FinalizeFileAction) -> Iterator[MirrorAction]:
        assert len(a.upload.etags) > 0
        self._verify_digest(a.file, a.upload.hasher)
        object_key = self._file_object_key(a.file)
        try:
            self._storage.complete_multipart_upload(object_key=object_key,
                                                    upload_id=a.upload.upload_id,
                                                    etags=a.upload.etags,
                                                    overwrite=False)
        except StorageObjectExists:
            log.info('Discarding redundant upload %r of %r', a.upload.upload_id, a.file)
            self._storage.abort_multipart_upload(object_key=object_key,
                                                 upload_id=a.upload.upload_id)
        self._create_info(a.file)
        log.info('Successfully mirrored file via multi-part upload: %r', a.file)
        return iter(())

    def _info(self, file: File, old_info: JSON | None = None) -> JSON:
        content_types: set[str] = set()
        content_type = 'content-type'
        if old_info is not None:
            old_content_types = old_info[content_type]
            if old_content_types is None:
                # Info objects in AnVIL are invalid against their schema
                # https://github.com/DataBiosphere/azul/issues/7675
                pass
            elif isinstance(old_content_types, str):
                # Content type in mirror info objects inconsistent with index
                # https://github.com/DataBiosphere/azul/issues/7193
                pass
            elif isinstance(old_content_types, list):
                content_types.update(json_element_strings(old_content_types))
            else:
                assert False, type(old_content_types)
        if file.content_type is not None:
            content_types.add(file.content_type)
        return {
            content_type: sorted(content_types),
            '$schema': str(self._schema_url_func(schema_name='info', version=2))
        }

    def _update_info(self, file: File):
        def update(data: bytes) -> bytes:
            return json.dumps(self._info(file, json.loads(data))).encode()

        key = self._info_object_key(file)
        self._storage.update_object(key, update, content_type='application/json')

    def _create_info(self, file: File):
        object_key = self._info_object_key(file)
        info = self._info(file)
        self._storage.put_object(object_key=object_key,
                                 data=json.dumps(info).encode(),
                                 content_type='application/json',
                                 overwrite=False)

    def _repository_url(self, file: File) -> furl:
        assert config.is_tdr_enabled(self.catalog), R(
            'Only TDR catalogs are supported', self.catalog)
        assert file.drs_uri is not None, R(
            'File cannot be downloaded', file)
        object = self.repository_plugin.drs_object(file.drs_uri)
        access = object.get(AccessMethod.gs)
        assert access.method is AccessMethod.https, access
        return furl(access.url)

    def _download(self, file: File, part: FilePart | None = None) -> bytes:
        url = self._repository_url(file)
        start = time.time()
        if part is None:
            headers = {}
            size = file.size
            expected_status = 200
        else:
            headers = {'Range': f'bytes={part.offset}-{part.offset + part.size - 1}'}
            size = part.size
            expected_status = 206
        # Ideally we would stream the response, but boto only supports uploading
        # from streams that are seekable.
        response = self._http_client.request('GET', str(url), headers=headers)
        if response.status == expected_status:
            actual_size = len(response.data)
            log.info('Downloaded %d bytes in %.3fs from file %r',
                     actual_size, time.time() - start, file)
            assert actual_size == size, R(f'Expected {size} bytes, got {actual_size}')
            return response.data
        else:
            raise RuntimeError('Unexpected response from repository', response.status)

    def _verify_digest(self, file: File, hasher: Hasher):
        expected_digest = file.digest
        actual_digest_value = hasher.hexdigest()
        assert expected_digest.value == actual_digest_value, R(
            'File digest value does not match its contents',
            expected_digest, file)
