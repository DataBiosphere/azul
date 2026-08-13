from collections.abc import (
    Collection,
    Iterator,
    Mapping,
    Sequence,
)
from dataclasses import (
    dataclass,
)
from datetime import (
    datetime,
    timedelta,
    timezone,
)
from email.utils import (
    parsedate_to_datetime,
)
import logging
import random
import time
from typing import (
    Callable,
    IO,
    TYPE_CHECKING,
    TypedDict,
)
from urllib.parse import (
    urlencode,
)

import botocore
import botocore.exceptions
from botocore.response import (
    StreamingBody,
)
from more_itertools import (
    chunked,
)
from werkzeug.http import (
    parse_dict_header,
)

from azul.deployment import (
    aws,
)
from azul.lib import (
    R,
)
from azul.lib.collections import (
    OrderedSet,
)

if TYPE_CHECKING:
    from mypy_boto3_s3.client import (
        S3Client,
    )
    from mypy_boto3_s3.type_defs import (
        CompleteMultipartUploadRequestTypeDef,
        CompletedPartTypeDef,
        CreateMultipartUploadRequestTypeDef,
        GetObjectOutputTypeDef,
        HeadObjectOutputTypeDef,
        PutObjectRequestTypeDef,
        PutObjectTaggingRequestTypeDef,
    )

log = logging.getLogger(__name__)

Tagging = Mapping[str, str]


class ObjectMetadata(TypedDict):
    Key: str
    LastModified: datetime
    Size: int


class StorageObjectNotFound(Exception):
    pass


class StorageObjectExists(Exception):
    pass


class StorageService:

    def __init__(self, bucket_name: str | None = None):
        if bucket_name is None:
            bucket_name = aws.storage_bucket
        self.bucket_name = bucket_name

    @property
    def _s3(self) -> S3Client:
        return aws.s3

    def object_exists(self, object_key: str) -> bool:
        try:
            self.head_object(object_key)
        except StorageObjectNotFound:
            return False
        else:
            return True

    def head_object(self, object_key: str) -> HeadObjectOutputTypeDef:
        try:
            return self._s3.head_object(Bucket=self.bucket_name,
                                        Key=object_key)
        except self._s3.exceptions.ClientError as e:
            if int(e.response['Error']['Code']) == 404:
                raise StorageObjectNotFound(object_key)
            else:
                raise e

    def get_object(self, object_key: str) -> bytes:
        return self._get_object(object_key)['Body'].read()

    def _get_object(self, object_key: str) -> GetObjectOutputTypeDef:
        try:
            response = self._s3.get_object(Bucket=self.bucket_name,
                                           Key=object_key)
        except self._s3.exceptions.NoSuchKey:
            raise StorageObjectNotFound(object_key)
        else:
            return response

    def put_object(self,
                   *,
                   object_key: str,
                   data: bytes,
                   content_type: str | None = None,
                   tagging: Tagging | None = None,
                   etag: str | None = None,
                   overwrite: bool = True):
        try:
            request: PutObjectRequestTypeDef
            request = dict(Bucket=self.bucket_name, Key=object_key, Body=data)
            if content_type is not None:
                request['ContentType'] = content_type
            if tagging is not None:
                request['Tagging'] = urlencode(tagging)
            if etag is not None:
                request['IfMatch'] = etag
            if overwrite is False:
                request['IfNoneMatch'] = '*'
            self._s3.put_object(**request)
        except botocore.exceptions.ClientError as e:
            self._handle_overwrite(e, object_key)

    def update_object(self,
                      object_key: str,
                      updater: Callable[[bytes], bytes],
                      *,
                      max_attempts: int = 10,
                      content_type: str | None = None,
                      touch: bool = False
                      ):
        """
        Updates the contents and/or content type of an object, based on its
        existing contents, while ensuring that concurrent updates are not
        overwritten. Expects a callback that returns the desired contents of the
        object given its current contents. If the callback returns its argument
        unchanged and the specified content type is None or matches the current
        content type, no further writes will be attempted unless ``touch`` is
        True, in which case the object will be written regardless, updating its
        ``LastModified`` time. If the object does not exist at any point during
        the update, StorageObjectNotFound is raised.
        """
        for i in range(max_attempts):
            response = self._get_object(object_key)
            etag = response['ETag']
            data = response['Body'].read()
            if content_type is None:
                content_type = response['ContentType']
            new_data = updater(data)
            if not touch and new_data == data and content_type == response['ContentType']:
                log.info('Object contents of %r is already up to date during attempt #%r/%r.',
                         object_key, i + 1, max_attempts)
                break
            else:
                try:
                    self.put_object(object_key=object_key,
                                    data=new_data,
                                    etag=etag,
                                    content_type=content_type)
                except botocore.exceptions.ClientError as e:
                    error = e.response['Error']
                    code, condition = error['Code'], error.get('Condition')
                    if code == 'PreconditionFailed' and condition == 'If-Match':
                        log.info('Conflict during attempt #%r/%r of updating %r from %r to %r',
                                 i + 1, max_attempts, object_key)
                        if i >= max_attempts - 1:
                            raise
                        else:
                            time.sleep(random.uniform(0.5, 5.0))
                    else:
                        raise
                else:
                    log.info('Update of %r succeeded after %r attempts', object_key, i + 1)
                    break

    def delete_objects(self,
                       object_keys: Collection[str],
                       batch_size: int = 1000
                       ) -> None:
        """
        Delete the objects with the given keys, in batches of the given size.
        This method is idempotent: passing the key of an object that was already
        deleted, or that never existed, will not cause an error. This method is
        not atomic: a requirement error will be raised for the first batch with
        an object that failed to be deleted, and any subsequent batches will be
        ignored.

        :param object_keys: a collection of keys of objects to be deleted

        :param batch_size: the number of objects to delete per request
        """
        assert batch_size <= 1000, R('Batch size must <= 1000', batch_size)
        num_keys, num_deleted = len(object_keys), 0
        for batch in chunked(object_keys, batch_size):
            if log.isEnabledFor(logging.DEBUG):
                log.debug('Deleting batch of objects: %r', batch)
            else:
                log.info('Deleting batch of %d object(s)', len(batch))
            response = self._s3.delete_objects(
                Bucket=self.bucket_name,
                Delete=dict(Quiet=True,
                            Objects=[dict(Key=key) for key in batch])
            )
            try:
                errors = response['Errors']
            except KeyError:
                num_deleted += len(batch)
            else:
                assert False, R('Failed to delete some objects', errors)
        assert num_deleted == num_keys, (num_deleted, num_keys)
        log.info('Deleted %d objects overall', num_deleted)

    def list_objects(self, prefix: str) -> Iterator[ObjectMetadata]:
        paginator = self._s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            contents = page.get('Contents', ())
            log.info('Got page of %d object(s) from %r, starting at %r',
                     len(contents), self.bucket_name,
                     contents[0]['Key'] if contents else None)
            for object in contents:
                yield ObjectMetadata(Key=object['Key'],
                                     LastModified=object['LastModified'],
                                     Size=object['Size'])

    def list_keys(self, prefix: str) -> OrderedSet[str]:
        keys: OrderedSet[str] = OrderedSet()
        for object in self.list_objects(prefix):
            key = object['Key']
            assert key not in keys, R('Got duplicate key from S3', key)
            keys.add(key)
        return keys

    def create_multipart_upload(self,
                                *,
                                object_key: str,
                                content_type: str | None = None,
                                tagging: Tagging | None = None
                                ) -> str:
        request: CreateMultipartUploadRequestTypeDef
        request = dict(Bucket=self.bucket_name, Key=object_key)
        if content_type is not None:
            request['ContentType'] = content_type
        if tagging is not None:
            request['Tagging'] = urlencode(tagging)
        response = self._s3.create_multipart_upload(**request)
        return response['UploadId']

    def upload_multipart_part(self,
                              *,
                              object_key: str,
                              upload_id: str,
                              part_number: int,
                              buffer: str | bytes | IO | StreamingBody
                              ) -> str:
        response = self._s3.upload_part(Bucket=self.bucket_name,
                                        Key=object_key,
                                        UploadId=upload_id,
                                        PartNumber=part_number,
                                        Body=buffer)
        return response['ETag']

    def complete_multipart_upload(self,
                                  *,
                                  object_key: str,
                                  upload_id: str,
                                  etags: Sequence[str],
                                  overwrite: bool = True,
                                  ) -> None:
        if len(etags) == 0:
            # S3 requires at least one part, even for empty files
            etags = [self.upload_multipart_part(object_key=object_key,
                                                upload_id=upload_id,
                                                part_number=1,
                                                buffer=b'')]
        parts: list[CompletedPartTypeDef] = [
            {
                'PartNumber': index + 1,
                'ETag': etag
            }
            for index, etag in enumerate(etags)
        ]
        try:
            request: CompleteMultipartUploadRequestTypeDef
            request = dict(Bucket=self.bucket_name,
                           Key=object_key,
                           UploadId=upload_id,
                           MultipartUpload={'Parts': parts})
            if overwrite is False:
                request['IfNoneMatch'] = '*'
            self._s3.complete_multipart_upload(**request)
        except botocore.exceptions.ClientError as e:
            self._handle_overwrite(e, object_key)

    def abort_multipart_upload(self,
                               *,
                               object_key: str,
                               upload_id: str):
        self._s3.abort_multipart_upload(Bucket=self.bucket_name,
                                        Key=object_key,
                                        UploadId=upload_id)

    def upload(self,
               file_path: str,
               object_key: str,
               content_type: str | None = None,
               tagging: Tagging | None = None):
        extra_args: dict[str, str] = {}
        if content_type is not None:
            extra_args['ContentType'] = content_type
        if tagging is not None:
            extra_args['Tagging'] = urlencode(tagging)
        self._s3.upload_file(Filename=file_path,
                             Bucket=self.bucket_name,
                             Key=object_key,
                             ExtraArgs=extra_args)

    def get_presigned_url(self,
                          object_key: str,
                          *,
                          file_name: str | None = None,
                          content_type: str | None = None
                          ) -> str:
        """
        Return a pre-signed URL of the object at the given key.

        :param object_key: The key of the S3 object whose content a request to
                           the signed URL will return

        :param file_name: the file name to be returned as part of a
                          Content-Disposition header in the response to a
                          request to the signed URL. If None, no such header
                          will be present in the response.

        :param content_type: the value for the Content-Type header in the
                             response to a request to the signed URL. If None,
                             the value stored in the object's metadata will be
                             used.
        """
        assert file_name is None or '"' not in file_name, file_name
        params = {
            'Bucket': self.bucket_name,
            'Key': object_key,
        }
        if file_name is not None:
            params['ResponseContentDisposition'] = f'attachment;filename="{file_name}"'
        if content_type is not None:
            params['ResponseContentType'] = content_type
        return self._s3.generate_presigned_url(Params=params,
                                               ClientMethod=self._s3.get_object.__name__)

    def put_object_tagging(self, object_key: str, tagging: Tagging):
        log.info('Tagging object %r with %r', object_key, tagging)
        request: PutObjectTaggingRequestTypeDef
        request = dict(Bucket=self.bucket_name,
                       Key=object_key,
                       Tagging=dict(TagSet=[dict(Key=k, Value=v) for k, v in tagging.items()]))
        deadline = time.time() + 60
        while True:
            try:
                self._s3.put_object_tagging(**request)
            except self._s3.exceptions.NoSuchKey:
                if time.time() > deadline:
                    log.error('Unable to tag %s on object.', tagging)
                    raise
                else:
                    log.warning('Object key %s is not found. Retrying in 5 s.', object_key)
                    time.sleep(5)
            else:
                break

    def get_object_tagging(self, object_key: str) -> Tagging:
        response = self._s3.get_object_tagging(Bucket=self.bucket_name, Key=object_key)
        tagging = {tag['Key']: tag['Value'] for tag in response['TagSet']}
        return tagging

    def time_until_object_expires(self, object_key: str, expiration: int) -> float:
        """
        The time, in seconds, before the object at the given key will expire.

        :param object_key: The key of the object

        :param expiration: the number of days between the last write of an
                           object and its expected expiration by a bucket
                           lifecycle rule. This parameter is solely used to
                           verify the return value.
        """
        response = self.head_object(object_key)
        return self._time_until_object_expires(response, expiration)

    def _time_until_object_expires(self,
                                   head_response: HeadObjectOutputTypeDef,
                                   expiration: int
                                   ) -> float:
        now = datetime.now(timezone.utc)
        # Example header value
        # expiry-date="Fri, 21 Dec 2012 00:00:00 GMT", rule-id="Rule for testfile.txt"
        expiration_header = parse_dict_header(head_response['Expiration'])
        expiry_date = expiration_header['expiry-date']
        assert expiry_date is not None
        expiry = parsedate_to_datetime(expiry_date)
        time_left = (expiry - now).total_seconds()
        # Verify the 'Expiration' value is what is expected given the
        # 'LastModified' value, the number of days before expiration, and that
        # AWS rounds the expiration up to midnight UTC.
        last_modified = head_response['LastModified']
        last_modified_floor = last_modified.replace(hour=0,
                                                    minute=0,
                                                    second=0,
                                                    microsecond=0)
        if last_modified != last_modified_floor:
            expiration += 1
        expected_expiry = last_modified_floor + timedelta(days=expiration)
        if expiry == expected_expiry:
            log.debug('Object expires in %s seconds, on %s',
                      time_left, expiry)
        else:
            log.error('Actual object expiration (%s) does not match expected value (%s)',
                      expiration_header, expected_expiry)
        return time_left

    def _handle_overwrite(self,
                          exception: botocore.exceptions.ClientError,
                          object_key: str
                          ):
        error = exception.response['Error']
        # `Condition` is only present when using conditional writes
        code, condition = error['Code'], error.get('Condition')
        if code == 'PreconditionFailed' and condition == 'If-None-Match':
            raise StorageObjectExists(object_key)
        else:
            raise exception


@dataclass
class Part:
    etag: str | None  # If ETag is defined, the content is already pushed to S3.
    part_number: int
    content: bytes

    @property
    def already_uploaded(self):
        return self.etag is not None

    def to_dict(self):
        return dict(PartNumber=self.part_number, ETag=self.etag)


class MultipartUploadError(RuntimeError):

    def __init__(self, bucket_name, object_key):
        super(MultipartUploadError, self).__init__(f'{bucket_name}/{object_key}')
