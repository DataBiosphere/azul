from collections.abc import (
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
from logging import (
    getLogger,
)
import time
from typing import (
    Collection,
    IO,
    TYPE_CHECKING,
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

from azul import (
    R,
)
from azul.collections import (
    OrderedSet,
)
from azul.deployment import (
    aws,
)

if TYPE_CHECKING:
    from mypy_boto3_s3.client import (
        S3Client,
    )
    from mypy_boto3_s3.service_resource import (
        MultipartUpload,
    )
    from mypy_boto3_s3.type_defs import (
        HeadObjectOutputTypeDef,
    )

log = getLogger(__name__)

# 5 MB; see https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
AWS_S3_DEFAULT_MINIMUM_PART_SIZE = 5242880

MULTIPART_UPLOAD_MAX_WORKERS = 4

# The amount of pending tasks that can be queued for execution. A value of 0
# allows no tasks to be queued, only running tasks allowed in the thread pool.
MULTIPART_UPLOAD_MAX_PENDING_PARTS = 4

Tagging = Mapping[str, str]


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
    def _s3(self) -> 'S3Client':
        return aws.s3

    def head(self, object_key: str) -> 'HeadObjectOutputTypeDef':
        try:
            return self._s3.head_object(Bucket=self.bucket_name,
                                        Key=object_key)
        except self._s3.exceptions.ClientError as e:
            if int(e.response['Error']['Code']) == 404:
                raise StorageObjectNotFound(object_key)
            else:
                raise e

    def get(self, object_key: str) -> bytes:
        try:
            response = self._s3.get_object(Bucket=self.bucket_name,
                                           Key=object_key)
        except self._s3.exceptions.NoSuchKey:
            raise StorageObjectNotFound(object_key)
        else:
            return response['Body'].read()

    def put(self,
            object_key: str,
            data: bytes,
            content_type: str | None = None,
            tagging: Tagging | None = None,
            *,
            overwrite: bool = True,
            **kwargs):
        try:
            self._s3.put_object(Bucket=self.bucket_name,
                                Key=object_key,
                                Body=data,
                                **self._object_creation_kwargs(content_type=content_type,
                                                               tagging=tagging,
                                                               overwrite=overwrite),
                                **kwargs)
        except botocore.exceptions.ClientError as e:
            self._handle_overwrite(e, object_key)

    def delete(self, keys: Collection[str], batch_size: int = 1000) -> None:
        assert batch_size <= 1000, R('Batch size must <= 1000', batch_size)
        num_keys = len(keys)
        for batch in chunked(keys, batch_size):
            log.debug('Deleting batch of objects: %r', batch)
            self._s3.delete_objects(Bucket=self.bucket_name,
                                    Delete={
                                        'Objects': [
                                            {'Key': key}
                                            for key in batch
                                        ]
                                    })
        log.info('Deleted %d objects overall', num_keys)

    def list(self, prefix: str) -> OrderedSet[str]:
        keys, num_keys = OrderedSet(), 0
        paginator = self._s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            contents = page.get('Contents', ())
            num_keys += len(contents)
            keys.update(object['Key'] for object in contents)
            assert len(keys) == num_keys, R('Got duplicate keys from S3')
        return keys

    def create_multipart_upload(self,
                                object_key: str,
                                content_type: str | None = None,
                                tagging: Tagging | None = None
                                ) -> 'MultipartUpload':
        kwargs = self._object_creation_kwargs(content_type=content_type,
                                              tagging=tagging)
        return self._create_multipart_upload(object_key=object_key, **kwargs)

    def _create_multipart_upload(self, *, object_key, **kwargs) -> 'MultipartUpload':
        api_response = self._s3.create_multipart_upload(Bucket=self.bucket_name,
                                                        Key=object_key,
                                                        **kwargs)
        upload_id = api_response['UploadId']
        return self.load_multipart_upload(object_key, upload_id)

    def load_multipart_upload(self, object_key, upload_id) -> 'MultipartUpload':
        s3 = aws.s3_resource
        return s3.MultipartUpload(self.bucket_name, object_key, upload_id)

    def upload_multipart_part(self,
                              buffer: str | bytes | IO | StreamingBody,
                              part_number: int,
                              upload: 'MultipartUpload'
                              ) -> str:
        return upload.Part(part_number).upload(Body=buffer)['ETag']

    def complete_multipart_upload(self,
                                  upload: 'MultipartUpload',
                                  etags: Sequence[str],
                                  *,
                                  overwrite: bool = True,
                                  ) -> None:
        parts = [
            {
                'PartNumber': index + 1,
                'ETag': etag
            }
            for index, etag in enumerate(etags)
        ]
        try:
            upload.complete(MultipartUpload={'Parts': parts},
                            **self._object_creation_kwargs(overwrite=overwrite))
        except botocore.exceptions.ClientError as e:
            self._handle_overwrite(e, upload.object_key)

    def upload(self,
               file_path: str,
               object_key: str,
               content_type: str | None = None,
               tagging: Tagging | None = None):
        self._s3.upload_file(Filename=file_path,
                             Bucket=self.bucket_name,
                             Key=object_key,
                             ExtraArgs=self._object_creation_kwargs(content_type=content_type))
        # upload_file doesn't support tags so we need to make a separate request
        # https://stackoverflow.com/a/56351011/7830612
        if tagging:
            self.put_object_tagging(object_key, tagging)

    def _object_creation_kwargs(self,
                                *,
                                content_type: str | None = None,
                                tagging: Tagging | None = None,
                                overwrite: bool = True
                                ) -> Mapping[str, str]:
        kwargs = {}
        if content_type is not None:
            kwargs['ContentType'] = content_type
        if tagging is not None:
            kwargs['Tagging'] = urlencode(tagging)
        if overwrite is False:
            kwargs['IfNoneMatch'] = '*'
        return kwargs

    def get_presigned_url(self,
                          key: str,
                          *,
                          file_name: str | None = None,
                          content_type: str | None = None
                          ) -> str:
        """
        Return a pre-signed URL to the given key.

        :param key: The key of the S3 object whose content a request to the
                    signed URL will return

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
        return self._s3.generate_presigned_url(
            ClientMethod=self._s3.get_object.__name__,
            Params={
                'Bucket': self.bucket_name,
                'Key': key,
                **(
                    {}
                    if file_name is None else
                    {'ResponseContentDisposition': f'attachment;filename="{file_name}"'}
                ),
                **(
                    {}
                    if content_type is None else
                    {'ResponseContentType': content_type}
                )
            })

    def put_object_tagging(self, object_key: str, tagging: Tagging = None):
        deadline = time.time() + 60
        tagging = {'TagSet': [{'Key': k, 'Value': v} for k, v in tagging.items()]}
        log.info('Tagging object %r with %r', object_key, tagging)
        while True:
            try:
                self._s3.put_object_tagging(Bucket=self.bucket_name,
                                            Key=object_key,
                                            Tagging=tagging)
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
        response = self.head(object_key)
        return self._time_until_object_expires(response, expiration)

    def _time_until_object_expires(self,
                                   head_response: 'HeadObjectOutputTypeDef',
                                   expiration: int
                                   ) -> float:
        now = datetime.now(timezone.utc)
        # Example header value
        # expiry-date="Fri, 21 Dec 2012 00:00:00 GMT", rule-id="Rule for testfile.txt"
        expiration_header = parse_dict_header(head_response['Expiration'])
        expiry = parsedate_to_datetime(expiration_header['expiry-date'])
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
