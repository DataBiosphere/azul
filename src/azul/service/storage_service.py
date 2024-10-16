from __future__ import (
    annotations,
)

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
    IO,
    Optional,
    TYPE_CHECKING,
)
from urllib.parse import (
    urlencode,
)

from werkzeug.http import (
    parse_dict_header,
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


class StorageService:

    @property
    def bucket_name(self):
        return aws.storage_bucket

    @property
    def _s3(self) -> S3Client:
        return aws.s3

    def head(self, object_key: str) -> HeadObjectOutputTypeDef:
        try:
            return self._s3.head_object(Bucket=self.bucket_name,
                                        Key=object_key)
        except self._s3.exceptions.ClientError as e:
            if int(e.response['Error']['Code']) == 404:
                raise StorageObjectNotFound
            else:
                raise e

    def get(self, object_key: str) -> bytes:
        try:
            response = self._s3.get_object(Bucket=self.bucket_name,
                                           Key=object_key)
        except self._s3.exceptions.NoSuchKey:
            raise StorageObjectNotFound
        else:
            return response['Body'].read()

    def put(self,
            object_key: str,
            data: bytes,
            content_type: Optional[str] = None,
            tagging: Optional[Tagging] = None,
            **kwargs):
        self._s3.put_object(Bucket=self.bucket_name,
                            Key=object_key,
                            Body=data,
                            **self._object_creation_kwargs(content_type=content_type, tagging=tagging),
                            **kwargs)

    def create_multipart_upload(self,
                                object_key: str,
                                content_type: Optional[str] = None,
                                tagging: Optional[Tagging] = None) -> MultipartUpload:
        kwargs = self._object_creation_kwargs(content_type=content_type,
                                              tagging=tagging)
        return self._create_multipart_upload(object_key=object_key, **kwargs)

    def _create_multipart_upload(self, *, object_key, **kwargs) -> MultipartUpload:
        api_response = self._s3.create_multipart_upload(Bucket=self.bucket_name,
                                                        Key=object_key,
                                                        **kwargs)
        upload_id = api_response['UploadId']
        return self.load_multipart_upload(object_key, upload_id)

    def load_multipart_upload(self, object_key, upload_id) -> MultipartUpload:
        s3 = aws.resource('s3')
        return s3.MultipartUpload(self.bucket_name, object_key, upload_id)

    def upload_multipart_part(self,
                              buffer: IO[bytes],
                              part_number: int,
                              upload: MultipartUpload) -> str:
        return upload.Part(part_number).upload(Body=buffer)['ETag']

    def complete_multipart_upload(self,
                                  upload: MultipartUpload,
                                  etags: Sequence[str]) -> None:
        parts = [
            {
                'PartNumber': index + 1,
                'ETag': etag
            }
            for index, etag in enumerate(etags)
        ]
        upload.complete(MultipartUpload={'Parts': parts})

    def upload(self,
               file_path: str,
               object_key: str,
               content_type: Optional[str] = None,
               tagging: Optional[Tagging] = None):
        self._s3.upload_file(Filename=file_path,
                             Bucket=self.bucket_name,
                             Key=object_key,
                             ExtraArgs=self._object_creation_kwargs(content_type=content_type))
        # upload_file doesn't support tags so we need to make a separate request
        # https://stackoverflow.com/a/56351011/7830612
        if tagging:
            self.put_object_tagging(object_key, tagging)

    def _object_creation_kwargs(self, *,
                                content_type: Optional[str] = None,
                                tagging: Optional[Tagging] = None):
        kwargs = {}
        if content_type is not None:
            kwargs['ContentType'] = content_type
        if tagging is not None:
            kwargs['Tagging'] = urlencode(tagging)
        return kwargs

    def get_presigned_url(self, key: str, file_name: Optional[str] = None) -> str:
        """
        Return a pre-signed URL to the given key.

        :param key: the key of the S3 object whose content a request to the signed URL will return

        :param file_name: the file name to be returned as part of a Content-Disposition header in the response to a
                          request to the signed URL. If None, no such header will be present in the response.
        """
        assert file_name is None or '"' not in file_name
        return self._s3.generate_presigned_url(
            ClientMethod=self._s3.get_object.__name__,
            Params={
                'Bucket': self.bucket_name,
                'Key': key,
                **({} if file_name is None else {'ResponseContentDisposition': f'attachment;filename="{file_name}"'})
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
                                   head_response: HeadObjectOutputTypeDef,
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


@dataclass
class Part:
    etag: Optional[str]  # If ETag is defined, the content is already pushed to S3.
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
