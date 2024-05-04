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

from azul import (
    config,
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
        S3ServiceResource,
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

    def __init__(self, bucket_name: str | None = None):
        if bucket_name is None:
            bucket_name = aws.storage_bucket
        self.bucket_name = bucket_name

    @property
    def client(self) -> S3Client:
        return aws.s3

    @property
    def resource(self) -> S3ServiceResource:
        return aws.resource('s3')

    def head(self, object_key: str) -> dict:
        try:
            return self.client.head_object(Bucket=self.bucket_name,
                                           Key=object_key)
        except self.client.exceptions.ClientError as e:
            if int(e.response['Error']['Code']) == 404:
                raise StorageObjectNotFound
            else:
                raise e

    def get(self, object_key: str) -> bytes:
        try:
            response = self.client.get_object(Bucket=self.bucket_name,
                                              Key=object_key)
        except self.client.exceptions.NoSuchKey:
            raise StorageObjectNotFound
        else:
            return response['Body'].read()

    def put(self,
            object_key: str,
            data: bytes,
            content_type: Optional[str] = None,
            tagging: Optional[Tagging] = None,
            **kwargs):
        self.client.put_object(Bucket=self.bucket_name,
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
        api_response = self.client.create_multipart_upload(Bucket=self.bucket_name,
                                                           Key=object_key,
                                                           **kwargs)
        upload_id = api_response['UploadId']
        return self.load_multipart_upload(object_key, upload_id)

    def load_multipart_upload(self, object_key, upload_id) -> MultipartUpload:
        return self.resource.MultipartUpload(self.bucket_name, object_key, upload_id)

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
        self.client.upload_file(Filename=file_path,
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
        return self.client.generate_presigned_url(
            ClientMethod=self.client.get_object.__name__,
            Params={
                'Bucket': self.bucket_name,
                'Key': key,
                **({} if file_name is None else {'ResponseContentDisposition': f'attachment;filename="{file_name}"'})
            })

    def create_bucket(self, bucket_name: Optional[str] = None):
        self.client.create_bucket(Bucket=(bucket_name or self.bucket_name),
                                  CreateBucketConfiguration={
                                      'LocationConstraint': config.region
                                  })

    def put_object_tagging(self, object_key: str, tagging: Tagging = None):
        deadline = time.time() + 60
        tagging = {'TagSet': [{'Key': k, 'Value': v} for k, v in tagging.items()]}
        log.info('Tagging object %r with %r', object_key, tagging)
        while True:
            try:
                self.client.put_object_tagging(Bucket=self.bucket_name,
                                               Key=object_key,
                                               Tagging=tagging)
            except self.client.exceptions.NoSuchKey:
                if time.time() > deadline:
                    log.error('Unable to tag %s on object.', tagging)
                    raise
                else:
                    log.warning('Object key %s is not found. Retrying in 5 s.', object_key)
                    time.sleep(5)
            else:
                break

    def get_object_tagging(self, object_key: str) -> Tagging:
        response = self.client.get_object_tagging(Bucket=self.bucket_name, Key=object_key)
        tagging = {tag['Key']: tag['Value'] for tag in response['TagSet']}
        return tagging


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
