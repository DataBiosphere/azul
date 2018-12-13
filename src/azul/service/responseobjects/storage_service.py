from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from functools import lru_cache
from logging import getLogger
from typing import Optional
import boto3
from azul import config

logger = getLogger(__name__)
AWS_S3_DEFAULT_MINIMUM_PART_SIZE = 5242880  # 5 MB; see https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
MULTIPART_UPLOAD_MAX_WORKERS = 4


class StorageService:

    def __init__(self):
        self.bucket_name = config.s3_bucket

    @property
    @lru_cache(maxsize=1)
    def client(self):
        return boto3.client('s3')

    def get(self, object_key: str) -> bytes:
        try:
            return self.client.get_object(Bucket=self.bucket_name, Key=object_key)['Body'].read()
        except self.client.exceptions.NoSuchKey:
            raise GetObjectError(object_key)

    def put(self, object_key: str, data: bytes, content_type: Optional[str] = None) -> str:
        params = {'Bucket': self.bucket_name, 'Key': object_key, 'Body': data}

        if content_type:
            params['ContentType'] = content_type

        self.client.put_object(**params)

        return object_key

    def get_presigned_url(self, key: str) -> str:
        return self.client.generate_presigned_url(ClientMethod='get_object',
                                                  Params=dict(Bucket=self.bucket_name, Key=key))

    def create_bucket(self, bucket_name: str = None):
        self.client.create_bucket(Bucket=(bucket_name or self.bucket_name))


class MultipartUploadHandler:
    """
    A context manager that facilitates multipart upload to S3. It uploads parts
    concurrently.

    Sample usage:

    .. code-block:: python

       with MultipartUploadHandler('samples.txt', 'text/plain'):
           handler.push(b'abc')
           handler.push(b'defg')
           # ...

    Upon exit of the body of the with statement, all parts will have been
    uploaded and the S3 object is guaranteed to exist, or an exception is raised.
    When an exception is raised within the context, the upload will be aborted
    automatically.
    """

    def __init__(self, object_key, content_type: str):
        self.bucket_name = config.s3_bucket
        self.object_key = object_key
        self.upload_id = None
        self.mp_upload = None
        self.next_part_number = 1
        self.content_type = content_type
        self.parts = []
        self.futures = []
        self.thread_pool = None

    def __enter__(self):
        api_response = boto3.client('s3').create_multipart_upload(Bucket=self.bucket_name,
                                                                  Key=self.object_key,
                                                                  ContentType=self.content_type)
        self.upload_id = api_response['UploadId']
        self.mp_upload = boto3.resource('s3').MultipartUpload(self.bucket_name, self.object_key, self.upload_id)
        self.thread_pool = ThreadPoolExecutor(max_workers=MULTIPART_UPLOAD_MAX_WORKERS)
        return self

    def __exit__(self, etype, value, traceback):
        if etype:
            logger.error('Upload %s: Error detected within the MPU context.',
                         self.upload_id,
                         exc_info = (etype, value, traceback)
                         )
            self.__abort()
        else:
            self.__complete()

    def __complete(self):
        for future in as_completed(self.futures):
            exception = future.exception()
            if exception is not None:
                logger.error('Upload %s: Error detected while uploading a part.',
                             self.upload_id,
                             exc_info=(type(exception), exception, None))
                self.__abort()
                raise UnexpectedMultipartUploadAbort(f'{self.bucket_name}/{self.object_key}') from exception

        try:
            self.mp_upload.complete(MultipartUpload={"Parts": [part.to_dict() for part in self.parts]})
        except self.mp_upload.meta.client.exceptions.ClientError as exception:
            logger.error('Upload %s: Error detected while completing the upload.',
                         self.upload_id,
                         exc_info=(type(exception), exception, None))
            self.__abort()
            if 'EntityTooSmall' in exception.args[0]:
                raise UploadPartSizeOutOfBoundError(f'{self.bucket_name}/{self.object_key}')
            else:
                raise UnexpectedMultipartUploadAbort(f'{self.bucket_name}/{self.object_key}')

        self.mp_upload = None
        self.thread_pool.shutdown()

    def __abort(self):
        logger.info('Upload %s: Aborting', self.upload_id)
        # This implementation will ignore any pending/active part uploads and force the thread pool to shut down.
        self.mp_upload.abort()
        self.mp_upload = None
        self.thread_pool.shutdown(wait=False)
        logger.warning('Upload %s: Aborted', self.upload_id)

    def push(self, data: bytes):
        part = self._create_new_part(data)
        self.futures.append(self.thread_pool.submit(self._upload_part, part))

    def _create_new_part(self, data: bytes):
        part = Part(part_number=self.next_part_number, etag=None, content=data)
        self.parts.append(part)
        self.next_part_number += 1

        return part

    def _upload_part(self, part):
        upload_part = self.mp_upload.Part(part.part_number)
        result = upload_part.upload(Body=part.content)
        part.etag = result['ETag']


@dataclass
class Part:
    etag: str  # If ETag is defined, the content is already pushed to S3.
    part_number: int
    content: bytes

    @property
    def already_uploaded(self):
        return self.etag is not None

    def to_dict(self):
        return dict(PartNumber=self.part_number, ETag=self.etag)


class GetObjectError(RuntimeError):
    pass


class EmptyMultipartUploadError(RuntimeError):
    pass


class UnexpectedMultipartUploadAbort(RuntimeError):
    pass


class UploadPartSizeOutOfBoundError(RuntimeError):
    pass
