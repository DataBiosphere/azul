from __future__ import (
    annotations,
)

from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
)
from dataclasses import (
    dataclass,
)
from itertools import (
    count,
)
from logging import (
    getLogger,
)
from threading import (
    BoundedSemaphore,
)
import time
from typing import (
    IO,
    Mapping,
    Optional,
    Sequence,
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

logger = getLogger(__name__)

AWS_S3_DEFAULT_MINIMUM_PART_SIZE = 5242880  # 5 MB; see https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html

MULTIPART_UPLOAD_MAX_WORKERS = 4

# The amount of pending tasks that can be queued for execution. A value of 0
# allows no tasks to be queued, only running tasks allowed in the thread pool.
MULTIPART_UPLOAD_MAX_PENDING_PARTS = 4

Tagging = Mapping[str, str]


class StorageService:

    def __init__(self, bucket_name=config.s3_bucket):
        self.bucket_name = bucket_name

    @property
    def client(self) -> S3Client:
        return aws.client('s3')

    @property
    def resource(self) -> S3ServiceResource:
        return aws.resource('s3')

    def head(self, object_key: str) -> dict:
        return self.client.head_object(Bucket=self.bucket_name, Key=object_key)

    def get(self, object_key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket_name, Key=object_key)['Body'].read()

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

    def put_multipart(self,
                      object_key: str,
                      content_type: Optional[str] = None,
                      tagging: Optional[Tagging] = None
                      ) -> 'MultipartUploadHandler':
        """
        Returns a context manager that facilitates multipart upload to S3. It
        uploads parts concurrently.

        Sample usage:

        .. code-block:: python

           with service.put_multipart('samples.txt'):
               handler.push(b'abc')
               handler.push(b'defg')
               # ...

        Upon exit of the body of the with statement, all parts will have been
        uploaded and the S3 object is guaranteed to exist, or an exception is
        raised. When an exception is raised within the context, the upload will
        be aborted automatically.
        """
        kwargs = self._object_creation_kwargs(content_type=content_type,
                                              tagging=tagging)
        return MultipartUploadHandler(service=self,
                                      object_key=object_key,
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
        while True:
            try:
                self.client.put_object_tagging(Bucket=self.bucket_name,
                                               Key=object_key,
                                               Tagging=tagging)
            except self.client.exceptions.NoSuchKey:
                if time.time() > deadline:
                    logger.error('Unable to tag %s on object.', tagging)
                    raise
                else:
                    logger.warning('Object key %s is not found. Retrying in 5 s.', object_key)
                    time.sleep(5)
            else:
                break

    def get_object_tagging(self, object_key: str) -> Tagging:
        response = self.client.get_object_tagging(Bucket=self.bucket_name, Key=object_key)
        tagging = {tag['Key']: tag['Value'] for tag in response['TagSet']}
        return tagging


class MultipartUploadHandler:

    def __init__(self, *, service: StorageService, **kwargs):
        self.service = service
        self.kwargs = kwargs
        self.__reset()

    def __reset(self):
        self.mp_upload = None
        self.part_number = None
        self.parts = None
        self.futures = None
        self.thread_pool = None
        self.semaphore = None

    @property
    def object_key(self):
        return self.mp_upload.object_key

    @property
    def bucket_name(self):
        return self.mp_upload.bucket_name

    def __enter__(self):
        self.part_number = iter(count(1))
        self.parts = []
        self.futures = []
        self.mp_upload = self.service._create_multipart_upload(**self.kwargs)
        self.thread_pool = ThreadPoolExecutor(max_workers=MULTIPART_UPLOAD_MAX_WORKERS)
        self.semaphore = BoundedSemaphore(MULTIPART_UPLOAD_MAX_PENDING_PARTS + MULTIPART_UPLOAD_MAX_WORKERS)
        return self

    def __exit__(self, etype, value, traceback):
        if etype:
            logger.error('Upload %s: Error detected within the MPU context.',
                         self.mp_upload.id,
                         exc_info=(etype, value, traceback)
                         )
            self.__abort()
        else:
            self.__complete()
        self.__reset()

    def __complete(self):
        for future in as_completed(self.futures):
            exception = future.exception()
            if exception is not None:
                logger.error('Upload %s: Error detected while uploading a part.',
                             self.mp_upload.id,
                             exc_info=exception)
                self.__abort()
                raise MultipartUploadError(self.bucket_name, self.object_key) from exception

        try:
            self.mp_upload.complete(MultipartUpload={"Parts": [part.to_dict() for part in self.parts]})
        except self.service.client.exceptions.ClientError as exception:
            logger.error('Upload %s: Error detected while completing the upload.',
                         self.mp_upload.id,
                         exc_info=exception)
            self.__abort()
            raise MultipartUploadError(self.bucket_name, self.object_key) from exception

        self.thread_pool.shutdown()

    def __abort(self):
        logger.info('Upload %s: Aborting', self.mp_upload.id)
        # This implementation will ignore any pending/active part uploads and force the thread pool to shut down.
        self.mp_upload.abort()
        self.thread_pool.shutdown(wait=False)
        logger.warning('Upload %s: Aborted', self.mp_upload.id)

    def _submit(self, fn, *args, **kwargs):
        # Taken from https://www.bettercodebytes.com/theadpoolexecutor-with-a-bounded-queue-in-python/
        self.semaphore.acquire()
        try:
            future = self.thread_pool.submit(fn, *args, **kwargs)
        except Exception as e:
            self.semaphore.release()
            raise e
        else:
            future.add_done_callback(lambda _future: self.semaphore.release())
            return future

    def push(self, data: bytes):
        part = self._create_new_part(data)
        self.futures.append(self._submit(self._upload_part, part))

    def _create_new_part(self, data: bytes):
        part = Part(part_number=next(self.part_number), etag=None, content=data)
        self.parts.append(part)
        return part

    def _upload_part(self, part):
        upload_part = self.mp_upload.Part(part.part_number)
        result = upload_part.upload(Body=part.content)
        part.etag = result['ETag']
        part.content = None


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
