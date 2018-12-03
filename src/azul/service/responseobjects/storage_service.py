from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from functools import lru_cache
from logging import getLogger
from typing import Optional, List
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

    def get(self, object_key: str) -> str:
        try:
            return self.client.get_object(Bucket=self.bucket_name, Key=object_key)['Body'].read().decode()
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
    S3 Multipart Upload Handler

    This class is to facilitate multipart upload to S3 storage. The class itself
    is also a context manager.

    Here is the sample usage.

    .. code-block:: python

       handler = MultipartUploadHandler('samples.txt', 'text/plain')
       handler.start()
       handler.push(b'abc')
       handler.push(b'defg')
       # ...
       handler.shutdown()

    or

    .. code-block:: python

       with MultipartUploadHandler('samples.txt', 'text/plain'):
           handler.push(b'abc')
           handler.push(b'defg')
           # ...

    where the context pattern will automatically shutdown upon exiting the context.
    """

    def __init__(self, object_key, content_type: str):
        self.bucket_name = config.s3_bucket
        self.object_key = object_key
        self.upload_id = None
        self.mp_upload = None
        self.next_part_number = 1
        self.closed = False
        self.content_type = content_type
        self.parts = []
        self.futures = []
        self.thread_pool = None

    def __enter__(self):
        return self.start()

    def __exit__(self, type, value, traceback):
        if type:
            self.abort()
            raise UnexpectedMultipartUploadAbort(f'{self.bucket_name}/{self.object_key}')
        self.shutdown()

    @property
    def is_active(self):
        return not self.closed

    def start(self):
        api_response = boto3.client('s3').create_multipart_upload(Bucket=self.bucket_name,
                                                                  Key=self.object_key,
                                                                  ContentType=self.content_type)
        self.upload_id = api_response['UploadId']
        self.mp_upload = boto3.resource('s3').MultipartUpload(self.bucket_name, self.object_key, self.upload_id)
        self.thread_pool = ThreadPoolExecutor(max_workers=MULTIPART_UPLOAD_MAX_WORKERS)
        return self

    def shutdown(self):
        self.complete()
        self.thread_pool.shutdown()

    def complete(self):
        """
        Completes a multipart upload by assembling previously uploaded parts.

        When this method is invoked, if the last part is not uploaded, the method
        will upload that part before assembling the list of uploaded parts.

        In addition, this method raises :class:`EmptyMultipartUploadError` if no
        parts are uploaded.
        """
        if not self.is_active:
            return

        if not self.parts:
            self.abort()
            raise EmptyMultipartUploadError(f'{self.bucket_name}/{self.object_key}')

        last_part = self.parts[-1]

        if not last_part.already_uploaded:
            # Per documentation, the last part can be at any size. Hence, the uploadable condition is ignored.
            self.futures.append(self.thread_pool.submit(self._upload_part, last_part))

        for _ in as_completed(self.futures):
            pass  # Blocked until all uploads are done.

        self.mp_upload.complete(MultipartUpload={"Parts": [part.to_dict() for part in self.parts]})
        self.mp_upload = None
        self.closed = True

    def abort(self):
        if not self.is_active:
            return

        self.mp_upload.abort()
        self.mp_upload = None
        self.closed = True
        self.thread_pool.shutdown()

        logger.warning('Upload %s: Aborted', self.upload_id)

    def push(self, data: bytes):
        part = self._get_next_part(data)

        if not part.is_uploadable:
            return

        part.uploaded = True

        self.futures.append(self.thread_pool.submit(self._upload_part, part))

    def _create_new_part(self, data: bytes):
        part = Part(part_number=self.next_part_number, etag=None, content=[data], uploaded=False)
        self.parts.append(part)
        self.next_part_number += 1

        return part

    def _get_next_part(self, data):
        if not self.parts:
            part = self._create_new_part(data)
        else:
            # If the last part is under the minimum limit, the data will be appended.
            last_part = self.parts[-1]
            if last_part.already_uploaded:
                part = self._create_new_part(data)
            else:
                part = last_part
                part.content.append(data)

        return part

    def _upload_part(self, part):
        upload_part = self.mp_upload.Part(part.part_number)
        result = upload_part.upload(Body=b''.join(part.content))
        part.etag = result['ETag']


@dataclass
class Part:
    etag: str  # If ETag is defined, the content is already pushed to S3.
    part_number: int
    content: List[bytes]
    uploaded: bool  # If true, the content is either being uploaded or already pushed to S3 (with etag defined).

    @property
    def already_uploaded(self):
        return self.uploaded or self.etag is not None

    @property
    def is_uploadable(self):
        return len(b''.join(self.content)) >= AWS_S3_DEFAULT_MINIMUM_PART_SIZE

    def to_dict(self):
        return dict(PartNumber=self.part_number, ETag=self.etag)


class GetObjectError(RuntimeError):
    pass


class EmptyMultipartUploadError(RuntimeError):
    pass


class UnexpectedMultipartUploadAbort(RuntimeError):
    pass
