from contextlib import contextmanager
from dataclasses import dataclass
from logging import getLogger
from threading import Lock, Thread
from typing import Optional, List
import boto3
from azul import config

logger = getLogger(__name__)
MINIMUM_PART_SIZE = 5242880  # 5 MB; see https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html


class StorageService:

    def __init__(self):
        self.__bucket_name = config.s3_bucket
        self.__client = None  # the default client will be assigned later to allow patching.

    @property
    def client(self):
        if not self.__client:
            self.__client = boto3.client('s3')
        return self.__client

    def set_client(self, client):
        self.__client = client

    def get(self, object_key: str):
        try:
            return self.client.get_object(Bucket=self.__bucket_name, Key=object_key)['Body'].read().decode()
        except self.client.exceptions.NoSuchKey:
            raise GetObjectError(object_key)

    def put(self, object_key: str, data: bytes, content_type: Optional[str] = None) -> str:
        params = {'Bucket': self.__bucket_name, 'Key': object_key, 'Body': data}

        if content_type:
            params['ContentType'] = content_type

        self.client.put_object(**params)

        return object_key

    @contextmanager
    def multipart_upload(self, object_key: str, content_type: str):
        # logger.info(f'multipart_upload: begin')
        api_response = self.client.create_multipart_upload(Bucket=self.__bucket_name,
                                                           Key=object_key,
                                                           ContentType=content_type)
        upload_id = api_response['UploadId']
        handler = MultipartUploadHandler(self.__bucket_name, object_key, upload_id)

        # logger.info(f'multipart_upload: ready to upload')
        yield handler
        # logger.info(f'multipart_upload: cleaning up')

        if handler.is_active:
            handler.complete()
        # logger.info(f'multipart_upload: end')

    def get_presigned_url(self, key: str) -> str:
        return self.client.generate_presigned_url(ClientMethod='get_object',
                                                  Params=dict(Bucket=self.__bucket_name, Key=key))

    def create_bucket(self, bucket_name: str = None):
        self.client.create_bucket(Bucket=(bucket_name or self.__bucket_name))
        # logger.warning(f'{type(self).__name__}: Created a bucket called "{bucket_name or self.__bucket_name}"')


class MultipartUploadHandler:

    def __init__(self, bucket_name, object_key, upload_id):
        self.__resource = boto3.resource('s3')
        self.__bucket_name = bucket_name
        self.__object_key = object_key
        self.__upload_id = upload_id
        self.__handler = self.__resource.MultipartUpload(self.__bucket_name, self.__object_key, self.__upload_id)
        self.__next_part_number = 1
        self.__closed = False
        self.__parts = []
        self.__upload_lock = Lock()

    @property
    def is_active(self):
        return not self.__closed

    def complete(self):
        if not self.is_active:
            return

        if not self.__parts:
            # logger.warning(f'multipart_upload: the upload is empty.')
            self.abort()
            # logger.warning(f'multipart_upload: aborted')
            raise EmptyMultipartUploadError(f'{self.__bucket_name}/{self.__object_key}')

        # logger.info(f'multipart_upload: Waiting for active uploads to complete.')
        while self._has_active_uploads():
            pass  # Ensure that hanging threads
        # logger.info(f'multipart_upload: Check the last part.')

        last_part = self.__parts[-1]

        if not last_part.already_uploaded:
            # Per documentation, the last part can be at any size. Hence, the uploadable condition is ignored.
            # logger.warning(f'multipart_upload: uploading the last part')
            self._upload_part(last_part)
            # logger.info(f'multipart_upload: uploaded the last part')

        self.__handler.complete(MultipartUpload={"Parts": [part.to_dict() for part in self.__parts]})
        self.__handler = None
        self.__closed = True

    def abort(self):
        if not self.is_active:
            return

        self.__handler.abort()
        self.__handler = None
        self.__closed = True

    def push(self, data: bytes):
        # If the last part is under the minimum limit, the data will be appended.
        if not self.__parts:
            part = self._create_new_part(data)
        else:
            last_part = self.__parts[-1]
            if last_part.already_uploaded:
                part = self._create_new_part(data)
            else:
                part = last_part
                part.content.append(data)

        if not part.is_uploadable:
            return

        # logger.info(f'multipart_upload/push: Part {part.part_number}: uploading in a background thread')
        Thread(target=self._upload_part, args=(part,)).start()
        # logger.info(f'multipart_upload/push: Part {part.part_number}: the background thread is active')

    def _create_new_part(self, data: bytes):
        part = Part(part_number=self.__next_part_number, etag=None, content=[data], uploaded=False)
        self.__parts.append(part)
        self.__next_part_number += 1
        return part

    def _upload_part(self, part):
        # logger.info(f'multipart_upload/_upload_part: Part {part.part_number}: begin')
        self.__upload_lock.acquire()
        if part.uploaded:
            self.__upload_lock.release()
            # logger.info(f'multipart_upload/_upload_part: Part {part.part_number}: end (already uploaded, ignored)')
            return
        part.uploaded = True
        self.__upload_lock.release()
        # logger.info(f'multipart_upload/_upload_part: Part {part.part_number}: uploading...')
        upload_part = self.__handler.Part(part.part_number)
        result = upload_part.upload(Body=b''.join(part.content))
        part.etag = result['ETag']
        logger.info(f'multipart_upload/_upload_part: Part {part.part_number}: end (uploaded)')

    def _has_active_uploads(self) -> bool:
        """
        Check if there are active uploads, except the last one.
        """
        for part in self.__parts[:-1]:
            if part.etag:
                return True
        return False


@dataclass
class Part:
    etag: str  # If ETag is defined, the content is already pushed to S3.
    part_number: int
    content: List[bytes]
    uploaded: bool  # This flag is used for multi-threading.

    @property
    def already_uploaded(self):
        return self.uploaded or self.etag is not None

    @property
    def is_uploadable(self):
        return len(self.content) >= MINIMUM_PART_SIZE

    def to_dict(self):
        return dict(PartNumber=self.part_number, ETag=self.etag)


class GetObjectError(RuntimeError):
    pass


class EmptyMultipartUploadError(RuntimeError):
    pass
