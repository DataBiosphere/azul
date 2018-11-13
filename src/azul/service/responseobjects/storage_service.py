from logging import getLogger
from typing import Optional
import boto3
from azul import config

logger = getLogger(__name__)


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

    def get_presigned_url(self, key: str) -> str:
        return self.client.generate_presigned_url(ClientMethod='get_object',
                                                  Params=dict(Bucket=self.__bucket_name, Key=key))

    def create_bucket(self, bucket_name: str = None):
        self.client.create_bucket(Bucket=(bucket_name or self.__bucket_name))
        logger.warning(f'{type(self).__name__}: Created a bucket called "{bucket_name or self.__bucket_name}"')


class GetObjectError(RuntimeError):
    pass
