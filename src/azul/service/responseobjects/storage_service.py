from logging import getLogger
from typing import Optional
import boto3
from azul import config


class StorageService:
    def __init__(self, bucket_name: str=None, client=None):
        self.__logger = getLogger(type(self).__name__)
        self.__bucket_name = bucket_name or config.s3_bucket
        self.__client = client  # the default client will be assigned later to allow patching.

    @property
    def client(self):
        if not self.__client:
            self.__client = boto3.client('s3')
            self.__logger.warning(f'{type(self).__name__}: Connected to {self.__client._endpoint}')
        return self.__client

    def set_client(self, client):
        self.__client = client
        self.__logger.warning(f'{type(self).__name__}: Re-connected to {self.__client._endpoint}')

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

    def delete(self, object_key: str):
        self.client.delete_object(Bucket=self.__bucket_name, Key=object_key)

    def get_presigned_url(self, key: str) -> str:
        return self.client.generate_presigned_url(ClientMethod='get_object',
                                                  Params=dict(Bucket=self.__bucket_name, Key=key))

    def create_bucket(self, bucket_name: str = None):
        self.__logger.warning(f'{type(self).__name__}: Creating a bucket called "{bucket_name or self.__bucket_name}"')
        self.client.create_bucket(Bucket=(bucket_name or self.__bucket_name))
        self.__logger.warning(f'{type(self).__name__}: Created a bucket called "{bucket_name or self.__bucket_name}"')


class GetObjectError(RuntimeError):
    pass
