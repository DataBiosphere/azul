from typing import Optional

from azul import config
from azul.deployment import aws


class StorageService:
    def __init__(self, bucket_name: str=None, s3=None):
        self.__bucket_name = bucket_name or config.s3_bucket
        self.__s3 = s3 or aws.s3

    def set_client(self, client):
        self.__s3 = client

    def get(self, object_key: str):
        try:
            return self.__s3.get_object(Bucket=self.__bucket_name, Key=object_key)['Body'].read().decode()
        except self.__s3.exceptions.NoSuchKey:
            # NOTE: Normally, we should expect specific error classes, like botocore.errorfactory.NoSuchKey (class).
            #       However, that exception is created on demand and it is impossible for us to catch a specific
            #       exception. Hence, we have to catch all sorts of exceptions.
            raise GetObjectError(object_key)

    def put(self, object_key: str, data: bytes, content_type: Optional[str] = None) -> str:
        params = {'Bucket': self.__bucket_name, 'Key': object_key, 'Body': data}

        if content_type:
            params['ContentType'] = content_type

        self.__s3.put_object(**params)

        return object_key

    def delete(self, object_key: str):
        self.__s3.delete_object(Bucket=self.__bucket_name, Key=object_key)

    def get_presigned_url(self, key: str) -> str:
        return self.__s3.generate_presigned_url(ClientMethod='get_object',
                                                Params=dict(Bucket=self.__bucket_name, Key=key))


class GetObjectError(RuntimeError):
    pass
