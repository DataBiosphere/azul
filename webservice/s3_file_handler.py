#! /usr/bin/env python

import requests
import boto3
import botocore.session
from botocore.exceptions import ClientError


class S3FileHandler:
    def __init__(self, region, access_key_id, secret_key):
        """
        This class simplifies some actions with AWS S3 storage.
        See https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList
        for an exhaustive list of error codes.

        :param region: AWS region
        :type region: str
        :param access_key_id: AWS access key ID
        :type access_key_id: str
        :param secret_key: AWS secret access key
        :type secret_key: str
        """
        service = 's3'
        self.bucket = boto3.client(service,
                                   aws_access_key_id=access_key_id,
                                   aws_secret_access_key=secret_key)
        self.resource = boto3.resource(service)
        session = botocore.session.get_session()
        self.session = session.create_client(service, region)
        self.region = region

    def bucket_exists(self, bucket):
        """
        Check whether bucket with this name exists in AWS account.
        :param bucket: name of bucket in account
        :type bucket: str
        :return: HTTP status code
        :rtype: integer
        """
        try:
            self.resource.meta.client.head_bucket(Bucket=bucket)
            return 200  # bucket exists and access authorized
        except ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 403:
                return 403  # access to bucket not authorized
            elif error_code == 404:
                return 404  # bucket does not exist

    def create_bucket(self, bucket_name):
        """
        :param bucket_name: name of bucket in account
        :type bucket_name: str
        :return: response with details
        :rtype: JSON / Python dict
        """
        if self.bucket_exists(bucket_name) == 200:
            self.bucket_name = bucket_name
            return self.bucket.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': self.region}
            )

    def list_objects_in_bucket(self, bucket):
        """
        :param bucket: name of bucket in account
        :type bucket: str
        :return: list of objects in bucket_name
        :rtype: list
        """
        response = self.session.list_objects_v2(Bucket=bucket)
        if response.has_key('Contents'):
            L = [response['Contents'][x]['Key']
                 for x in range(len(response['Contents']))]
            return [str(r) for r in L]  # convert to utf-8 (is unicode)
        else:
            return []

    def get_bucket_list(self):
        """
        :returns a list of all buckets in AWS account
        :rtype: list"""
        buckets = self.bucket.list_buckets()
        return [bucket['Name'] for bucket in buckets['Buckets']]

    def upload_object_to_bucket(self, bucket, filename, key):
        """Uploads some object (e.g., a file) to an S3 bucket.
        :param bucket: name of bucket in AWS account
        :type bucket: str
        :param filename: absolute name of the object to upload
        :type filename: str
        :param key: object name in bucket on S3
        :type key: str
        :returns: status code, and msg if not 200
        :rtype: JSON
        """

        status_code = self.bucket_exists(bucket)
        if status_code == 200:
            try:
                # If it throws no exception we assume all went well.
                self.resource.meta.client.upload_file(
                    filename, bucket, key)  # executes silently
                return {'status_code': 200}
            except ClientError as e:
                return {'status_code': e.response['Error']['Code']}
        else:
            return {'status_code': status_code}  # e.g., NoSuchBucket = 404

    def create_presigned_url(self, bucket_name, key_name):
        """
        :return: the presigned URL of the key `key_name` in bucket `bucket_name`
        :rtype: str
        """
        status_code = self.bucket_exists(bucket_name)
        if status_code == 200:
            url = self.bucket.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': bucket_name,
                    'Key': key_name
                }
            )

            response = requests.get(url)
            _presigned_url = response.url
            presigned_url = _presigned_url.encode('utf-8')
            return presigned_url
        else:
            return status_code


if __name__ == '__main__':
    pass
