#! /usr/bin/env python

import boto3
import botocore.session


class S3FileHandler:

    def __init__(self, region, access_key_id, secret_key):
        """
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
        :return: true if it exists
        :rtype: boolean
        """
        return self.resource.Bucket(bucket) in self.resource.buckets.all()

    def create_bucket(self, bucket_name):
        """
        :param bucket_name: name of bucket in account
        :type bucket_name: str
        :return: response with details
        :rtype: JSON / Python dict
        """
        if not self.bucket_exists(bucket_name):
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
        :returns: True if it went well, False if bucket does not exist
        :rtype:bool
        """

        if self.bucket_exists(bucket):
            self.resource.meta.client.upload_file(
                filename, bucket, key)  # executes silently
            return True
        else:
            return False  # bucket_name doesn't exist


if __name__ == '__main__':
    pass
