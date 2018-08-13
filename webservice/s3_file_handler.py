#! /usr/bin/env python

import boto3
import botocore.session


class S3FileHandler:

    def __init__(self, location):
        """Expects AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to be in 
        in a config dictionary or as environment variables."""
        service = 's3'
        self.bucket = boto3.client(service)
        self.resource = boto3.resource(service)
        session = botocore.session.get_session()
        self.session = session.create_client(service, location)
        self.location = location

    def bucket_exists(self, bucket_name):
        """
        Check whether bucket with this name exists in AWS account.
        :param bucket_name: (str)
        :return:(bool) True for exists
        """
        return self.resource.Bucket(bucket_name) in self.resource.buckets.all()

    def create_bucket(self, bucket_name):
        """
        :param bucket_name: name of the bucket 
        :return: response with details
        :rtype: JSON / Python dict
        """
        if not self.bucket_exists(bucket_name):
            self.bucket_name = bucket_name
            return self.bucket.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': self.location}
            )

    def list_objects_in_bucket(self, bucket_name):
        """
        :param bucket_name: (str) name of bucket in account
        :return: list of objects in bucket_name  
        """
        response = self.session.list_objects_v2(Bucket=bucket_name)
        if response.has_key('Contents'):
            L = [response['Contents'][x]['Key']
                    for x in range(len(response['Contents']))]
            return [str(r) for r in L]  # convert to utf-8 (is unicode)
        else:
            return []

    def get_bucket_list(self):
        """
        :returns a list of all buckets in AWS account
        :rtype list"""
        buckets = self.bucket.list_buckets()
        return [bucket['Name'] for bucket in buckets['Buckets']]

    def delete_bucket(self, bucket_name):
        """
        :param bucket_name: (str)
        :return: (bool) True if bucket could be delete, False if 
        """
        if self.bucket_exists(bucket_name):
            bucket = self.resource.Bucket(bucket_name)
            # This could be more complicated if the bucket is versioned.
            bucket.objects.all().delete()
            bucket.delete()
            if not self.bucket_exists(bucket_name):
                return True
        else:
            return False  # bucket_name doesn't exist

    def upload_object_to_bucket(self, bucket_name, fname, name_in_bucket):
        """Uploads some object (e.g., a file) to an S3 bucket.
        :param bucket_name: (str) name of bucket in AWS account
        :param fname: (str) absolute name of the object to upload
        :param name_in_bucket: (str) object name in bucket
        :returns: (bool) True if it went well, False if bucket does not exist"""

        if self.bucket_exists(bucket_name):
            self.resource.meta.client.upload_file(
                fname, bucket_name, name_in_bucket)  # executes silently
            return True
        else:
            return False  # bucket_name doesn't exist


if __name__ == '__main__':
    pass
