#! /usr/bin/env python

import unittest
import requests
import botocore
from mock import patch, Mock
from s3_file_handler import S3FileHandler as s3handler
from botocore.exceptions import ClientError


class TestS3FileHandler(unittest.TestCase):

    def setUp(self):
        self.bucket = s3handler(region='us-west-2',
                                access_key_id='abc-123',
                                secret_key='abc-123')
        # Real-world response from method create_bucket method for mocks.
        self.response = {
            u'Location': 'http://my_bucket.s3.amazonaws.com/',
            'ResponseMetadata': {
                'HTTPHeaders': {
                    'content-length': '0',
                    'date': 'Thu, 09 Aug 2018 20:51:00 GMT',
                    'location': 'http://my_bucket.s3.amazonaws.com/',
                    'server': 'AmazonS3',
                    'x-amz-id-2': 'wnWUNgDrhe87VMPO+NeBuzZF7VI0639EdiDsKMWIUOr9NobeCaU2hDe4UT+2wIZrBto8HbdeIs0=',
                    'x-amz-request-id': '10C648333E274D33'},
                'HTTPStatusCode': 200,
                'HostId': 'wnWUNgDrhe87VMPO+NeBuzZF7VI0639EdiDsKMWIUOr9NobeCaU2hDe4UT+2wIZrBto8HbdeIs0=',
                'RequestId': '10C648333E274D33',
                'RetryAttempts': 0
            }
        }

        # Mocked return value from method get_bucket_list.
        self.bucket_list = ['my_bucket', 'my_bucket2', 'my_bucket3']
        # Mocked return value from method list_objects_in_bucket.
        self.list_objects_in_bucket = ['file1.txt', 'file2.txt', 'file3.txt']

    @patch('s3_file_handler.S3FileHandler.bucket_exists')
    def test_bucket_exists(self, mock_bucket_exists):
        mock_bucket_exists.return_value = 200
        status_code = self.bucket.bucket_exists('my_bucket')
        self.assertEqual(status_code, 200)

        mock_bucket_exists.return_value = 404
        status_code = self.bucket.bucket_exists('my_bucket')
        self.assertEqual(status_code, 404)

    @patch('s3_file_handler.boto3')
    def test_bucket_exists_client_error403(self, boto3):

        boto3.resource('s3')
        boto3.resource.assert_called_with('s3')

        boto3.return_value.resource.return_value.Object.return_value.\
            upload_file.side_effect = botocore.exceptions.ClientError(
                {'Error': {'Code': '403', 'Message': 'something'}},
                'PutObject')
        status_code = self.bucket.bucket_exists('my_bucket')
        self.assertEqual(status_code, 403)

    @patch('s3_file_handler.S3FileHandler.get_bucket_list')
    def test_get_bucket_list(self, mock_bucket_list):
        mock_bucket_list.return_value = self.bucket_list
        L = self.bucket.get_bucket_list()
        self.assertListEqual(L, self.bucket_list)

    @patch('s3_file_handler.S3FileHandler.bucket_exists')
    @patch('s3_file_handler.S3FileHandler.upload_object_to_bucket')
    def test_upload_object_to_bucket(self, mock_upload, mock_bucketexists):
        # Mock values for the happy path.
        mock_bucketexists.return_value = 200
        mock_upload.return_value = {'status_code': 200}

        bucket = 'test_bucket'
        filename = 'test_file'
        key = 'test_key'

        status_code = self.bucket.upload_object_to_bucket(bucket, filename, key)
        self.assertEqual(status_code, {'status_code': 200})

    @patch('s3_file_handler.S3FileHandler.list_objects_in_bucket')
    def test_list_objects_in_bucket(self, mock_list_objects_in_bucket):
        mock_list_objects_in_bucket.return_value = self.list_objects_in_bucket
        L = self.bucket.list_objects_in_bucket('my_bucket')
        self.assertListEqual(L, self.list_objects_in_bucket)

    @patch('s3_file_handler.S3FileHandler.bucket_exists')
    @patch.object(requests, 'get')
    def test_create_presigned_url(self, mock_get, mock_bucketexists):
        """This mocks the happy path: the bucket exists, the presigned URL can
         be generated, and the GET request is successful."""

        mock_bucketexists.return_value = 200
        mockresponse = Mock()
        mock_get.return_value = mockresponse
        mockresponse.status_code = 200
        mockresponse.url = u'http://aws.s3.test_presigned_url'

        # Run the function and return mock result.
        bucket = 'test_bucket'
        key = 'test_file'
        expiration_time = 120
        mock_result = self.bucket.create_presigned_url(bucket, key,
                                                       expiration_time)

        # Define expected result.
        result = {'status_code': 200,
                  'presigned_url': 'http://aws.s3.test_presigned_url'}

        self.assertEqual(result, mock_result)

    @patch('s3_file_handler.S3FileHandler.bucket_exists')
    def test_create_presigned_url_no_such_bucket(
            self, mock_bucketexists):
        """The bucket bucket_name does not exist. It returns an empty
        dictionary."""

        # Mock value for error message "NoSuchBucket".
        mock_bucketexists.return_value = 404

        # Run the function and return mock result.
        bucket = 'test_bucket'
        key = 'test_file'
        mock_result = self.bucket.create_presigned_url(bucket, key,
                                                       expiration_time=120)

        # Define expected result.
        result = {'status_code': 404,
                  'msg': 'Error: Bucket does not exist.'}

        self.assertEqual(result, mock_result)

    @patch('s3_file_handler.S3FileHandler.bucket_exists')
    @patch.object(requests, 'get')
    def test_create_presigned_url_get_fails(self, mock_get, mock_bucketexists):
        """This mocks the case that the GET request fails, resulting in a
         status code that's not 200. In that case we return that status code
         and an empty string for the presigned URL."""

        # Mock value for bucket exists.
        mock_bucketexists.return_value = 200

        # Mock values for requests response.
        mockresponse = Mock()
        mock_get.return_value = mockresponse
        mockresponse.status_code = 300
        mockresponse.url = ''

        # Run method and return mocked result.
        bucket = 'test_bucket'
        key = 'test_file'
        mock_result = self.bucket.create_presigned_url(bucket, key,
                                                       expiration_time=120)

        # Define expected result.
        result = {'status_code': 300,
                  'msg': 'Error: GET request failed.'}

        self.assertEqual(result, mock_result)


if __name__ == '__main__':
    unittest.main()
