#! /usr/bin/env python

import unittest
from mock import patch
from mock import MagicMock
from s3_file_handler import S3FileHandler as s3handler
import botocore
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

    @patch('s3_file_handler.S3FileHandler.create_bucket')
    def test_create_bucket(self, mock_create_bucket):
        mock_create_bucket.return_value = self.response
        r = self.bucket.create_bucket(
            bucket_name='bagtest')
        self.assertDictEqual(r, self.response)

    @patch('s3_file_handler.S3FileHandler.bucket_exists')
    def test_bucket_exists(self, mock_bucket_exists):
        mock_bucket_exists.return_value = 200
        status_code = self.bucket.bucket_exists('my_bucket')
        self.assertEqual(status_code, 200)

        mock_bucket_exists.return_value = 403
        status_code = self.bucket.bucket_exists('my_bucket')
        self.assertEqual(status_code, 403)

    @patch('s3_file_handler.boto3')
    def test_bucket_exists_client_error(self, boto3):
        boto3 = MagicMock()
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

    @patch('s3_file_handler.S3FileHandler.upload_object_to_bucket')
    def test_upload_object_to_bucket(self, mock_upload_object_to_bucket):
        mock_upload_object_to_bucket.return_value = {'status_code': 200}
        bucket_name = 'my_bucket'
        filename = '/tmp/bdbag.zip'
        key = 'bdbag.zip'
        response = self.bucket.upload_object_to_bucket(
            bucket_name, filename, key)
        self.assertEqual(response, {'status_code': 200})

    @patch('s3_file_handler.S3FileHandler.list_objects_in_bucket')
    def test_list_objects_in_bucket(self, mock_list_objects_in_bucket):
        mock_list_objects_in_bucket.return_value = self.list_objects_in_bucket
        L = self.bucket.list_objects_in_bucket('my_bucket')
        self.assertListEqual(L, self.list_objects_in_bucket)

if __name__ == '__main__':
    unittest.main()