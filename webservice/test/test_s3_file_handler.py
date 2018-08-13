#! /usr/bin/env python

import unittest
from mock import patch
from s3_file_handler import S3FileHandler as s3handler


class TestS3FileHandler(unittest.TestCase):

    def setUp(self):
        self.bucket = s3handler(location='us-west-2')
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
    def test_bucket_exist_bool(self, mock_bucket_exists):
        mock_bucket_exists.return_value = True
        tf = self.bucket.bucket_exists('my_bucket')
        self.assertTrue(tf)

    @patch('s3_file_handler.S3FileHandler.get_bucket_list')
    def test_get_bucket_list(self, mock_bucket_list):
        mock_bucket_list.return_value = self.bucket_list
        L = self.bucket.get_bucket_list()
        self.assertListEqual(L, self.bucket_list)

    @patch('s3_file_handler.S3FileHandler.upload_object_to_bucket')
    def test_upload_object_to_bucket(self, mock_upload_object_to_bucket):
        mock_upload_object_to_bucket.return_value = True
        bucket_name = 'my_bucket'
        fname = '/tmp/bdbag.zip'
        name_in_bucket = 'bdbag.zip'
        tf = self.bucket.upload_object_to_bucket(
            bucket_name, fname, name_in_bucket)
        self.assertTrue(tf)

    @patch('s3_file_handler.S3FileHandler.list_objects_in_bucket')
    def test_list_objects_in_bucket(self, mock_list_objects_in_bucket):
        mock_list_objects_in_bucket.return_value = self.list_objects_in_bucket
        L = self.bucket.list_objects_in_bucket('my_bucket')
        self.assertListEqual(L, self.list_objects_in_bucket)

if __name__ == '__main__':
    unittest.main()