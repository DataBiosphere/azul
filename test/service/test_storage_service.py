import json
from unittest import TestCase
from unittest.mock import patch

import requests

from azul.service.responseobjects.storage_service import StorageService, GetObjectError

from s3_test_case_mixin import S3TestCaseHelper


class StorageServiceTest(TestCase):
    """ Functional Test for Storage Service """
    storage_service: StorageService

    @classmethod
    def setUpClass(cls):
        S3TestCaseHelper.start_s3_server()

    @classmethod
    def tearDownClass(cls):
        S3TestCaseHelper.stop_s3_server()

    def setUp(self):
        S3TestCaseHelper.s3_create_bucket('samples')

    def tearDown(self):
        S3TestCaseHelper.s3_remove_bucket('samples')

    @patch('boto3.client')
    def test_simple_get_put_and_delete_with_client_override(self, client):
        client.return_value = S3TestCaseHelper.s3_client()

        sample_key = 'foo-simple'
        sample_content = 'bar'

        storage_service = StorageService('samples')

        # Ensure that the key does not exist before writing.
        with self.assertRaises(GetObjectError):
            storage_service.get(sample_key)

        storage_service.put(sample_key, sample_content)

        self.assertEqual(sample_content, storage_service.get(sample_key))

        storage_service.delete(sample_key)

        # Ensure that the key does not exist after test.
        with self.assertRaises(GetObjectError):
            storage_service.get(sample_key)

    @patch('boto3.client')
    def test_delete_with_non_existing_key_raises_no_error(self, client):
        client.return_value = S3TestCaseHelper.s3_client()

        sample_key = 'foo-nothing'

        storage_service = StorageService('samples')
        storage_service.delete(sample_key)

        self.assertTrue(True)

    @patch('boto3.client')
    def test_presigned_url(self, client):
        client.return_value = S3TestCaseHelper.s3_client()

        sample_key = 'foo-presigned-url'
        sample_content = json.dumps({"a": 1})

        storage_service = StorageService('samples')
        storage_service.put(sample_key, sample_content)

        presigned_url = storage_service.get_presigned_url(sample_key)

        response = requests.get(presigned_url)

        self.assertEqual(sample_content, response.text)
