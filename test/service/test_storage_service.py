import json
from unittest import TestCase

import requests

from azul.service.responseobjects.storage_service import StorageService, GetObjectError

from s3_test_case_mixin import S3TestCaseMixin


class StorageServiceTest(TestCase, S3TestCaseMixin):
    """ Functional Test for Storage Service """
    def setUp(self):
        self.start_s3_server()

    def tearDown(self):
        self.stop_s3_server()

    def test_simple_get_put_and_delete(self):
        self.s3_client.create_bucket(Bucket='samples')

        sample_key = 'foo-simple'
        sample_content = 'bar'

        storage_service = StorageService('samples', self.s3_client)

        # Ensure that the key does not exist before writing.
        with self.assertRaises(GetObjectError):
            storage_service.get(sample_key)

        storage_service.put(sample_key, sample_content)

        self.assertEqual(sample_content, storage_service.get(sample_key))

        storage_service.delete(sample_key)

        # Ensure that the key does not exist after test.
        with self.assertRaises(GetObjectError):
            storage_service.get(sample_key)

    def test_delete_with_non_existing_key_raises_no_error(self):
        self.s3_client.create_bucket(Bucket='samples')

        sample_key = 'foo-nothing'

        storage_service = StorageService('samples', self.s3_client)
        storage_service.delete(sample_key)

        self.assertTrue(True)

    def test_presigned_url(self):
        self.s3_client.create_bucket(Bucket='samples')

        sample_key = 'foo-presigned-url'
        sample_content = json.dumps({"a": 1})

        storage_service = StorageService('samples', self.s3_client)
        storage_service.put(sample_key, sample_content)

        presigned_url = storage_service.get_presigned_url(sample_key)

        response = requests.get(presigned_url)

        self.assertEqual(sample_content, response.text)