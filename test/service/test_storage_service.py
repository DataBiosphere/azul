import json
from unittest import TestCase

from moto import mock_s3, mock_sts
import requests

from azul import config
from azul.service.responseobjects.storage_service import StorageService, GetObjectError


class StorageServiceTest(TestCase):
    """
    Functional Test for Storage Service
    """
    storage_service: StorageService

    @mock_s3
    @mock_sts
    def test_simple_get_put(self):
        sample_key = 'foo-simple'
        sample_content = 'bar'

        storage_service = StorageService()
        storage_service.create_bucket()

        # NOTE: Ensure that the key does not exist before writing.
        with self.assertRaises(GetObjectError):
            storage_service.get(sample_key)

        storage_service.put(sample_key, sample_content)

        self.assertEqual(sample_content, storage_service.get(sample_key))

    @mock_s3
    @mock_sts
    def test_simple_get_unknown_item(self):
        sample_key = 'foo-simple'
        sample_content = 'bar'

        storage_service = StorageService()
        storage_service.create_bucket()

        with self.assertRaises(GetObjectError):
            storage_service.get(sample_key)

    @mock_s3
    @mock_sts
    def test_presigned_url(self):
        sample_key = 'foo-presigned-url'
        sample_content = json.dumps({"a": 1})

        storage_service = StorageService()
        storage_service.create_bucket()
        storage_service.put(sample_key, sample_content)

        presigned_url = storage_service.get_presigned_url(sample_key)

        response = requests.get(presigned_url)

        self.assertEqual(sample_content, response.text)
