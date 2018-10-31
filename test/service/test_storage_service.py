import json
from unittest import TestCase

from moto import mock_s3, mock_sts
import requests

from azul.service.responseobjects.storage_service import StorageService, GetObjectError, EmptyMultipartUploadError


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

    @mock_s3
    @mock_sts
    def test_multipart_upload_ok_with_one_part(self):
        sample_key = 'foo-multipart-upload'
        sample_content_parts = [
            "a" * 1024  # The last part can be smaller than the limit.
        ]
        expected_content = "".join(sample_content_parts)

        storage_service = StorageService()
        storage_service.create_bucket()
        with storage_service.multipart_upload(sample_key, 'text/plain') as upload:
            for part in sample_content_parts:
                upload.push(part.encode())

        self.assertEqual(expected_content, storage_service.get(sample_key))

    @mock_s3
    @mock_sts
    def test_multipart_upload_ok_with_n_parts(self):
        sample_key = 'foo-multipart-upload'
        sample_content_parts = [
            "a" * 5242880,  # The minimum file size for multipart upload is 5 MB.
            "b" * 5242880,
            "c" * 1024  # The last part can be smaller than the limit.
        ]
        expected_content = "".join(sample_content_parts)

        storage_service = StorageService()
        storage_service.create_bucket()
        with storage_service.multipart_upload(sample_key, 'text/plain') as upload:
            for part in sample_content_parts:
                upload.push(part.encode())

        self.assertEqual(expected_content, storage_service.get(sample_key))

    @mock_s3
    @mock_sts
    def test_multipart_upload_ok_with_merging(self):
        sample_key = 'foo-multipart-upload'
        sample_content_parts = [
            "a" * 1024,  # This part will not be uploaded right away and merged
                         # with the next part.
            "b" * 5242880,
            "c" * 1024
        ]
        expected_content = "".join(sample_content_parts)

        storage_service = StorageService()
        storage_service.create_bucket()
        with storage_service.multipart_upload(sample_key, 'text/plain') as upload:
            for part in sample_content_parts:
                upload.push(part.encode())

        self.assertEqual(expected_content, storage_service.get(sample_key))

    @mock_s3
    @mock_sts
    def test_multipart_upload_error_with_nothing_pushed(self):
        sample_key = 'foo-multipart-upload-error'
        storage_service = StorageService()
        storage_service.create_bucket()
        with self.assertRaises(EmptyMultipartUploadError):
            with storage_service.multipart_upload(sample_key, 'text/plain') as upload:
                pass  # upload nothing... this should fail the "complete" process.