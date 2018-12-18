import json
from unittest import TestCase
from unittest.mock import patch

from moto import mock_s3, mock_sts
import requests

from azul.service.responseobjects.storage_service import (StorageService,
                                                          GetObjectError,
                                                          MultipartUploadError,
                                                          MultipartUploadHandler)


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

        self.assertEqual(sample_content, storage_service.get(sample_key).decode('utf-8'))

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
        with MultipartUploadHandler(sample_key, 'text/plain') as upload:
            for part in sample_content_parts:
                upload.push(part.encode())

        self.assertEqual(expected_content, storage_service.get(sample_key).decode('utf-8'))

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
        with MultipartUploadHandler(sample_key, 'text/plain') as upload:
            for part in sample_content_parts:
                upload.push(part.encode())

        self.assertEqual(expected_content, storage_service.get(sample_key).decode('utf-8'))

    @mock_s3
    @mock_sts
    def test_multipart_upload_error_with_out_of_bound_part(self):
        sample_key = 'foo-multipart-upload'
        sample_content_parts = [
            "a" * 1024,  # This part will cause an error raised by MPU.
            "b" * 5242880,
            "c" * 1024
        ]

        storage_service = StorageService()
        storage_service.create_bucket()

        with self.assertRaises(MultipartUploadError):
            with MultipartUploadHandler(sample_key, 'text/plain') as upload:
                for part in sample_content_parts:
                    upload.push(part.encode())

    @mock_s3
    @mock_sts
    def test_multipart_upload_error_inside_context_with_nothing_pushed(self):
        sample_key = 'foo-multipart-upload-error'
        sample_content_parts = [
            "a" * 5242880,
            "b" * 5242880,
            1234567,  # This should cause an error.
            "c" * 1024
        ]

        storage_service = StorageService()
        storage_service.create_bucket()
        with self.assertRaises(AttributeError):
            with MultipartUploadHandler(sample_key, 'text/plain') as upload:
                for part in sample_content_parts:
                    upload.push(part.encode())

    @mock_s3
    @mock_sts
    def test_multipart_upload_error_inside_thread_with_nothing_pushed(self):
        sample_key = 'foo-multipart-upload-error'
        sample_content_parts = [
            "a" * 5242880,
            "b" * 5242880
        ]

        storage_service = StorageService()
        storage_service.create_bucket()
        with patch.object(MultipartUploadHandler, '_upload_part', side_effect=RuntimeError('test')):
            with self.assertRaises(MultipartUploadError):
                with MultipartUploadHandler(sample_key, 'text/plain') as upload:
                    for part in sample_content_parts:
                        upload.push(part.encode())