import json
import tempfile

from moto import (
    mock_s3,
    mock_sts,
)
import requests

from azul.logging import (
    configure_test_logging,
)
from azul.service.storage_service import (
    StorageObjectNotFound,
)
from azul_test_case import (
    AzulUnitTestCase,
)
from service import (
    StorageServiceTestCase,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


@mock_s3
@mock_sts
class StorageServiceTest(AzulUnitTestCase, StorageServiceTestCase):
    """
    Functional Test for Storage Service
    """

    def setUp(self) -> None:
        super().setUp()
        self.storage_service.create_bucket()

    def test_upload_tags(self):
        object_key = 'test_file'
        with tempfile.NamedTemporaryFile('w') as f:
            f.write('some contents')
            f.flush()
            for tags in (None, {}, {'Name': 'foo', 'game': 'bar'}):
                with self.subTest(tags=tags):
                    self.storage_service.upload(file_path=f.name,
                                                object_key=object_key,
                                                tagging=tags)
                    if tags is None:
                        tags = {}
                    upload_tags = self.storage_service.get_object_tagging(object_key)
                    self.assertEqual(tags,
                                     upload_tags)

    def test_simple_get_put(self):
        sample_key = 'foo-simple'
        sample_content = b'bar'

        # NOTE: Ensure that the key does not exist before writing.
        with self.assertRaises(StorageObjectNotFound):
            self.storage_service.get(sample_key)

        self.storage_service.put(sample_key, sample_content)

        self.assertEqual(sample_content, self.storage_service.get(sample_key))

    def test_simple_get_unknown_item(self):
        sample_key = 'foo-simple'

        with self.assertRaises(StorageObjectNotFound):
            self.storage_service.get(sample_key)

    def test_presigned_url(self):
        sample_key = 'foo-presigned-url'
        sample_content = json.dumps({'a': 1})

        self.storage_service.put(sample_key, sample_content.encode())

        for file_name in None, 'foo.json':
            with self.subTest(file_name=file_name):
                presigned_url = self.storage_service.get_presigned_url(sample_key,
                                                                       file_name=file_name)
                response = requests.get(presigned_url)
                if file_name is None:
                    self.assertNotIn('Content-Disposition', response.headers)
                else:
                    # noinspection PyUnreachableCode
                    if False:  # no coverage
                        # Unfortunately, moto does not support emulating S3's
                        # mechanism of specifying response headers via request
                        # parameters (https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html,
                        # section Request Parameters).
                        self.assertEqual(response.headers['Content-Disposition'], f'attachment;filename="{file_name}"')
                self.assertEqual(sample_content, response.text)
