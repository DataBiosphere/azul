from datetime import (
    datetime,
    timedelta,
    timezone,
)
import json
import tempfile
from unittest.mock import (
    patch,
)

import requests

from azul.logging import (
    configure_test_logging,
)
from azul.service import (
    storage_service,
)
from azul.service.storage_service import (
    StorageObjectNotFound,
)
from service import (
    StorageServiceTestCase,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class StorageServiceTest(StorageServiceTestCase):
    """
    Functional Test for Storage Service
    """

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

    def test_time_until_object_expires(self):
        test_data = [(1, False), (0, False), (-1, True)]
        for object_age, expect_error in test_data:
            with self.subTest(object_age=object_age, expect_error=expect_error):
                with patch.object(storage_service, 'datetime') as mock_datetime:
                    now = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
                    mock_datetime.now.return_value = now
                    expiration = 7
                    headers = {
                        'Expiration': 'expiry-date="Wed, 01 Jan 2020 00:00:00 UTC", rule-id="Test Rule"',
                        'LastModified': now - timedelta(days=float(expiration), seconds=object_age)
                    }
                    with patch.object(self.storage_service, 'head', return_value=headers):
                        with self.assertLogs(logger=storage_service.log, level='DEBUG') as logs:
                            actual = self.storage_service.time_until_object_expires('foo', expiration)
                            self.assertEqual(0, actual)
                        got_error = any('does not match' in log for log in logs.output)
                        self.assertIs(expect_error, got_error)
