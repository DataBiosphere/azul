from datetime import (
    datetime,
    timedelta,
)
import json
import logging
import tempfile
from unittest import (
    mock,
)

from dateutil.tz import (
    tzutc,
)
from more_itertools import (
    one,
)
from moto import (
    mock_s3,
    mock_sts,
)
import requests

from azul.deployment import (
    aws,
)
from azul.logging import (
    configure_test_logging,
)
from azul_test_case import (
    AzulUnitTestCase,
)
from service import (
    StorageServiceTestMixin,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class StorageServiceTest(AzulUnitTestCase, StorageServiceTestMixin):
    """
    Functional Test for Storage Service
    """

    @mock_s3
    @mock_sts
    def test_upload_tags(self):
        self.storage_service.create_bucket()

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

    @mock_s3
    @mock_sts
    def test_simple_get_put(self):
        sample_key = 'foo-simple'
        sample_content = b'bar'

        self.storage_service.create_bucket()

        # NOTE: Ensure that the key does not exist before writing.
        with self.assertRaises(self.storage_service.client.exceptions.NoSuchKey):
            self.storage_service.get(sample_key)

        self.storage_service.put(sample_key, sample_content)

        self.assertEqual(sample_content, self.storage_service.get(sample_key))

    @mock_s3
    @mock_sts
    def test_simple_get_unknown_item(self):
        sample_key = 'foo-simple'

        self.storage_service.create_bucket()

        with self.assertRaises(self.storage_service.client.exceptions.NoSuchKey):
            self.storage_service.get(sample_key)

    @mock_s3
    @mock_sts
    def test_presigned_url(self):
        sample_key = 'foo-presigned-url'
        sample_content = json.dumps({'a': 1})

        self.storage_service.create_bucket()
        self.storage_service.put(sample_key, sample_content.encode())

        for expiry_minutes, expect_warning in [(30, True), (90, False)]:
            with self.subTest(expiry_minutes=expiry_minutes, expect_warning=expect_warning):
                with mock.patch.object(aws.s3._request_signer._credentials, '_expiry_time',
                                       new=datetime.now(tz=tzutc()) + timedelta(minutes=expiry_minutes)):
                    assertion_method = self.assertLogs if expect_warning else self.assertNoLogs
                    with assertion_method(level=logging.WARNING) as logs:
                        self.storage_service.get_presigned_url(sample_key, file_name='foo.json')
                    if expect_warning:
                        self.assertRegex(one(logs.output),
                                         r'^WARNING:azul\.service\.storage_service:'
                                         r'Current credentials expire .* before the signed URL would .*')

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
