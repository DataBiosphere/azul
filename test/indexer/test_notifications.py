from unittest.mock import (
    patch,
)
from uuid import (
    uuid4,
)

from moto import (
    mock_sqs,
    mock_sts,
)
import requests
from requests_http_signature import (
    HTTPSignatureAuth,
)

from app_test_case import (
    LocalAppTestCase,
)
from azul import (
    JSON,
    config,
    hmac,
)
from azul.deployment import (
    aws,
)


class TestValidNotificationRequests(LocalAppTestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return "indexer"

    @mock_sts
    @mock_sqs
    def test_successful_notifications(self):
        self._create_mock_notifications_queue()
        body = {
            'match': {
                'bundle_uuid': 'bb2365b9-5a5b-436f-92e3-4fc6d86a9efd',
                'bundle_version': '2018-03-28T13:55:26.044Z'
            }
        }
        for delete in False, True:
            with self.subTest(delete=delete):
                response = self._test(body, delete, valid_auth=True)
                self.assertEqual(202, response.status_code)
                self.assertEqual('', response.text)

    @mock_sts
    @mock_sqs
    def test_invalid_notifications(self):
        bodies = {
            "Missing body": {},
            "Missing bundle_uuid":
                {
                    'match': {
                        'bundle_version': '2018-03-28T13:55:26.044Z'
                    }
                },
            "bundle_uuid is None":
                {
                    'match': {
                        'bundle_uuid': None,
                        'bundle_version': '2018-03-28T13:55:26.044Z'
                    }
                },
            "Missing bundle_version":
                {
                    'match': {
                        'bundle_uuid': 'bb2365b9-5a5b-436f-92e3-4fc6d86a9efd'
                    }
                },
            "bundle_version is None":
                {
                    'match': {
                        'bundle_uuid': 'bb2365b9-5a5b-436f-92e3-4fc6d86a9efd',
                        'bundle_version': None
                    }
                },
            'Malformed bundle_uuis value':
                {
                    'match': {
                        'bundle_uuid': f'}}{str(uuid4())}{{',
                        'bundle_version': "2019-12-31T00:00:00.000Z"
                    }
                },
            'Malformed bundle_version':
                {
                    'match': {
                        'bundle_uuid': str(uuid4()),
                        'bundle_version': ''
                    }
                }
        }
        for delete in False, True:
            with self.subTest(endpoint=delete):
                for test, body in bodies.items():
                    with self.subTest(test):
                        response = self._test(body, delete, valid_auth=True)
                        self.assertEqual(400, response.status_code)

    @mock_sts
    @mock_sqs
    def test_invalid_auth_for_notification_request(self):
        self._create_mock_notifications_queue()
        body = {
            "match": {
                'bundle_uuid': str(uuid4()),
                'bundle_version': 'SomeBundleVersion'
            }
        }
        for delete in False, True:
            with self.subTest(delete=delete):
                response = self._test(body, delete, valid_auth=False)
                self.assertEqual(401, response.status_code)

    def _test(self, body: JSON, delete: bool, valid_auth: bool) -> requests.Response:
        hmac_creds = {'key': b'good key', 'key_id': 'the id'}
        with patch('azul.deployment.aws.get_hmac_key_and_id', return_value=hmac_creds):
            if valid_auth:
                auth = hmac.prepare()
            else:
                auth = HTTPSignatureAuth(key=b'bad key', key_id='the id')
            url = self.base_url.set(path=(self.catalog, 'delete' if delete else 'add'))
            return requests.post(str(url), json=body, auth=auth)

    @staticmethod
    def _create_mock_notifications_queue():
        sqs = aws.resource('sqs')
        sqs.create_queue(QueueName=config.notifications_queue_name())
