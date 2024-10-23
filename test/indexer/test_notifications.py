from unittest.mock import (
    patch,
)
from uuid import (
    uuid4,
)

from moto import (
    mock_aws,
)
import requests

from app_test_case import (
    LocalAppTestCase,
)
from azul import (
    JSON,
)
from azul.deployment import (
    aws,
)
from azul.hmac import (
    SignatureHelper,
)
from azul_test_case import (
    DCP1TestCase,
)
from sqs_test_case import (
    SqsTestCase,
)


class TestValidNotificationRequests(LocalAppTestCase,
                                    DCP1TestCase,
                                    SqsTestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return 'indexer'

    @mock_aws
    def test_successful_notifications(self):
        self._create_mock_notifications_queue()
        body = {
            'bundle_fqid': {
                'uuid': 'bb2365b9-5a5b-436f-92e3-4fc6d86a9efd',
                'version': '2018-03-28T13:55:26.044Z'
            }
        }
        for delete in False, True:
            with self.subTest(delete=delete):
                response = self._test(body, delete, valid_auth=True)
                self.assertEqual(202, response.status_code)
                self.assertEqual('', response.text)

    @mock_aws
    def test_invalid_notifications(self):
        bodies = {
            'Missing body': {},
            'Missing bundle uuid':
                {
                    'bundle_fqid': {
                        'version': '2018-03-28T13:55:26.044Z'
                    }
                },
            'bundle uuid is None':
                {
                    'bundle_fqid': {
                        'uuid': None,
                        'version': '2018-03-28T13:55:26.044Z'
                    }
                },
            'Missing bundle_version':
                {
                    'bundle_fqid': {
                        'uuid': 'bb2365b9-5a5b-436f-92e3-4fc6d86a9efd'
                    }
                },
            'bundle version is None':
                {
                    'bundle_fqid': {
                        'uuid': 'bb2365b9-5a5b-436f-92e3-4fc6d86a9efd',
                        'version': None
                    }
                },
            'Malformed bundle uuid value':
                {
                    'bundle_fqid': {
                        'uuid': f'}}{str(uuid4())}{{',
                        'version': '2019-12-31T00:00:00.000Z'
                    }
                },
            'Malformed bundle version':
                {
                    'bundle_fqid': {
                        'uuid': str(uuid4()),
                        'version': ''
                    }
                }
        }
        for delete in False, True:
            with self.subTest(endpoint=delete):
                for test, body in bodies.items():
                    with self.subTest(test):
                        response = self._test(body, delete, valid_auth=True)
                        self.assertEqual(400, response.status_code)

    @mock_aws
    def test_invalid_auth_for_notification_request(self):
        self._create_mock_notifications_queue()
        body = {
            'bundle_fqid': {
                'uuid': str(uuid4()),
                'version': 'SomeBundleVersion'
            }
        }
        for delete in False, True:
            with self.subTest(delete=delete):
                response = self._test(body, delete, valid_auth=False)
                self.assertEqual(401, response.status_code)

    def _test(self,
              body: JSON,
              delete: bool,
              *,
              valid_auth: bool
              ) -> requests.Response:
        with patch.object(aws, 'get_hmac_key_and_id') as get_hmac_key_and_id:
            get_hmac_key_and_id.return_value = b'good key', 'the id'
            url = self.base_url.set(path=(self.catalog, 'delete' if delete else 'add'))
            request = requests.Request(method='POST', url=str(url), json=body)
            hmac_support = SignatureHelper()
            if valid_auth:
                return hmac_support.sign_and_send(request)
            else:
                with patch.object(hmac_support, 'resolve_private_key') as p:
                    p.return_value = b'bad key'
                    return hmac_support.sign_and_send(request)
