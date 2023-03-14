import gzip
from io import (
    StringIO,
)
import json
from typing import (
    Callable,
)
from unittest.mock import (
    MagicMock,
    patch,
)

import chalice.app
from moto import (
    mock_s3,
)

from azul import (
    cached_property,
    config,
)
from azul.deployment import (
    aws,
)
from azul.indexer.log_forwarding_controller import (
    LogForwardingController,
)
from azul.service.storage_service import (
    StorageService,
)
from azul.types import (
    JSONs,
)
from azul_test_case import (
    AzulUnitTestCase,
)


class TestLogForwarding(AzulUnitTestCase):
    maxDiff = None

    @property
    def log_bucket(self) -> str:
        return aws.qualified_bucket_name(config.logs_term)

    @property
    def log_file_key(self) -> str:
        prefix = config.alb_access_log_path_prefix('gitlab')
        return '/'.join([prefix, 'AWSLogs', '123123123123', 'elasticloadbalancing', '2023', '01', '01', 'test.log.gz'])

    @cached_property
    def storage_service(self) -> StorageService:
        return StorageService(bucket_name=self.log_bucket)

    @cached_property
    def controller(self) -> LogForwardingController:
        return LogForwardingController(app=MagicMock())

    @mock_s3
    def test_alb(self):
        self.storage_service.create_bucket()
        log_escape_sequences_by_input = {
            # Quotation marks are escaped because they are used wrap fields that
            # may contain spaces
            '"': r'\x22',
            # Backslashes are escaped because they are used to escape non-ASCII
            # characters (see subsequent cases)
            '\\': r'\x5c',
            # Non-ASCII character (Latin-1)
            'È': r'\xC3\x88',
            # Non-ASCII character (Cyrillic)
            'Ё': r'\xD0\x81',
            # Non-ASCII character (Emoticon)
            '😀': r'\xf0\x9f\x98\x80',
        }

        for raw, escaped in log_escape_sequences_by_input.items():
            with self.subTest(raw=raw):
                input = [' '.join([
                    'https',
                    '2022-12-31T23:55:00.388951Z',
                    'app/azul-gitlab-alb/c051f98624e68d7e',
                    '172.71.0.215:36056', '172.71.0.215:80',
                    '0.000', '0.002', '0.000',
                    '204', '204',
                    '963', '229',
                    '"POST '
                    f'https://gitlab.dev.singlecell.gi.ucsc.edu:443/api/v4/jobs/request?chars={escaped} '
                    'HTTP/1.1"',
                    f'"gitlab-runner 15.6.1 (15-6-stable; go1.18.8; linux/amd64; {escaped})"',
                    'ECDHE-RSA-AES128-GCM-SHA256',
                    'TLSv1.2',
                    'arn:aws:elasticloadbalancing:us-east-1:122796619775:targetgroup/azul-gitlab-http/136c2d6db59941f6',
                    '"Root=1-63b0cbd4-7d218b82786295005dbf8b6d"',
                    '"gitlab.dev.singlecell.gi.ucsc.edu"',
                    '"arn:aws:acm:us-east-1:122796619775:certificate/81241b8e-c875-4a22-a30e-58003ee139ae"',
                    '0',
                    '2022-12-31T23:55:00.386000Z',
                    '"forward"',
                    '"-"', '"-"',
                    '"172.71.0.215:80"',
                    '"204"',
                    '"-"', '"-"',
                ])]
                expected_output = [{
                    '_source_bucket': self.log_bucket,
                    '_source_key': self.log_file_key,
                    'actions_executed': 'forward',
                    'chosen_cert_arn': 'arn:aws:acm:us-east-1:122796619775:certificate/'
                                       '81241b8e-c875-4a22-a30e-58003ee139ae',
                    'client:port': '172.71.0.215:36056',
                    'domain_name': 'gitlab.dev.singlecell.gi.ucsc.edu',
                    'elb': 'app/azul-gitlab-alb/c051f98624e68d7e',
                    'elb_status_code': '204',
                    'error_reason': '-',
                    'matched_rule_priority': '0',
                    'received_bytes': '963',
                    'redirect_url': '-',
                    'request': f'POST https://gitlab.dev.singlecell.gi.ucsc.edu:443'
                               f'/api/v4/jobs/request?chars={escaped} HTTP/1.1',
                    'request_creation_time': '2022-12-31T23:55:00.386000Z',
                    'request_processing_time': '0.000',
                    'response_processing_time': '0.000',
                    'sent_bytes': '229',
                    'ssl_cipher': 'ECDHE-RSA-AES128-GCM-SHA256',
                    'ssl_protocol': 'TLSv1.2',
                    'target:port': '172.71.0.215:80',
                    'target:port_list': '172.71.0.215:80',
                    'target_group_arn': 'arn:aws:elasticloadbalancing:us-east-1:'
                                        '122796619775:targetgroup/azul-gitlab-http/136c2d6db59941f6',
                    'target_processing_time': '0.002',
                    'target_status_code': '204',
                    'target_status_code_list': '204',
                    'time': '2022-12-31T23:55:00.388951Z',
                    'trace_id': 'Root=1-63b0cbd4-7d218b82786295005dbf8b6d',
                    'type': 'https',
                    'user_agent': f'gitlab-runner 15.6.1 (15-6-stable; go1.18.8; linux/amd64; {escaped})'
                }]
                input = gzip.compress('\n'.join(input).encode('ascii'))
                self._test(self.controller.forward_alb_logs, input, expected_output)

    def _test(self,
              forward_method: Callable[[chalice.app.S3Event], None],
              log_file_contents: bytes,
              expected_output: JSONs):
        self.storage_service.put(self.log_file_key, log_file_contents)

        event = chalice.app.S3Event(context={}, event_dict={
            'Records': [{
                's3': {
                    'bucket': {'name': self.log_bucket},
                    'object': {'key': self.log_file_key}
                }
            }]
        })

        with patch('sys.stdout', new=StringIO()) as stdout:
            forward_method(event)
            output = stdout.getvalue()

        output = list(map(json.loads, output.split('\n')[:-1]))
        self.assertEqual(expected_output, output)
