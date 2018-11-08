import time
from unittest import TestCase, mock

import requests
import responses
from chalice.config import Config as ChaliceConfig

from app_test_case import LocalAppTestCase
from azul import config


class TestDssProxy(LocalAppTestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return "service"

    def chalice_config(self):
        return ChaliceConfig.create(lambda_timeout=15)

    @responses.activate
    def test_dss_files_proxy(self):
        responses.add_passthru(self.base_url)
        with mock.patch.object(config, 'service_endpoint', lambda: self.base_url):
            file_uuid = '701c9a63-23da-4978-946b-7576b6ad088a'
            file_version = '2018-09-12T121154.054628Z'
            dss_url = f'{config.dss_endpoint}/files/{file_uuid}?replica=aws&version={file_version}'
            s3_url = 'https://org-humancellatlas-dss-checkout-staging.s3.amazonaws.com/blobs/some_blob'
            token = '&token=some_token'
            retry_after = 3
            responses.add(responses.Response(method='GET',
                                             url=dss_url,
                                             status=301,
                                             headers={'Location': dss_url + token,
                                                      'Retry-After': str(retry_after)}))
            azul_url = f'{self.base_url}/dss/files/{file_uuid}?replica=aws&version={file_version}'
            before = time.time()
            response = requests.get(azul_url, allow_redirects=False)
            request_duration = time.time() - before
            self.assertLessEqual(retry_after, request_duration)
            self.assertLess(request_duration, 2 * retry_after)
            self.assertEqual(301, response.status_code)
            self.assertEqual(azul_url + token, response.headers['Location'])
            responses.add(responses.Response(method='GET',
                                             url=dss_url + token,
                                             status=302,
                                             headers={'Location': s3_url}))
            response = requests.get(azul_url + token, allow_redirects=False)
            self.assertEqual(302, response.status_code)
            self.assertEqual(s3_url, response.headers['Location'])
