import json

from chalice.config import Config as ChaliceConfig
import requests
import responses

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
        azul_url = f'{self.base_url}/fetch/dss/files/{file_uuid}?replica=aws&version={file_version}'
        response = requests.get(azul_url, allow_redirects=False)
        self.assertEqual(200, response.status_code)
        body = json.loads(response.content)
        self.assertEqual(azul_url + token, body['Location'])
        self.assertEqual(retry_after, body["Retry-After"])
        self.assertEqual(301, body["Status"])
        responses.add(responses.Response(method='GET',
                                         url=dss_url + token,
                                         status=302,
                                         headers={'Location': s3_url}))
        response = requests.get(azul_url + token, allow_redirects=False)
        self.assertEqual(200, response.status_code)
        body = json.loads(response.content)
        self.assertEqual(302, body["Status"])
        self.assertEqual(s3_url, body['Location'])
