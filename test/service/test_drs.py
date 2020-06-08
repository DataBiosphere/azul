import json
import unittest
from unittest import mock
import urllib.parse

from more_itertools import one
import requests
import responses

from chalice.config import Config as ChaliceConfig
from azul import (
    drs,
    config,
)
from azul.logging import configure_test_logging
from retorts import ResponsesHelper
from service import WebServiceTestCase

configure_test_logging()


class DataRepositoryServiceEndpointTest(WebServiceTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    def chalice_config(self):
        return ChaliceConfig.create(lambda_timeout=15)

    @responses.activate
    def _get_data_object(self, file_uuid, file_version):
        with ResponsesHelper() as helper:
            helper.add_passthru(self.base_url)
            drs_url = drs.dos_http_object_url(file_uuid, file_version, base_url=self.base_url)
            with mock.patch('time.time', new=lambda: 1547691253.07010):
                dss_url = config.dss_endpoint + '/files/7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb'
                helper.add(responses.Response(method=responses.GET,
                                              url=dss_url,
                                              status=301,
                                              headers={'location': dss_url}))
                helper.add(responses.Response(method=responses.GET,
                                              url=dss_url,
                                              status=302,
                                              headers={'location': 'gs://foo/bar'}))
                drs_response = requests.get(drs_url)
            drs_response.raise_for_status()
            drs_response_json = drs_response.json()
            data_object = drs_response_json['data_object']
            return data_object

    def test_get_data_object(self):
        file_uuid = '7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb'
        file_version = '2018-11-02T113344.698028Z'
        data_object = self._get_data_object(file_uuid, file_version)
        self.assertEqual({
            'id': file_uuid,
            'urls': [
                {
                    'url': f"{self.base_url}/dss/files/{file_uuid}"
                           f"?version={file_version}"
                           f"&replica=aws"
                           f"&wait=1"
                           f"&fileName=SRR3562915_1.fastq.gz"
                },
                {
                    'url':
                        'gs://foo/bar'
                }
            ],
            'size': '195142097',
            'checksums': [
                {
                    'checksum': '77337cb51b2e584b5ae1b99db6c163b988cbc5b894dda2f5d22424978c3bfc7a',
                    'type': 'sha256'
                }
            ],
            'aliases': ['SRR3562915_1.fastq.gz'],
            'version': file_version,
            'name': 'SRR3562915_1.fastq.gz'
        }, data_object)

    def test_data_object_not_found(self):
        try:
            self._get_data_object("NOT_A_GOOD_IDEA", None)
        except requests.exceptions.HTTPError as e:
            self.assertEqual(e.response.status_code, 404)
        else:
            self.fail()


class DRSTest(WebServiceTestCase):
    maxDiff = None

    dss_headers = {
        "X-DSS-SHA1": "7ad306f154ce7de1a9a333cfd9100fc26ef652b4",
        "X-DSS-SHA256": "77337cb51b2e584b5ae1b99db6c163b988cbc5b894dda2f5d22424978c3bfc7a",
        "X-DSS-SIZE": "195142097",
        "X-DSS-VERSION": "2018-11-02T113344.698028Z",
    }

    @responses.activate
    def test_drs(self):
        """
        Mocks the DSS backend, then uses the DRS endpoints as a client is
        expected to.
        """
        file_uuid = '7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb'
        file_version = '2018-11-02T113344.698028Z'
        signed_url = 'https://org-hca-dss-checkout-prod.s3.amazonaws.com/blobs/307.a72.eb6?foo=bar&et=cetera'
        for redirects in (0, 1, 2, 6):
            with self.subTest(redirects=redirects):
                with ResponsesHelper() as helper:
                    helper.add_passthru(self.base_url)
                    self._mock_responses(helper, redirects, file_uuid, signed_url, file_version=file_version)
                    # Make first client request
                    drs_response = requests.get(
                        drs.http_object_url(file_uuid, file_version=file_version, base_url=self.base_url))
                    drs_response.raise_for_status()
                    drs_object = drs_response.json()
                    expected = {
                        'checksums': [
                            {'sha1': '7ad306f154ce7de1a9a333cfd9100fc26ef652b4'},
                            {'sha-256': '77337cb51b2e584b5ae1b99db6c163b988cbc5b894dda2f5d22424978c3bfc7a'}
                        ],
                        'created_time': '2018-11-02T11:33:44.698028Z',
                        'id': '7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb',
                        'self_uri': drs.object_url(file_uuid='7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb',
                                                   file_version='2018-11-02T113344.698028Z'),
                        'size': '195142097',
                        'version': '2018-11-02T113344.698028Z',
                    }
                    if not redirects:
                        # We expect a DRS object with an access URL
                        expected['access_methods'] = [
                            {
                                'access_url': {
                                    'url': ('https://org-hca-dss-checkout-prod.s3.amazonaws.com/'
                                            'blobs/307.a72.eb6?foo=bar&et=cetera')
                                },
                                'type': 'https'
                            }
                        ]
                        self.assertEqual(drs_object, expected)
                        # self.assertEqual(sort_frozen(freeze(drs_object)), sort_frozen(freeze(expected)))
                    else:
                        # DRS object should have an access ID
                        expected['access_methods'] = [
                            {
                                'access_id': ('eyJleGVjdXRpb25faWQiOiAiOTViMWZjZDAtNThjMi00ZjJjLWJiNDgtMTNhZDg1NmMyNGZj'
                                              'IiwgInN0YXJ0X3RpbWUiOiAxNTc1MzI0MzgxLjE5ODM4NjcsICJhdHRlbXB0cyI6IDB9'),
                                'type': 'https'
                            }
                        ]
                        # We must make another request with the access ID
                        self.assertEqual(expected, drs_object)
                        access_id = one(drs_object['access_methods'])['access_id']
                        for _ in range(redirects - 1):
                            # The first redirect gave us the access ID, the rest are retries on 202
                            drs_access_url = drs.http_object_url(file_uuid, file_version=file_version,
                                                                 base_url=self.base_url, access_id=access_id)
                            drs_response = requests.get(drs_access_url)
                            self.assertEqual(drs_response.status_code, 202)
                            self.assertEqual(drs_response.text, '')
                        # The final request should give us just the access URL
                        drs_access_url = drs.http_object_url(file_uuid, file_version=file_version,
                                                             base_url=self.base_url, access_id=access_id)
                        drs_response = requests.get(drs_access_url)
                        self.assertEqual(drs_response.status_code, 200)
                        self.assertEqual(drs_response.json(), {'url': signed_url})

    def _mock_responses(self, helper, redirects, file_uuid, signed_url, file_version=None):
        request_query = {
            'replica': 'aws',
            **({"version": file_version} if file_version else {})
        }
        location_query = {
            **request_query,
            'token': json.dumps({
                'execution_id': '95b1fcd0-58c2-4f2c-bb48-13ad856c24fc',
                'start_time': 1575324381.1983867,
                'attempts': 0
            })
        }
        file_url = f'{config.dss_endpoint}/files/{file_uuid}?'
        initial_url = file_url + urllib.parse.urlencode(request_query)
        retry_url = file_url + urllib.parse.urlencode(location_query)
        assert redirects >= 0
        helper.add_passthru(self.base_url)
        headers_302 = {'location': signed_url}
        headers_301 = {
            'location': retry_url,
            'retry-after': '1'  # the value is arbitrary for our purposes, but nonetheless expected
        }
        if redirects == 0:
            helper.add(responses.Response(method=responses.GET,
                                          url=initial_url,
                                          status=302,
                                          headers=headers_302))
            helper.add(responses.Response(method=responses.HEAD,
                                          url=initial_url,
                                          status=200,
                                          headers=self.dss_headers))
        else:
            helper.add(responses.Response(method=responses.GET,
                                          url=initial_url,
                                          status=301,
                                          headers=headers_301))
            helper.add(responses.Response(method=responses.HEAD,
                                          url=initial_url,
                                          status=200,
                                          headers=self.dss_headers))
            redirects -= 1
            for _ in range(redirects):
                helper.add(responses.Response(method=responses.GET,
                                              url=retry_url,
                                              status=301,
                                              headers=headers_301))
            helper.add(responses.Response(method=responses.GET,
                                          url=retry_url,
                                          status=302,
                                          headers=headers_302))

    @responses.activate
    def test_data_object_not_found(self):
        file_uuid = 'NOT_A_GOOD_IDEA'
        error_body = 'DRS should just proxy the DSS for error responses'
        with ResponsesHelper() as helper:
            helper.add_passthru(self.base_url)
            url = f'{config.dss_endpoint}/files/{file_uuid}'
            helper.add(responses.Response(method=responses.GET,
                                          body=error_body,
                                          url=url,
                                          status=404))
            drs_response = requests.get(
                drs.http_object_url(file_uuid, base_url=self.base_url))
            self.assertEqual(drs_response.status_code, 404)
            self.assertEqual(drs_response.text, error_body)


if __name__ == "__main__":
    unittest.main()
