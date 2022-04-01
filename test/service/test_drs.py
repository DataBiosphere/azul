import base64
import json
import unittest
from unittest import (
    mock,
)
from unittest.mock import (
    MagicMock,
)
import urllib.parse

from chalice.config import (
    Config as ChaliceConfig,
)
import requests
import responses

from azul import (
    config,
)
from azul.drs import (
    AccessMethod,
)
from azul.logging import (
    configure_test_logging,
)
from azul.service.drs_controller import (
    DRSController,
    dss_dos_object_url,
    dss_drs_object_uri,
    dss_drs_object_url,
)
from service import (
    DSSUnitTestCase,
    WebServiceTestCase,
    patch_dss_source,
    patch_source_cache,
)


# noinspection PyPep8Naming
def setupModule():
    configure_test_logging()


@patch_dss_source
@patch_source_cache
class DRSEndpointTest(WebServiceTestCase, DSSUnitTestCase):

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

    def _get_data_object(self, file_uuid, file_version):
        with responses.RequestsMock() as helper:
            helper.add_passthru(str(self.base_url))
            drs_url = dss_dos_object_url(file_uuid=file_uuid,
                                         catalog=self.catalog,
                                         file_version=file_version,
                                         base_url=str(self.base_url))
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
                    'url': str(self.base_url.set(path=('repository', 'files', file_uuid),
                                                 args=dict(catalog=self.catalog,
                                                           version=file_version,
                                                           wait=1,
                                                           fileName='SRR3562915_1.fastq.gz')))
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


class DRSTest(WebServiceTestCase, DSSUnitTestCase):
    maxDiff = None

    dss_headers = {
        "X-DSS-SHA1": "7ad306f154ce7de1a9a333cfd9100fc26ef652b4",
        "X-DSS-SHA256": "77337cb51b2e584b5ae1b99db6c163b988cbc5b894dda2f5d22424978c3bfc7a",
        "X-DSS-SIZE": "195142097",
        "X-DSS-VERSION": "2018-11-02T113344.698028Z",
    }

    signed_url = 'https://org-hca-dss-checkout-prod.s3.amazonaws.com/blobs/307.a72.eb6?foo=bar&et=cetera'
    gs_url = 'gs://important-bucket/object/path'

    def test_drs(self):
        """
        Mocks the DSS backend, then uses the DRS endpoints as a client is
        expected to.
        """
        file_uuid = '7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb'
        file_version = '2018-11-02T113344.698028Z'
        for redirects in (0, 1, 2, 6):
            with self.subTest(redirects=redirects):
                with responses.RequestsMock() as helper:
                    helper.add_passthru(str(self.base_url))
                    self._mock_responses(helper, redirects, file_uuid, file_version=file_version)
                    # Make first client request
                    drs_response = requests.get(
                        dss_drs_object_url(file_uuid,
                                           file_version=file_version,
                                           base_url=str(self.base_url)))
                    drs_response.raise_for_status()
                    drs_object = drs_response.json()
                    expected = {
                        'checksums': [
                            {'sha1': '7ad306f154ce7de1a9a333cfd9100fc26ef652b4'},
                            {'sha-256': '77337cb51b2e584b5ae1b99db6c163b988cbc5b894dda2f5d22424978c3bfc7a'}
                        ],
                        'created_time': '2018-11-02T11:33:44.698028Z',
                        'id': '7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb',
                        'self_uri': dss_drs_object_uri(file_uuid='7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb',
                                                       file_version='2018-11-02T113344.698028Z'),
                        'size': '195142097',
                        'version': '2018-11-02T113344.698028Z',
                    }
                    if not redirects:
                        # We expect a DRS object with an access URL
                        expected['access_methods'] = [
                            {
                                'access_url': {
                                    'url': 'https://org-hca-dss-checkout-prod.s3.amazonaws.com/'
                                           'blobs/307.a72.eb6?foo=bar&et=cetera'
                                },
                                'type': 'https'
                            },
                            {
                                'access_url': {'url': 'gs://important-bucket/object/path'},
                                'type': 'gs'
                            }
                        ]
                        self.assertEqual(drs_object, expected)
                    else:
                        # The access IDs are so similar because the mock tokens are the same...
                        expected['access_methods'] = [
                            {
                                'access_id': 'KCd7ImV4ZWN1dGlvbl9pZCI6ICI5NWIxZmNkMC01OGMyLTRmMmMtYmI0OC0xM2FkODU2YzI0Z'
                                             'mMiLCAic3RhcnRfdGltZSI6IDE1NzUzMjQzODEuMTk4Mzg2NywgImF0dGVtcHRzIjogMH0nLC'
                                             'AnYXdzJyk',
                                #               ^ ...but they do differ
                                'type': 'https'
                            },
                            {
                                'access_id': 'KCd7ImV4ZWN1dGlvbl9pZCI6ICI5NWIxZmNkMC01OGMyLTRmMmMtYmI0OC0xM2FkODU2YzI0Z'
                                             'mMiLCAic3RhcnRfdGltZSI6IDE1NzUzMjQzODEuMTk4Mzg2NywgImF0dGVtcHRzIjogMH0nLC'
                                             'AnZ2NwJyk',
                                'type': 'gs'
                            }
                        ]
                        # We must make another request with the access ID
                        self.assertEqual(expected, drs_object)
                        for method in drs_object['access_methods']:
                            access_id = method['access_id']
                            for _ in range(redirects - 1):
                                # The first redirect gave us the access ID, the rest are retries on 202
                                drs_access_url = dss_drs_object_url(file_uuid,
                                                                    file_version=file_version,
                                                                    base_url=str(self.base_url),
                                                                    access_id=access_id)
                                drs_response = requests.get(drs_access_url)
                                self.assertEqual(drs_response.status_code, 202)
                                self.assertEqual(drs_response.text, '')
                            # The final request should give us just the access URL
                            drs_access_url = dss_drs_object_url(file_uuid,
                                                                file_version=file_version,
                                                                base_url=str(self.base_url),
                                                                access_id=access_id)
                            drs_response = requests.get(drs_access_url)
                            self.assertEqual(drs_response.status_code, 200)
                            if method['type'] == AccessMethod.https.scheme:
                                self.assertEqual(drs_response.json(), {'url': self.signed_url})
                            elif method['type'] == AccessMethod.gs.scheme:
                                self.assertEqual(drs_response.json(), {'url': self.gs_url})
                            else:
                                assert False, f'Access type {method["type"]} is not supported'

    def _dss_response(self, file_uuid, file_version, replica, head=False, initial=True, _301=False):
        request_query = {
            'replica': replica,
            **({'version': file_version} if file_version else {}),
            **({} if head else {'directurl': replica == 'gcp'})
        }
        retry_query = {
            **request_query,
            'token': json.dumps({
                'execution_id': '95b1fcd0-58c2-4f2c-bb48-13ad856c24fc',
                'start_time': 1575324381.1983867,
                'attempts': 0
            })
        }
        file_url = f'{config.dss_endpoint}/files/{file_uuid}?'
        initial_url = file_url + urllib.parse.urlencode(request_query)
        retry_url = file_url + urllib.parse.urlencode(retry_query)
        headers_302 = {'location': self.gs_url if replica == 'gcp' else self.signed_url}
        headers_301 = {
            'location': retry_url,
            'retry-after': '1'  # the value is arbitrary for our purposes, but nonetheless expected
        }
        if head:
            return responses.Response(method=responses.HEAD, url=initial_url, status=200, headers=self.dss_headers)
        else:
            return responses.Response(method=responses.GET,
                                      url=initial_url if initial else retry_url,
                                      status=301 if _301 else 302,
                                      headers=headers_301 if _301 else headers_302)

    def _mock_responses(self, helper, redirects, file_uuid, file_version=None):
        assert redirects >= 0
        helper.add_passthru(str(self.base_url))
        if redirects == 0:
            helper.add(self._dss_response(file_uuid, file_version, 'aws', initial=True, _301=False))
            helper.add(self._dss_response(file_uuid, file_version, 'gcp', initial=True, _301=False))
            helper.add(self._dss_response(file_uuid, file_version, 'aws', head=True))
        else:
            helper.add(self._dss_response(file_uuid, file_version, 'aws', initial=True, _301=True))
            helper.add(self._dss_response(file_uuid, file_version, 'gcp', initial=True, _301=True))
            helper.add(self._dss_response(file_uuid, file_version, 'aws', head=True))
            redirects -= 1
            for _ in range(redirects):
                helper.add(self._dss_response(file_uuid, file_version, 'aws', initial=False, _301=True))
                helper.add(self._dss_response(file_uuid, file_version, 'gcp', initial=False, _301=True))
            helper.add(self._dss_response(file_uuid, file_version, 'aws', initial=False, _301=False))
            helper.add(self._dss_response(file_uuid, file_version, 'gcp', initial=False, _301=False))

    def test_data_object_not_found(self):
        file_uuid = 'NOT_A_GOOD_IDEA'
        error_body = 'DRS should just proxy the DSS for error responses'
        with responses.RequestsMock() as helper:
            helper.add_passthru(str(self.base_url))
            url = f'{config.dss_endpoint}/files/{file_uuid}'
            helper.add(responses.Response(method=responses.GET,
                                          body=error_body,
                                          url=url,
                                          status=404))
            drs_response = requests.get(
                dss_drs_object_url(file_uuid, base_url=str(self.base_url)))
            self.assertEqual(drs_response.status_code, 404)
            self.assertEqual(drs_response.text, error_body)


class TestDRSController(unittest.TestCase):

    def test_bad_token(self):
        controller = DRSController(lambda_context=MagicMock(),
                                   file_url_func=MagicMock())
        literal = repr({'a': 'malicious(?) access ID'}).encode()
        bad_access_id = base64.urlsafe_b64encode(literal).rstrip(b'=').decode()
        response = controller.get_object_access(bad_access_id, 'file_uuid', {})
        self.assertEqual(400, response.status_code)
        self.assertEqual('Invalid DRS access ID', response.body)


if __name__ == "__main__":
    unittest.main()
