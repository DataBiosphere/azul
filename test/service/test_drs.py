import base64
import json
from unittest.mock import (
    MagicMock,
)
import urllib.parse

from app_test_case import (
    LocalAppTestCase,
)
from azul import (
    config,
)
from azul.drs import (
    AccessMethod,
)
from azul.http import (
    raise_on_status,
)
from azul.lib.types import (
    MutableJSON,
)
from azul.logging import (
    configure_test_logging,
    get_test_logger,
)
from azul.service.drs_controller import (
    DRSController,
    DRSObject,
    dss_drs_object_uri,
    dss_drs_object_url,
)
from azul_test_case import (
    AzulUnitTestCase,
)
from indexer import (
    DCP1CannedBundleTestCase,
)
from urllib3_mock import (
    Urllib3Mock,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


log = get_test_logger(__name__)


class TestDRSEndpoint(DCP1CannedBundleTestCase, LocalAppTestCase):
    maxDiff = None

    dss_headers = {
        'X-DSS-SHA1': '7ad306f154ce7de1a9a333cfd9100fc26ef652b4',
        'X-DSS-SHA256': '77337cb51b2e584b5ae1b99db6c163b988cbc5b894dda2f5d22424978c3bfc7a',
        'X-DSS-SIZE': '195142097',
        'X-DSS-VERSION': '2018-11-02T113344.698028Z',
    }

    signed_url = 'https://org-hca-dss-checkout-prod.s3.amazonaws.com/blobs/307.a72.eb6?foo=bar&et=cetera'
    gs_url = 'gs://important-bucket/object/path'

    @classmethod
    def app_name(cls) -> str:
        return 'service'

    def test_drs(self):
        """
        Mocks the DSS backend, then uses the DRS endpoints as a client is
        expected to.
        """
        file_uuid = '7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb'
        file_version = '2018-11-02T113344.698028Z'
        for redirects in (0, 1, 2, 6):
            with self.subTest(redirects=redirects):
                with Urllib3Mock(DRSController, DRSObject) as helper:
                    self._mock_responses(helper, redirects, file_uuid, file_version=file_version)
                    # Make first client request
                    url = dss_drs_object_url(file_uuid=file_uuid,
                                             file_version=file_version,
                                             base_url=self.base_url)
                    drs_response = self._http_client.request('GET', str(url))
                    raise_on_status(drs_response)
                    drs_object = drs_response.json()
                    uri = dss_drs_object_uri(file_uuid=file_uuid,
                                             file_version='2018-11-02T113344.698028Z')
                    expected: MutableJSON = {
                        'checksums': [
                            {'sha1': '7ad306f154ce7de1a9a333cfd9100fc26ef652b4'},
                            {'sha-256': '77337cb51b2e584b5ae1b99db6c163b988cbc5b894dda2f5d22424978c3bfc7a'}
                        ],
                        'created_time': '2018-11-02T11:33:44.698028Z',
                        'id': file_uuid,
                        'self_uri': str(uri),
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
                                drs_access_url = dss_drs_object_url(file_uuid=file_uuid,
                                                                    file_version=file_version,
                                                                    base_url=self.base_url,
                                                                    access_id=access_id)
                                drs_response = self._http_client.request('GET', str(drs_access_url))
                                self.assertEqual(drs_response.status, 202)
                                self.assertEqual(drs_response.data.decode(), '')
                            # The final request should give us just the access URL
                            drs_access_url = dss_drs_object_url(file_uuid=file_uuid,
                                                                file_version=file_version,
                                                                base_url=self.base_url,
                                                                access_id=access_id)
                            drs_response = self._http_client.request('GET', str(drs_access_url))
                            self.assertEqual(drs_response.status, 200)
                            if method['type'] == AccessMethod.https.scheme:
                                self.assertEqual(drs_response.json(), {'url': self.signed_url})
                            elif method['type'] == AccessMethod.gs.scheme:
                                self.assertEqual(drs_response.json(), {'url': self.gs_url})
                            else:
                                assert False, f'Access type {method["type"]} is not supported'

    def _dss_response(self,
                      helper: Urllib3Mock,
                      file_uuid,
                      file_version,
                      replica,
                      head=False,
                      initial=True,
                      _301=False
                      ) -> None:
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
            # the value is arbitrary for our purposes, but nonetheless expected
            'retry-after': '1'
        }
        if head:
            helper.add(method='HEAD',
                       url=initial_url,
                       status=200,
                       headers=self.dss_headers)
        else:
            helper.add(method='GET',
                       url=initial_url if initial else retry_url,
                       status=301 if _301 else 302,
                       headers=headers_301 if _301 else headers_302)

    def _mock_responses(self,
                        helper: Urllib3Mock,
                        redirects,
                        file_uuid,
                        file_version=None):
        assert redirects >= 0
        if redirects == 0:
            self._dss_response(helper,
                               file_uuid,
                               file_version,
                               'aws',
                               initial=True,
                               _301=False)
            self._dss_response(helper,
                               file_uuid,
                               file_version,
                               'gcp',
                               initial=True,
                               _301=False)
            self._dss_response(helper, file_uuid, file_version, 'aws', head=True)
        else:
            self._dss_response(helper,
                               file_uuid,
                               file_version,
                               'aws',
                               initial=True,
                               _301=True)
            self._dss_response(helper,
                               file_uuid,
                               file_version,
                               'gcp',
                               initial=True,
                               _301=True)
            self._dss_response(helper, file_uuid, file_version, 'aws', head=True)
            redirects -= 1
            for _ in range(redirects):
                self._dss_response(helper,
                                   file_uuid,
                                   file_version,
                                   'aws',
                                   initial=False,
                                   _301=True)
                self._dss_response(helper,
                                   file_uuid,
                                   file_version,
                                   'gcp',
                                   initial=False,
                                   _301=True)
            self._dss_response(helper,
                               file_uuid,
                               file_version,
                               'aws',
                               initial=False,
                               _301=False)
            self._dss_response(helper,
                               file_uuid,
                               file_version,
                               'gcp',
                               initial=False,
                               _301=False)

    def test_data_object_not_found(self):
        file_uuid = 'NOT_A_GOOD_IDEA'
        error_body = 'DRS should just proxy the DSS for error responses'
        with Urllib3Mock(DRSController, DRSObject) as helper:
            # The controller calls dss_get_file which uses request() with
            # fields={'replica': 'aws', 'directurl': False}. RequestMethods
            # encodes these fields into the URL for GET requests.
            dss_url = f'{config.dss_endpoint}/files/{file_uuid}'
            query = urllib.parse.urlencode({'replica': 'aws', 'directurl': False})
            helper.add(method='GET', url=f'{dss_url}?{query}', status=404, body=error_body)
            url = dss_drs_object_url(file_uuid=file_uuid, base_url=self.base_url)
            drs_response = self._http_client.request('GET', str(url))
            self.assertEqual(404, drs_response.status)
            self.assertEqual(error_body, drs_response.data.decode())


class TestDRSController(AzulUnitTestCase):

    def test_bad_token(self):
        controller = DRSController(app=MagicMock())
        literal = repr({'a': 'malicious(?) access ID'}).encode()
        bad_access_id = base64.urlsafe_b64encode(literal).rstrip(b'=').decode()
        response = controller.get_object_access(bad_access_id, 'file_uuid', {})
        self.assertEqual(400, response.status_code)
        self.assertEqual('Invalid DRS access ID', response.body)
