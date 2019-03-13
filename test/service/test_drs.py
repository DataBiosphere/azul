import unittest

import requests

from azul import drs
from service import WebServiceTestCase


class DataRepositoryServiceEndpointTest(WebServiceTestCase):

    def _get_data_object(self, file_uuid, file_version):
        drs_url = drs.http_object_url(file_uuid, file_version, base_url=self.base_url)
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
                {'url': f"{self.base_url}/dss/files/{file_uuid}?version={file_version}&replica=aws&wait=1"}
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


if __name__ == "__main__":
    unittest.main()
