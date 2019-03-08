import requests
import unittest
from azul.dos import dos_object_url

from service import WebServiceTestCase

class DataRepositoryServiceEndpointTest(WebServiceTestCase):

    def get_data_object(self):
        """
        Helper function to get a data object using the
        repository/files list feature.
        :return:
        """
        url = self.base_url + "/repository/files"
        response = requests.get(url)
        response.raise_for_status()
        response_json = response.json()
        file_id = response_json['hits'][0]['files'][0]['uuid']
        get_url = self.base_url + dos_object_url(file_id)
        drs_response = requests.get(get_url)
        drs_response.raise_for_status()
        drs_response_json = drs_response.json()
        data_object = drs_response_json['data_object']
        return data_object, file_id

    def test_get_data_object(self):
        """
        Ensures that a file requested via repository can also
        be found via DRS and that the returned document shares
        the same identifier.
        :return:
        """
        data_object, file_id = self.get_data_object()
        self.assertEqual(file_id, data_object['id'], "The IDs should match")
        azul_response = requests.get(f"{self.base_url}/repository/files/{file_id}").json()
        self.assertEqual(azul_response['files'][0]['url'], data_object['urls'][0]['url'])

    def test_data_object_not_found(self):
        """
        Ensures that when an unused identifier is requested a 404 status
        code is returned.
        :return:
        """
        file_id = "NOT_A_GOOD_IDEA"
        get_url = "{}/ga4gh/dos/v1/dataobjects/{}".format(self.base_url, file_id)
        # Should cause a 404 error
        drs_response = requests.get(get_url)
        self.assertEquals(404, drs_response.status_code)

if __name__ == "__main__":
    unittest.main()
