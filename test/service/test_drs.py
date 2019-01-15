import requests
import unittest
import sys

from service import WebServiceTestCase

sys.path.insert(0, ".")

from lambdas.service.app import azul_to_obj


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
        print(response_json)
        file_id = response_json['hits'][0]['files'][0]['uuid']
        get_url = "{}/ga4gh/dos/v1/dataobjects/{}".format(self.base_url, file_id)
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

    def test_data_object_not_found(self):
        """
        Ensures that when an unused identifier is requested a 404 status
        code is returned.
        :return:
        """
        file_id = "NOT_A_GOOD_IDEA"
        get_url = "{}/ga4gh/dos/v1/dataobjects/{}".format(self.base_url, file_id)
        # Should cause a 404 error
        with self.assertRaises(requests.exceptions.HTTPError):
            drs_response = requests.get(get_url)
            drs_response.raise_for_status()

    def test_url_presence(self):
        """
        Demonstrates the presence of URLs that can be used to fetch
        files within the DRS response.
        :return:
        """
        data_object, _ = self.get_data_object()
        print(data_object)
        url = data_object['urls'][0]['url']
        self.assertIn('http', url, "Make sure it is url-like")
        fetch_response = requests.get(url)
        fetch_response.raise_for_status()
        self.assertEqual(fetch_response.status_code, 200, "The file should fetch based"
                                                          "on the URL provided in DRS")


    def test_azul_to_drs(self):
        """
        A unit test rather than integration test for demonstrating
        the function that translates from Azul file index to DRS.
        :return:
        """
        mock_azul_file = {
            "format": "fastq.gz",
            "name": "AB-HE0202B-CZI-day3-Drop_S3_R1_001.fastq.gz",
            "sha256": "a91d88ac03b52649d7299f8a5efcd74fc5b4f8d1901214c2a80b29f325573a37",
            "size": 2067772560,
            "url": "http://localhost:8000/fetch/dss/files/b3ebf536-e8a6-4796-a66a-7b3c088680a3?version=2018-12-05T230803.983133Z&replica=aws",
            "uuid": "b3ebf536-e8a6-4796-a66a-7b3c088680a3",
            "version": "2018-12-05T230803.983133Z"
        }
        # First try to transfer a bad entry
        with self.assertRaises(KeyError):
            obj = azul_to_obj({})

        data_object = azul_to_obj(mock_azul_file)
        self.assertEqual(mock_azul_file['uuid'], data_object['id'])
        self.assertEqual(mock_azul_file['url'], data_object['urls'][0]['url'])




if __name__ == "__main__":
    unittest.main()
