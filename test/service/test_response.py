#!/usr/bin/python

import json
import unittest
import os

import requests

from azul.service.responseobjects.hca_response_v5 import KeywordSearchResponse, FileSearchResponse
from service import WebServiceTestCase


class TestResponse(WebServiceTestCase):
    def load_webservice_data(self, filename):
        data_folder_filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
        with open(os.path.join(data_folder_filepath, filename)) as fp:
            return json.load(fp)

    def test_key_search_response(self):
        """
        This method tests the KeywordSearchResponse object.
        It will make sure the functionality works as
        appropriate by asserting the apiResponse attribute
        is the same as expected.
        """
        # Still need a way to test the response.
        keyword_response = KeywordSearchResponse(
            self.load_webservice_data("response_test_input.json")
        ).return_response().to_json()

        self.assertEqual(json.dumps(keyword_response, sort_keys=True),
                         json.dumps(self.load_webservice_data("response_keysearch_output.json"), sort_keys=True))

    def test_file_search_response(self):
        """
        n=1
        This method tests the FileSearchResponse object.
        It will make sure the functionality works as
        appropriate by asserting the apiResponse attribute
        is the same as expected.

        n=2 (test_file_search_response_sapagination)
        This method tests the FileSearchResponse object,
        using 'search_after' pagination.
        """
        for n in [1,2]:
            filesearch_response = FileSearchResponse(
                self.load_webservice_data("response_test_input.json"),
                self.load_webservice_data(f"response_pagination_input{n}.json"),
                self.load_webservice_data("response_facets_input.json")
            ).return_response().to_json()

            self.assertEqual(json.dumps(filesearch_response, sort_keys=True),
                             json.dumps(self.load_webservice_data(f"response_filesearch_output{n}.json"), sort_keys=True))

    def test_summary_endpoint(self):
        url = self.base_url + "repository/files/summary"
        response = requests.get(url)
        response.raise_for_status()
        summary_object = response.json()
        self.assertGreater(summary_object['fileCount'], 0)
        self.assertGreater(summary_object['organCount'], 0)
        self.assertIsNotNone(summary_object['organSummaries'])

    def test_default_sorting_parameter(self):
        base_url = self.base_url
        url = base_url + "repository/files"
        response = requests.get(url)
        response.raise_for_status()
        summary_object = response.json()
        self.assertEqual(summary_object['pagination']["sort"], "entryId")

if __name__ == '__main__':
    unittest.main()
