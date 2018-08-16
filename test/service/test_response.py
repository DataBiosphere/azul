#!/usr/bin/python

import json
import unittest
import os

import requests

from azul.service.responseobjects.hca_response_v5 import KeywordSearchResponse, FileSearchResponse
from service import WebServiceTestCase


class TestResponse(WebServiceTestCase):

    data_folder_filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

    def test_key_search_response(self):
        """
        This method tests the KeywordSearchResponse object.
        It will make sure the functionality works as
        appropriate by asserting the apiResponse attribute
        is the same as expected.
        """
        with open('{}/test1index.json'.format(self.data_folder_filepath)) as json_test:
            test_json_input = json.load(json_test)

        with open('{}/keyword_test1.json'.format(self.data_folder_filepath)) as test1:
            expected_output = json.load(test1)

        # Still need a way to test the response.
        keyword_response = KeywordSearchResponse(test_json_input).return_response().to_json()
        # Transform both json objects to a string
        json_expected_output = json.dumps(expected_output, sort_keys=True)
        json_actual_output = json.dumps(keyword_response, sort_keys=True)
        self.assertEqual(json_actual_output, json_expected_output)

    def test_file_search_response(self):
        """
        This method tests the FileSearchResponse object.
        It will make sure the functionality works as
        appropriate by asserting the apiResponse attribute
        is the same as expected.
        """
        with open('{}/test1index.json'.format(self.data_folder_filepath)) as json_test:
            test_json = json.load(json_test)

        with open('{}/filesearch_test1.json'.format(self.data_folder_filepath)) as test1:
            file_search_test = json.load(test1)

        # This is what will be used as the comparing standard
        with open('{}/facets_test_input1.json'.format(
                self.data_folder_filepath)) as facet_input:
            facet_test = json.load(facet_input)

        with open('{}/pagination_test_input1.json'.format(
                self.data_folder_filepath)) as pagination_input:
            pagination_test = json.load(pagination_input)

        # Still need a way to test the response.
        file_search_response = FileSearchResponse(
            test_json,
            pagination_test,
            facet_test
        ).return_response().to_json()

        # Transform both json objects to a string
        json_response = json.dumps(file_search_test, sort_keys=True)  # loaded from json
        json_test = json.dumps(file_search_response, sort_keys=True)  # generated
        self.assertEqual(json_test, json_response)

    def test_file_search_response_sapagination(self):
        """
        This method tests the FileSearchResponse object,
        using 'search_after' pagination.
        """
        with open('{}/test1index.json'.format(self.data_folder_filepath)) as json_test:
            test_json = json.load(json_test)

        with open('{}/filesearch_test2.json'.format(self.data_folder_filepath)) as test1:
            file_search_test = json.load(test1)

        # This is what will be used as the comparing standard
        with open('{}/facets_test_input1.json'.format(
                self.data_folder_filepath)) as facet_input:
            facet_test = json.load(facet_input)

        with open('{}/pagination_test_input2.json'.format(
                self.data_folder_filepath)) as pagination_input:
            pagination_test = json.load(pagination_input)

        # Still need a way to test the response.
        file_search_response = FileSearchResponse(
            test_json,
            pagination_test,
            facet_test
        ).return_response().to_json()

        # Transform both json objects to a string
        json_response = json.dumps(file_search_test, sort_keys=True)  # loaded from json
        json_test = json.dumps(file_search_response, sort_keys=True)  # generated
        self.assertEqual(json_test, json_response)

    def test_summary_endpoint(self):
        url = self.base_url + "repository/files/summary"
        response = requests.get(url)
        response.raise_for_status()
        summary_object = response.json()
        self.assertGreater(summary_object['fileCount'], 0)
        self.assertGreater(summary_object['organCount'], 0)
        self.assertIsNotNone(summary_object['organSummaries'])


if __name__ == '__main__':
    unittest.main()
