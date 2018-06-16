#!/usr/bin/python

import json
import difflib
import unittest
from chalicelib.responseobjects.hca_response_v5 import KeywordSearchResponse, \
    FileSearchResponse
import os

base_path = os.path.dirname(os.path.abspath(__file__))


class MyTestCase(unittest.TestCase):
    def test_key_search_response(self):
        """
        This method tests the KeywordSearchResponse object.
        It will make sure the functionality works as
        appropriate by asserting the apiResponse attribute
        is the same as expected.
        :return:
        """

        with open('{}/test1index.json'.format(base_path)) as json_test:
            test_json_input = json.load(json_test)
            json_test.close()

        with open('{}/keyword_test1.json'.format(base_path)) as test1:
            expected_output = json.load(test1)
            test1.close()
        # Still need a way to test the response.
        keyword_response = KeywordSearchResponse(test_json_input).return_response().to_json()
        # Transform both json objects to a string
        json_expected_output = json.dumps(expected_output, sort_keys=True)
        json_actual_output = json.dumps(keyword_response, sort_keys=True)

        print("expected: " + json_expected_output)
        print("actual: " + json_actual_output)

        # Now show differences so message is helpful
        print("Comparing the two dictionaries built.")
        print('{}... => {}...'.format(json_actual_output[:20], json_expected_output[:20]))
        for i, s in enumerate(difflib.ndiff(json_actual_output, json_expected_output)):
            if s[0] == ' ':
                continue
            elif s[0] == '-':
                print(u'Delete "{}" from position {}'.format(s[-1], i))
            elif s[0] == '+':
                print(u'Add "{}" to position {}'.format(s[-1], i))
        self.assertEqual(json_actual_output, json_expected_output)

    def test_null_key_search_response(self):
        """
        This method tests the KeywordSearchResponse object.
        It will make sure the functionality works as
        appropriate by asserting the apiResponse attribute is
        the same as expected.
        :return:
        """
        with open('{}/test1index.json'.format(base_path)) as json_test:
            test_json = json.load(json_test)
            json_test.close()

        with open('{}/keyword_null_test1.json'.format(base_path)) as test1:
            keyword_test = json.load(test1)
            test1.close()
        # Still need a way to test the response.
        keyword_response = KeywordSearchResponse(test_json).return_response().to_json()
        # Transform both json objects to a string
        json_expected_output = json.dumps(keyword_test, sort_keys=True)
        json_actual_output = json.dumps(keyword_response, sort_keys=True)

        print("expected: " + json_expected_output)
        print("actual: " + json_actual_output)

        # Now show differences so message is helpful
        print("Comparing the two dictionaries built.")
        print('{}... => {}...'.format(json_actual_output[:20], json_expected_output[:20]))
        for i, s in enumerate(difflib.ndiff(json_actual_output, json_expected_output)):
            if s[0] == ' ':
                continue
            elif s[0] == '-':
                print(u'Delete "{}" from position {}'.format(s[-1], i))
            elif s[0] == '+':
                print(u'Add "{}" to position {}'.format(s[-1], i))
        self.assertEqual(json_actual_output, json_expected_output)

    def test_file_search_response(self):
        """
        This method tests the FileSearchResponse object.
        It will make sure the functionality works as
        appropriate by asserting the apiResponse attribute
        is the same as expected.
        :return:
        """

        with open('{}/test1index.json'.format(base_path)) as json_test:
            test_json = json.load(json_test)
            json_test.close()

        with open('{}/filesearch_test1.json'.format(base_path)) as test1:
            file_search_test = json.load(test1)
            test1.close()

        # This is what will be used as the comparing standard
        with open('{}/facets_test_input1.json'.format(
                base_path)) as facet_input:
            facet_test = json.load(facet_input)
            facet_input.close()

        with open('{}/pagination_test_input1.json'.format(
                base_path)) as pagination_input:
            pagination_test = json.load(pagination_input)
            pagination_input.close()

        # Still need a way to test the response.
        file_search_response = FileSearchResponse(
            test_json,
            pagination_test,
            facet_test
        ).return_response().to_json()

        # Transform both json objects to a string
        json_response = json.dumps(file_search_test, sort_keys=True)  # loaded from json
        json_test = json.dumps(file_search_response, sort_keys=True)  # generated

        print("expected: " + json_response)
        print("actual: " + json_test)

        # Now show differences so message is helpful
        print("Comparing the two dictionaries built.")
        print('{}... => {}...'.format(json_test[:20], json_response[:20]))
        for i, s in enumerate(difflib.ndiff(json_test, json_response)):
            if s[0] == ' ':
                continue
            elif s[0] == '-':
                print(u'Delete "{}" from position {}'.format(s[-1], i))
            elif s[0] == '+':
                print(u'Add "{}" to position {}'.format(s[-1], i))
        self.assertEqual(json_test, json_response)

    def test_file_search_response_sapagination(self):
        """
        This method tests the FileSearchResponse object,
        using 'search_after' pagination.
        :return:
        """

        with open('{}/test1index.json'.format(base_path)) as json_test:
            test_json = json.load(json_test)
            json_test.close()

        with open('{}/filesearch_test2.json'.format(base_path)) as test1:
            file_search_test = json.load(test1)
            test1.close()

        # This is what will be used as the comparing standard
        with open('{}/facets_test_input1.json'.format(
                base_path)) as facet_input:
            facet_test = json.load(facet_input)
            facet_input.close()

        with open('{}/pagination_test_input2.json'.format(
                base_path)) as pagination_input:
            pagination_test = json.load(pagination_input)
            pagination_input.close()

        # Still need a way to test the response.
        file_search_response = FileSearchResponse(
            test_json,
            pagination_test,
            facet_test
        ).return_response().to_json()

        # Transform both json objects to a string
        json_response = json.dumps(file_search_test, sort_keys=True)  # loaded from json
        json_test = json.dumps(file_search_response, sort_keys=True)  # generated

        print("expected: " + json_response)
        print("actual: " + json_test)

        # Now show differences so message is helpful
        print("Comparing the two dictionaries built.")
        print('{}... => {}...'.format(json_test[:20], json_response[:20]))
        for i, s in enumerate(difflib.ndiff(json_test, json_response)):
            if s[0] == ' ':
                continue
            elif s[0] == '-':
                print(u'Delete "{}" from position {}'.format(s[-1], i))
            elif s[0] == '+':
                print(u'Add "{}" to position {}'.format(s[-1], i))
        self.assertEqual(json_test, json_response)


if __name__ == '__main__':
    unittest.main()
