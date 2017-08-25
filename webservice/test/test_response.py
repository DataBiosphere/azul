#!/usr/bin/python

import json
import difflib
import unittest
from responseobjects.api_response import KeywordSearchResponse, FileSearchResponse


class MyTestCase(unittest.TestCase):
    def test_key_search_response(self):
        """
        This method tests the KeywordSearchResponse object. It will make sure the functionality works as
        appropriate by asserting the apiResponse attribute is the same as expected.
        :return:
        """
        with open('test_mapping_config.json') as json_mapping:
            test_mapping = json.load(json_mapping)
            json_mapping.close()

        with open('test1index.json') as json_test:
            test_json = json.load(json_test)
            json_test.close()

        with open('keyword_test1.json') as test1:
            keyword_test = json.load(test1)
            test1.close()
        # Still need a way to test the response.
        keyword_response = KeywordSearchResponse(test_mapping, test_json).return_response().to_json()
        # Transform both json objects to a string
        json_response = json.dumps(keyword_test, sort_keys=True)
        json_test = json.dumps(keyword_response, sort_keys=True)
        # Now show differences so message is helpful
        print "Comparing the two dictionaries built."
        print('{}... => {}...'.format(json_test[:20], json_response[:20]))
        for i, s in enumerate(difflib.ndiff(json_test, json_response)):
            if s[0] == ' ':
                continue
            elif s[0] == '-':
                print(u'Delete "{}" from position {}'.format(s[-1], i))
            elif s[0] == '+':
                print(u'Add "{}" to position {}'.format(s[-1], i))
        self.assertEqual(json_test, json_response)

    def test_file_search_response(self):
        """
        This method tests the FileSearchResponse object. It will make sure the functionality works as
        appropriate by asserting the apiResponse attribute is the same as expected.
        :return:
        """
        with open('test_mapping_config.json') as json_mapping:
            test_mapping = json.load(json_mapping)
            json_mapping.close()

        with open('test1index.json') as json_test:
            test_json = json.load(json_test)
            json_test.close()

        with open('filesearch_test1.json') as test1:
            file_search_test = json.load(test1)
            test1.close()

        # This is what will be used as the comparing standard
        with open('facets_test_input1.json') as facet_input:
            facet_test = json.load(facet_input)
            facet_input.close()

        with open('pagination_test_input1.json') as pagination_input:
            pagination_test = json.load(pagination_input)
            pagination_input.close()

        # Still need a way to test the response.
        file_search_response = FileSearchResponse(test_mapping,
                                                  test_json,
                                                  pagination_test,
                                                  facet_test).return_response().to_json()

        # Transform both json objects to a string
        json_response = json.dumps(file_search_test, sort_keys=True)
        json_test = json.dumps(file_search_response, sort_keys=True)
        # Now show differences so message is helpful
        print "Comparing the two dictionaries built."
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
