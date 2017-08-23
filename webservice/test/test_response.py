#!/usr/bin/python

import json
import difflib
import unittest
from responseobjects.api_response import KeywordSearchResponse


class MyTestCase(unittest.TestCase):
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

        with open('keyword_test1.json') as test1:
            keyword_test = json.load(test1)
            test1.close()
        # Still need a way to test the response.
        keyword_response = KeywordSearchResponse(test_mapping, test_json).return_response().to_json()

        print(json.dumps(keyword_response))
        # Use cmp() method for comparing two dictionaries
        print type(keyword_response)
        # Transform both json objects to a string
        json_response = json.dumps(keyword_test, sort_keys=True)
        json_test = json.dumps(keyword_response, sort_keys=True)
        print(json_response)
        print(json_test)
        # Now show differences so message is helpful
        #cmp(keyword_test, keyword_response)
        print('{}... => {}...'.format(json_test[:20], json_response[:20]))
        for i, s in enumerate(difflib.ndiff(json_test, json_response)):
            if s[0] == ' ':
                continue
            elif s[0] == '-':
                print(u'Delete "{}" from position {}'.format(s[-1], i))
            elif s[0] == '+':
                print(u'Add "{}" to position {}'.format(s[-1], i))
        #self.assertEqual(keyword_test, keyword_response.return_response().to_json())
        self.assertEqual(json_test, json_response)
        self.assertEqual('test', 'test')

if __name__ == '__main__':
    unittest.main()
