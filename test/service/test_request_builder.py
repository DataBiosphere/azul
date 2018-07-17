#!/usr/bin/python

import json
import difflib
import logging.config
import os
import unittest
from azul.service.responseobjects.elastic_request_builder import ElasticTransformDump as EsTd
import azul.service.config
from azul import config

logging.config.fileConfig(os.path.join(os.path.dirname(azul.service.config.__file__), 'logging.conf'))
logger = logging.getLogger("dashboardService")


class MyTestCase(unittest.TestCase):

    def _load_json(self, name):
        return EsTd.open_and_return_json(os.path.join(os.path.dirname(__file__), name))

    def test_create_request(self):
        """
        Tests creation of a simple request
        :return: True or False depending on the assertion
        """
        # Load files required for this test
        request_config = self._load_json('test_request_config.json')
        expected_output = self._load_json('request_builder_test1.json')
        # Create a simple filter to test on
        sample_filter = {"entity_id": {"is": ["cbb998ce-ddaf-34fa-e163-d14b399c6b34"]}}
        # Need to work on a couple cases:
        # - The empty case
        # - The 1 filter case
        # - The complex multiple filters case

        # Create ElasticTransformDump instance
        es_ts_instance = EsTd(es_endpoint=config.es_endpoint)
        # Create a request object
        es_search = EsTd.create_request(
            sample_filter,
            es_ts_instance.es_client,
            request_config,
            post_filter=True)
        # Convert objects to be compared to strings
        expected_output = json.dumps(
            expected_output,
            sort_keys=True)
        actual_output = json.dumps(
            es_search.to_dict(),
            sort_keys=True)
        # Print the 2 strings for reference
        # print "Printing expected output: \n %s" % expected_output
        # print "Printing actual output: \n %s" % actual_output
        # Now show differences so message is helpful

        print("Comparing the two dictionaries built.")
        print('{}... => {}...'.format(
            actual_output[:20],
            expected_output[:20]))
        for i, s in enumerate(
                difflib.ndiff(actual_output, expected_output)):
            if s[0] == ' ':
                continue
            elif s[0] == '-':
                print(u'Delete "{}" from position {}'.format(s[-1], i))
            elif s[0] == '+':
                print(u'Add "{}" to position {}'.format(s[-1], i))
        # Testing first case with 1 filter
        self.assertEqual(actual_output, expected_output)

    def test_create_request_empty(self):
        """
        Tests creation of an empty request. That is, no filter
        :return: True or false depending on the test
        """
        # Testing with default (that is, no) filter
        # Load files required for this test
        request_config = self._load_json('test_request_config.json')
        expected_output = self._load_json('request_builder_test2.json')
        # Create empty filter
        # TODO: Need some form of handler for the query language
        sample_filter = {}
        # Create ElasticTransformDump instance
        es_ts_instance = EsTd(es_endpoint=config.es_endpoint)
        # Create a request object
        es_search = EsTd.create_request(
            sample_filter,
            es_ts_instance.es_client,
            request_config)
        # Convert objects to be compared to strings
        expected_output = json.dumps(expected_output, sort_keys=True)
        actual_output = json.dumps(es_search.to_dict(), sort_keys=True)
        # Print the 2 strings for reference
        # print "Printing expected output: \n %s" % expected_output
        # print "Printing actual output: \n %s" % actual_output
        # Now show differences so message is helpful
        print("Comparing the two dictionaries built.")
        print('{}... => {}...'.format(
            actual_output[:20], expected_output[:20]))
        for i, s in enumerate(
                difflib.ndiff(actual_output, expected_output)):
            if s[0] == ' ':
                continue
            elif s[0] == '-':
                print(u'Delete "{}" from position {}'.format(s[-1], i))
            elif s[0] == '+':
                print(u'Add "{}" to position {}'.format(s[-1], i))
        # Testing first case with 1 filter
        self.assertEqual(actual_output, expected_output)

    def test_create_request_complex(self):
        """
        Tests creation of a complex request.
        :return: True or false depending on the test
        """
        # Testing with default (that is, no) filter
        # Load files required for this test
        request_config = self._load_json('test_request_config.json')
        expected_output = self._load_json('request_builder_test3.json')
        # Create sample filter
        sample_filter = {
            "entity_id":
                {
                    "is": ["cbb998ce-ddaf-34fa-e163-d14b399c6b34"]
                },
            "entity_version":
                {
                    "is": ["1993-07-19T23:50:09"]
                }
        }

        # Create ElasticTransformDump instance
        es_ts_instance = EsTd(es_endpoint=config.es_endpoint)
        # Create a request object
        es_search = EsTd.create_request(
            sample_filter,
            es_ts_instance.es_client,
            request_config,
            post_filter=True)
        # Convert objects to be compared to strings
        expected_output = json.dumps(expected_output, sort_keys=True)
        actual_output = json.dumps(es_search.to_dict(), sort_keys=True)
        # Print the 2 strings for reference
        print("Printing expected output: \n %s" % expected_output)
        print("Printing actual output: \n %s" % actual_output)
        # Now show differences so message is helpful
        print("Comparing the two dictionaries built.")
        print('{}... => {}...'.format(actual_output[:20], expected_output[:20]))
        for i, s in enumerate(
                difflib.ndiff(actual_output, expected_output)):
            if s[0] == ' ':
                continue
            elif s[0] == '-':
                print(u'Delete "{}" from position {}'.format(s[-1], i))
            elif s[0] == '+':
                print(u'Add "{}" to position {}'.format(s[-1], i))
        # Testing first case with 1 filter
        self.assertEqual(actual_output, expected_output)


if __name__ == '__main__':
    unittest.main()
