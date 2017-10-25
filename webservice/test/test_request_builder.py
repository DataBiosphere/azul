#!/usr/bin/python

import json
import difflib
import os
import unittest
from responseobjects.elastic_request_builder import ElasticTransformDump as EsTd

base_path = os.path.dirname(os.path.abspath(__file__))
#es_domain='localhost', es_port=9200, es_protocol='http'
#es_domain=es_domain, es_port=es_port, es_protocol=es_protocol
es_domain = os.getenv('ES_SERVICE', 'localhost')
es_port = os.getenv('ES_PORT', '9200')
es_protocol = os.getenv('ES_PROTOCOL', 'http')

class MyTestCase(unittest.TestCase):

    def test_create_request(self):
        """
        Tests creation of a simple request
        :return: True or False depending on the assertion
        """
        # Load files required for this test
        request_config = EsTd.open_and_return_json('{}/test_request_config.json'.format(base_path))
        expected_output = EsTd.open_and_return_json('{}/request_builder_test1.json'.format(base_path))
        # Create a simple filter to test on
        sample_filter = {"file": {"projectCode": {"is": ["CGL"]}}}
        # Need to work on a couple cases:
        # - The empty case
        # - The 1 filter case
        # - The complex multiple filters case

        # Create ElasticTransformDump instance
        es_ts_instance = EsTd(es_domain=es_domain, es_port=es_port, es_protocol=es_protocol)
        # Create a request object
        es_search = EsTd.create_request(sample_filter, es_ts_instance.es_client, request_config, post_filter=True)
        # Convert objects to be compared to strings
        expected_output = json.dumps(expected_output, sort_keys=True)
        actual_output = json.dumps(es_search.to_dict(), sort_keys=True)
        # Print the 2 strings for reference
        print "Printing expected output: \n %s" % expected_output
        print "Printing actual output: \n %s" % actual_output
        # Now show differences so message is helpful
        print "Comparing the two dictionaries built."
        print('{}... => {}...'.format(actual_output[:20], expected_output[:20]))
        for i, s in enumerate(difflib.ndiff(actual_output, expected_output)):
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
        request_config = EsTd.open_and_return_json('{}/test_request_config.json'.format(base_path))
        expected_output = EsTd.open_and_return_json('{}/request_builder_test2.json'.format(base_path))
        # Create empty filter
        sample_filter = {"file": {}}  # TODO: Need some form of handler for the query language
        # Create ElasticTransformDump instance
        es_ts_instance = EsTd(es_domain=es_domain, es_port=es_port, es_protocol=es_protocol)
        # Create a request object
        es_search = EsTd.create_request(sample_filter, es_ts_instance.es_client, request_config)
        # Convert objects to be compared to strings
        expected_output = json.dumps(expected_output, sort_keys=True)
        actual_output = json.dumps(es_search.to_dict(), sort_keys=True)
        # Print the 2 strings for reference
        print "Printing expected output: \n %s" % expected_output
        print "Printing actual output: \n %s" % actual_output
        # Now show differences so message is helpful
        print "Comparing the two dictionaries built."
        print('{}... => {}...'.format(actual_output[:20], expected_output[:20]))
        for i, s in enumerate(difflib.ndiff(actual_output, expected_output)):
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
        request_config = EsTd.open_and_return_json('{}/test_request_config.json'.format(base_path))
        expected_output = EsTd.open_and_return_json('{}/request_builder_test3.json'.format(base_path))
        # Create sample filter
        sample_filter = {"file": {"project": {"is": ["CGP", "CAR", "CGL"]}, "analysis_type": {
            "is": ["sequence_upload", "rna_seq_quantification"]}, "file_type": {"is": ["fastq.gz", "bam"]}}}
        # Create ElasticTransformDump instance
        es_ts_instance = EsTd(es_domain=es_domain, es_port=es_port, es_protocol=es_protocol)
        # Create a request object
        es_search = EsTd.create_request(sample_filter, es_ts_instance.es_client, request_config, post_filter=True)
        # Convert objects to be compared to strings
        expected_output = json.dumps(expected_output, sort_keys=True)
        actual_output = json.dumps(es_search.to_dict(), sort_keys=True)
        # Print the 2 strings for reference
        print "Printing expected output: \n %s" % expected_output
        print "Printing actual output: \n %s" % actual_output
        # Now show differences so message is helpful
        print "Comparing the two dictionaries built."
        print('{}... => {}...'.format(actual_output[:20], expected_output[:20]))
        for i, s in enumerate(difflib.ndiff(actual_output, expected_output)):
            if s[0] == ' ':
                continue
            elif s[0] == '-':
                print(u'Delete "{}" from position {}'.format(s[-1], i))
            elif s[0] == '+':
                print(u'Add "{}" to position {}'.format(s[-1], i))
        # Testing first case with 1 filter
        self.assertEqual(actual_output, expected_output)

    def test_transform_request(self):
        """
        Testes the transform request function from ElasticTransformDump. This test assumes that ElasticSearch is
        operational, and that the domain and port for ElasticSearch are provided in the form of environmental
        variables. It must be loaded with known test data
        :return: True or false depending on the assertion
        """
        # Get the parameters to be used to the EsTd
        mapping_config = '../test/test_mapping_config.json'
        request_config = '../test/test_request_config.json'
        sample_filter = {"file": {"project": {"is": ["CGP", "CAR", "CGL"]}, "analysis_type": {
            "is": ["sequence_upload", "rna_seq_quantification"]}, "file_type": {"is": ["fastq.gz", "bam"]}}}
        post_filter = True
        pagination = {
            "from": 1,
            "order": "desc",
            "size": 5,
            "sort": "center_name",
        }
        # Set up the ElasticTransformDump  instance
        es_domain = os.getenv('ES_DOMAIN', 'localhost')
        es_port = os.getenv('ES_PORT', 9200)
        es_requester = EsTd(es_domain=es_domain, es_port=es_port, es_protocol=es_protocol)

        actual_output = es_requester.transform_request(request_config_file=request_config,
                                                       mapping_config_file=mapping_config,
                                                       filters=sample_filter,
                                                       pagination=pagination,
                                                       post_filter=post_filter)
        expected_output = EsTd.open_and_return_json('test_transform_request_test1.json')
        # Convert objects to be compared to strings
        expected_output = json.dumps(expected_output, sort_keys=True)
        actual_output = json.dumps(actual_output, sort_keys=True)
        # Print the 2 strings for reference
        print "Printing expected output: \n %s" % expected_output
        print "Printing actual output: \n %s" % actual_output
        # Now show differences so message is helpful
        print "Comparing the two dictionaries built."
        print('{}... => {}...'.format(actual_output[:20], expected_output[:20]))
        for i, s in enumerate(difflib.ndiff(actual_output, expected_output)):
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
