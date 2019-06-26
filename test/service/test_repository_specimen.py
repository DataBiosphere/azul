import logging

import requests

from service import WebServiceTestCase


def setUpModule():
    logging.basicConfig(level=logging.INFO)


class RepositorySpecimenEndpointTest(WebServiceTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    def test_basic_response(self):
        url = self.base_url + "/repository/samples"
        response = requests.get(url)
        response.raise_for_status()
        response_json = response.json()

        def assert_file_type_summaries(hit):
            self.assertEqual(len(hit['fileTypeSummaries']), 1)
            self.assertTrue('fileType' in hit['fileTypeSummaries'][0])
            self.assertGreater(hit['fileTypeSummaries'][0]['count'], 0)
            self.assertGreater(hit['fileTypeSummaries'][0]['totalSize'], 0)

        self.assertTrue('hits' in response_json)
        self.assertGreater(len(response_json['hits']), 0)
        for hit in response_json['hits']:
            self.assertTrue('protocols' in hit)
            self.assertTrue('entryId' in hit)
            assert_file_type_summaries(hit)
            self.assertTrue('projects' in hit)
            self.assertTrue('samples' in hit)
            self.assertTrue('specimens' in hit)
            self.assertTrue('cellLines' in hit)
            self.assertTrue('donorOrganisms' in hit)
            self.assertTrue('organoids' in hit)
            self.assertTrue('cellSuspensions' in hit)
            self.assertFalse('projectSummary' in hit)
            self.assertFalse('files' in hit)
        self.assertTrue('pagination' in response_json)
        self.assertTrue('termFacets' in response_json)
