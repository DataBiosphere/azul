import requests

from service import WebServiceTestCase


class RepositorySpecimenEndpointTest(WebServiceTestCase):

    def test_basic_response(self):
        url = self.base_url + "/repository/specimens"
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
            self.assertTrue('specimens' in hit)
            self.assertFalse('projectSummary' in hit)
            self.assertFalse('files' in hit)
        self.assertTrue('pagination' in response_json)
        self.assertTrue('termFacets' in response_json)
