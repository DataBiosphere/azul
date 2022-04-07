import requests

from azul.logging import (
    configure_test_logging,
)
from service import (
    WebServiceTestCase,
    patch_dss_source,
    patch_source_cache,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class RepositorySpecimenEndpointTest(WebServiceTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    @patch_dss_source
    @patch_source_cache
    def test_basic_response(self):
        url = self.base_url.set(path='/index/samples',
                                args=dict(catalog=self.catalog))
        response = requests.get(str(url))
        response.raise_for_status()
        response_json = response.json()

        def assert_file_type_summaries(hit):
            self.assertEqual(len(hit['fileTypeSummaries']), 1)
            self.assertIn('fileSource', hit['fileTypeSummaries'][0])
            self.assertIn('format', hit['fileTypeSummaries'][0])
            self.assertGreater(hit['fileTypeSummaries'][0]['count'], 0)
            self.assertGreater(hit['fileTypeSummaries'][0]['totalSize'], 0)

        self.assertIn('hits', response_json)
        self.assertGreater(len(response_json['hits']), 0)
        for hit in response_json['hits']:
            self.assertIn('protocols', hit)
            self.assertIn('entryId', hit)
            assert_file_type_summaries(hit)
            self.assertIn('projects', hit)
            self.assertIn('samples', hit)
            self.assertIn('specimens', hit)
            self.assertIn('cellLines', hit)
            self.assertIn('donorOrganisms', hit)
            self.assertIn('organoids', hit)
            self.assertIn('cellSuspensions', hit)
            self.assertNotIn('projectSummary', hit)
            self.assertNotIn('files', hit)
        self.assertIn('pagination', response_json)
        self.assertIn('termFacets', response_json)
