from more_itertools import (
    one,
)

from azul.http import (
    raise_on_status,
)
from azul.logging import (
    configure_test_logging,
    get_test_logger,
)
from indexer import (
    DCP1CannedBundleTestCase,
)
from service import (
    WebServiceTestCase,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


log = get_test_logger(__name__)


class TestIndexProjectsEndpoint(DCP1CannedBundleTestCase, WebServiceTestCase):
    # Set a seed so that we can test the detail response with a stable project ID
    seed = 123

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    def test_projects_response(self):
        """
        Verify some basic properties of the /index/projects response and
        that each hit in the response is equal to the single hit response of a
        request for one project (e.g. /index/projects/{uuid})
        """

        def get_response_json(uuid=None):
            url = self.base_url.set(path=('index', 'projects', uuid or ''),
                                    args=dict(catalog=self.catalog))
            response = self._http_client.request('GET', str(url))
            raise_on_status(response)
            return response.json()

        def assert_file_type_summaries(hit):
            self.assertEqual(len(hit['fileTypeSummaries']), 1)
            self.assertIn('fileSource', hit['fileTypeSummaries'][0])
            self.assertIn('format', hit['fileTypeSummaries'][0])
            self.assertGreater(hit['fileTypeSummaries'][0]['count'], 0)
            self.assertGreater(hit['fileTypeSummaries'][0]['totalSize'], 0)

        hit_properties = {
            'protocols',
            'entryId',
            'projects',
            'sources',
            'samples',
            'specimens',
            'cellLines',
            'donorOrganisms',
            'organoids',
            'cellSuspensions',
            'fileTypeSummaries',
            'dates',
        }
        projects_properties = {
            'accessible',
            'projectId',
            'projectTitle',
            'projectShortname',
            'azulSlug',
            'laboratory',
            'projectDescription',
            'contributors',
            'publications',
            'supplementaryLinks',
            'matrices',
            'contributedAnalyses',
            'accessions',
            'tissueAtlas',
            'isTissueAtlasProject',
            'bionetworkName',
            'estimatedCellCount',
            'dataUseRestriction',
            'duosId'
        }
        response_json = get_response_json()
        self.assertIn('hits', response_json)
        self.assertGreater(len(response_json['hits']), 0)
        for hit in response_json['hits']:
            self.assertEqual(hit_properties, set(hit.keys()))
            self.assertEqual(projects_properties, set(one(hit['projects']).keys()))
            assert_file_type_summaries(hit)
            self.assertNotIn('projectSummary', hit)
            self.assertNotIn('files', hit)
            single_hit = get_response_json(hit['entryId'])
            self.assertEqual(hit, single_hit)
        self.assertIn('pagination', response_json)
        self.assertIn('termFacets', response_json)
