import json
from unittest import (
    mock,
)

import furl
from more_itertools import (
    one,
)
import requests

from azul.indexer import (
    BundleFQID,
)
from azul.logging import (
    configure_test_logging,
)
from service import (
    WebServiceTestCase,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class RepositoryProjectsEndpointTest(WebServiceTestCase):
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
        a request for one project (eg. /index/projects/{uuid})
        """

        def get_response_json(uuid=None):
            url = f'{self.base_url}/index/projects/{uuid if uuid else ""}'
            response = requests.get(url, params=dict(catalog=self.catalog))
            response.raise_for_status()
            return response.json()

        def assert_file_type_summaries(hit):
            self.assertEqual(len(hit['fileTypeSummaries']), 1)
            self.assertIn('fileType', hit['fileTypeSummaries'][0])
            self.assertGreater(hit['fileTypeSummaries'][0]['count'], 0)
            self.assertGreater(hit['fileTypeSummaries'][0]['totalSize'], 0)

        hit_properties = {
            'protocols',
            'entryId',
            'projects',
            'samples',
            'specimens',
            'cellLines',
            'donorOrganisms',
            'organoids',
            'cellSuspensions',
            'fileTypeSummaries'
        }
        projects_properties = {
            'projectTitle',
            'projectShortname',
            'laboratory',
            'projectDescription',
            'contributors',
            'publications',
            'arrayExpressAccessions',
            'geoSeriesAccessions',
            'insdcProjectAccessions',
            'insdcStudyAccessions',
            'supplementaryLinks'
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

    @mock.patch('http.client._MAXHEADERS', new=1000)  # https://stackoverflow.com/questions/23055378
    def test_null_json_filters(self):
        imaging_bundle = BundleFQID('94f2ba52-30c8-4de0-a78e-f95a3f8deb9c', '')
        url = furl.furl(url=self.base_url, path='/index/projects/')
        self._index_canned_bundle(imaging_bundle)
        values = [
            ('in situ sequencing', 'ae5237b4-633f-403a-afc6-cb87e6f90db1'),
            (None, 'e8642221-4c2c-4fd7-b926-a68bce363c88')
        ]
        for assay_type, project_id in values:
            with self.subTest(assay_type=assay_type):
                _filter = {'assayType': {'is': [assay_type]}}
                response = requests.get(url.url, params={'filters': json.dumps(_filter)})
                response.raise_for_status()
                response_json = response.json()
                terms = {'term': assay_type, 'count': 1}
                self.assertIn(terms, response_json['termFacets']['assayType']['terms'])
                hit = one(response_json['hits'])
                self.assertEqual(hit['entryId'], project_id)
                if assay_type is None:
                    self.assertNotIn('assayType', (p for p in hit['protocols']))
                else:
                    self.assertIn(assay_type, (_assay
                                               for p in hit['protocols']
                                               for _assay in p.get('assayType', [])))
        self._index_canned_bundle(imaging_bundle, delete=True)
