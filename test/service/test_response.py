from collections import (
    Counter,
)
from itertools import (
    product,
)
import json
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Sequence,
)
import unittest
from unittest import (
    mock,
)
import urllib.parse

from more_itertools import (
    one,
)
import requests

from app_test_case import (
    LocalAppTestCase,
)
from azul import (
    cached_property,
    config,
)
from azul.collections import (
    none_safe_key,
)
from azul.indexer import (
    BundleFQID,
)
from azul.indexer.document import (
    null_str,
)
from azul.indexer.index_service import (
    IndexService,
)
from azul.logging import (
    configure_test_logging,
)
from azul.service.hca_response_v5 import (
    FileSearchResponse,
    KeywordSearchResponse,
)
from azul.types import (
    JSON,
)
from service import (
    DSSUnitTestCase,
    WebServiceTestCase,
    patch_dss_endpoint,
    patch_source_cache,
)
from service.test_pagination import (
    parse_url_qs,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


@patch_dss_endpoint
@patch_source_cache
class TestResponse(WebServiceTestCase):
    maxDiff = None

    @classmethod
    def bundles(cls) -> List[BundleFQID]:
        return super().bundles() + [
            cls.bundle_fqid(uuid='fa5be5eb-2d64-49f5-8ed8-bd627ac9bc7a',
                            version='2019-02-14T192438.034764Z'),
            cls.bundle_fqid(uuid='d0e17014-9a58-4763-9e66-59894efbdaa8',
                            version='2018-10-03T144137.044509Z'),
            cls.bundle_fqid(uuid='e0ae8cfa-2b51-4419-9cde-34df44c6458a',
                            version='2018-12-05T230917.591044Z'),
            cls.bundle_fqid(uuid='411cd8d5-5990-43cd-84cc-6c7796b8a76d',
                            version='2018-10-18T204655.866661Z'),
            cls.bundle_fqid(uuid='412cd8d5-5990-43cd-84cc-6c7796b8a76d',
                            version='2018-10-18T204655.866661Z'),
            cls.bundle_fqid(uuid='ffac201f-4b1c-4455-bd58-19c1a9e863b4',
                            version='2019-10-09T170735.528600Z'),
        ]

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    def get_hits(self, entity_type: str, entity_id: str):
        """
        Fetches hits from ES instance searching for a particular entity ID
        """
        body = {
            "query": {
                "term": {
                    "entity_id.keyword": entity_id
                }
            }
        }
        # Tests are assumed to only ever run with the azul dev index
        results = self.es_client.search(index=config.es_index_name(catalog=self.catalog,
                                                                   entity_type=entity_type,
                                                                   aggregate=True),
                                        body=body)
        return self._index_service.translate_fields(catalog=self.catalog,
                                                    doc=[results['hits']['hits'][0]['_source']],
                                                    forward=False)

    @cached_property
    def _index_service(self):
        return IndexService()

    def test_key_search_files_response(self):
        """
        This method tests the KeywordSearchResponse object for the files entity type.
        It will make sure the functionality works as appropriate by asserting the
        apiResponse attribute is the same as expected.
        """
        # Still need a way to test the response.
        # FIXME: Use response from `/index/files` to validate
        #        https://github.com/DataBiosphere/azul/issues/2970
        keyword_response = KeywordSearchResponse(
            # the entity_id is hardcoded, but corresponds to the bundle above
            hits=self.get_hits('files', '0c5ac7c0-817e-40d4-b1b1-34c3d5cfecdb'),
            entity_type='files',
            catalog=self.catalog
        ).return_response().to_json()

        expected_response = {
            "hits": [
                {
                    "bundles": [
                        {
                            "bundleUuid": "aaa96233-bf27-44c7-82df-b4dc15ad4d9d",
                            "bundleVersion": "2018-11-02T113344.698028Z"
                        }
                    ],
                    "cellLines": [

                    ],
                    "cellSuspensions": [
                        {
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "selectedCellType": [None],
                            "totalCells": 1,
                            "submissionDate": "2018-11-02T10:02:28.599000Z",
                            "updateDate": "2018-11-02T10:10:10.908000Z",
                        }
                    ],
                    "donorOrganisms": [
                        {
                            "biologicalSex": ["female"],
                            "disease": ['normal'],
                            "developmentStage": [None],
                            "genusSpecies": ["Australopithecus"],
                            "id": ["DID_scRSq06"],
                            "donorCount": 1,
                            "organismAge": [{"value": "38", "unit": "year"}],
                            "organismAgeRange": [{"gte": 1198368000.0, "lte": 1198368000.0}],
                            "submissionDate": "2018-11-02T10:02:12.191000Z",
                            "updateDate": "2018-11-02T10:07:39.622000Z",
                        }
                    ],
                    "entryId": "0c5ac7c0-817e-40d4-b1b1-34c3d5cfecdb",
                    "sources": [{
                        "sourceId": "4b737739-4dc9-5d4b-9989-a4942047c91c",
                        "sourceSpec": "test:/2"
                    }],
                    "files": [
                        {
                            "contentDescription": [None],
                            "format": "fastq.gz",
                            "matrix_cell_count": None,
                            "matrixCellCount": None,
                            "isIntermediate": None,
                            "name": "SRR3562915_1.fastq.gz",
                            "sha256": "77337cb51b2e584b5ae1b99db6c163b988cbc5b894dda2f5d22424978c3bfc7a",
                            "size": 195142097,
                            "submissionDate": "2018-11-02T10:03:39.600000Z",
                            "updateDate": "2018-11-02T10:35:07.705000Z",
                            "source": None,
                            "fileSource": None,
                            "url": None,
                            "uuid": "7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb",
                            "version": "2018-11-02T113344.698028Z"
                        }
                    ],
                    "organoids": [
                    ],
                    "projects": [
                        {
                            "laboratory": ["John Dear"],
                            "projectId": ["e8642221-4c2c-4fd7-b926-a68bce363c88"],
                            "projectShortname": ["Single of human pancreas"],
                            "projectTitle": ["Single cell transcriptome patterns."],
                            "submissionDate": "2018-11-02T10:02:12.133000Z",
                            "updateDate": "2018-11-02T10:07:39.499000Z",
                        }
                    ],
                    "protocols": [
                        {
                            "libraryConstructionApproach": ["Smart-seq2"],
                            "nucleicAcidSource": ["single cell"],
                            "submissionDate": "2018-11-02T10:05:05.547000Z",
                            "updateDate": "2018-11-02T10:05:10.360000Z",
                        },
                        {
                            "instrumentManufacturerModel": ["Illumina NextSeq 500"],
                            "pairedEnd": [True],
                            "submissionDate": "2018-11-02T10:05:05.555000Z",
                            "updateDate": "2018-11-02T10:05:10.376000Z",
                        }
                    ],
                    "samples": [
                        {
                            "sampleEntityType": ["specimens"],
                            "effectiveOrgan": ['pancreas'],
                            "disease": ["normal"],
                            "id": ["DID_scRSq06_pancreas"],
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "preservationMethod": [None],
                            "source": [
                                "specimen_from_organism"
                            ],
                            "submissionDate": "2018-11-02T10:02:12.298000Z",
                            "updateDate": "2018-11-02T10:09:26.517000Z",
                        }
                    ],
                    "specimens": [
                        {
                            "disease": ["normal"],
                            "id": ["DID_scRSq06_pancreas"],
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "preservationMethod": [None],
                            "source": [
                                "specimen_from_organism"
                            ],
                            "submissionDate": "2018-11-02T10:02:12.298000Z",
                            "updateDate": "2018-11-02T10:09:26.517000Z",
                        }
                    ]
                }
            ]
        }
        self.assertElasticsearchResultsEqual(keyword_response, expected_response)

    def test_key_search_samples_response(self):
        """
        KeywordSearchResponse for the specimens endpoint should return file type summaries instead of files
        """
        # FIXME: Use response from `/index/files` to validate
        #        https://github.com/DataBiosphere/azul/issues/2970
        keyword_response = KeywordSearchResponse(
            # the entity_id is hardcoded, but corresponds to the bundle above
            hits=self.get_hits('samples', 'a21dc760-a500-4236-bcff-da34a0e873d2'),
            entity_type='samples',
            catalog=self.catalog
        ).return_response().to_json()

        expected_response = {
            "hits": [
                {
                    "cellLines": [

                    ],
                    "cellSuspensions": [
                        {
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "selectedCellType": [None],
                            "totalCells": 1,
                            "submissionDate": "2018-11-02T10:02:28.599000Z",
                            "updateDate": "2018-11-02T10:10:10.908000Z",
                        }
                    ],
                    "donorOrganisms": [
                        {
                            "biologicalSex": ["female"],
                            "disease": ['normal'],
                            "developmentStage": [None],
                            "genusSpecies": ["Australopithecus"],
                            "id": ["DID_scRSq06"],
                            "donorCount": 1,
                            "organismAge": [{"value": "38", "unit": "year"}],
                            "organismAgeRange": [{"gte": 1198368000.0, "lte": 1198368000.0}],
                            "submissionDate": "2018-11-02T10:02:12.191000Z",
                            "updateDate": "2018-11-02T10:07:39.622000Z",
                        }
                    ],
                    "entryId": "a21dc760-a500-4236-bcff-da34a0e873d2",
                    "fileTypeSummaries": [
                        {
                            "contentDescription": [None],
                            "count": 2,
                            "fileType": "fastq.gz",
                            "format": "fastq.gz",
                            "matrixCellCount": None,
                            "isIntermediate": None,
                            "source": [None],
                            "fileSource": [None],
                            "totalSize": 385472253
                        }
                    ],
                    "organoids": [
                    ],
                    "projects": [
                        {
                            "laboratory": ["John Dear"],
                            "projectId": ["e8642221-4c2c-4fd7-b926-a68bce363c88"],
                            "projectShortname": ["Single of human pancreas"],
                            "projectTitle": ["Single cell transcriptome patterns."],
                            "submissionDate": "2018-11-02T10:02:12.133000Z",
                            "updateDate": "2018-11-02T10:07:39.499000Z",
                        }
                    ],
                    "protocols": [
                        {
                            "instrumentManufacturerModel": ["Illumina NextSeq 500"],
                            "pairedEnd": [True],
                            "submissionDate": "2018-11-02T10:05:05.555000Z",
                            "updateDate": "2018-11-02T10:05:10.376000Z",
                        },
                        {
                            "libraryConstructionApproach": ["Smart-seq2"],
                            "nucleicAcidSource": ["single cell"],
                            "submissionDate": "2018-11-02T10:05:05.547000Z",
                            "updateDate": "2018-11-02T10:05:10.360000Z",
                        }
                    ],
                    "samples": [
                        {
                            "sampleEntityType": "specimens",
                            "effectiveOrgan": "pancreas",
                            "id": "DID_scRSq06_pancreas",
                            "disease": ["normal"],
                            "organ": "pancreas",
                            "organPart": ["islet of Langerhans"],
                            "preservationMethod": None,
                            "source": "specimen_from_organism",
                            "submissionDate": "2018-11-02T10:02:12.298000Z",
                            "updateDate": "2018-11-02T10:09:26.517000Z",
                        }
                    ],
                    "sources": [{
                        "sourceId": "4b737739-4dc9-5d4b-9989-a4942047c91c",
                        "sourceSpec": "test:/2"
                    }],
                    "specimens": [
                        {
                            "disease": ["normal"],
                            "id": ["DID_scRSq06_pancreas"],
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "preservationMethod": [None],
                            "source": [
                                "specimen_from_organism",
                            ],
                            "submissionDate": "2018-11-02T10:02:12.298000Z",
                            "updateDate": "2018-11-02T10:09:26.517000Z",
                        }
                    ]
                }
            ]
        }
        self.assertElasticsearchResultsEqual(keyword_response, expected_response)

    @property
    def paginations(self):
        return [
            {
                "count": 2,
                "order": "desc",
                "pages": 1,
                "size": 5,
                "sort": "entryId",
                "total": 2
            },
            {
                "count": 2,
                "order": "desc",
                "pages": 1,
                "next": str(self.base_url.set(path='/index/files',
                                              args=dict(size=5,
                                                        search_after='cbb998ce-ddaf-34fa-e163-d14b399c6b34',
                                                        search_after_uid='meta%2332'))),
                "size": 5,
                "sort": "entryId",
                "total": 2
            }
        ]

    def test_file_search_response(self):
        """
        n=0: Test the FileSearchResponse object, making sure the functionality works as appropriate by asserting the
        apiResponse attribute is the same as expected.

        n=1: Tests the FileSearchResponse object, using 'next' pagination.
        """
        hits = [
            {
                "bundles": [
                    {
                        "bundleUuid": "aaa96233-bf27-44c7-82df-b4dc15ad4d9d",
                        "bundleVersion": "2018-11-02T113344.698028Z"
                    }
                ],
                "cellLines": [

                ],
                "cellSuspensions": [
                    {
                        "organ": ["pancreas"],
                        "organPart": ["islet of Langerhans"],
                        "selectedCellType": [None],
                        "totalCells": 1,
                        "submissionDate": "2018-11-02T10:02:28.599000Z",
                        "updateDate": "2018-11-02T10:10:10.908000Z",
                    }
                ],
                "donorOrganisms": [
                    {
                        "biologicalSex": ["female"],
                        "disease": ['normal'],
                        "developmentStage": [None],
                        "genusSpecies": ["Australopithecus"],
                        "id": ["DID_scRSq06"],
                        "donorCount": 1,
                        "organismAge": [{"value": "38", "unit": "year"}],
                        "organismAgeRange": [{"gte": 1198368000.0, "lte": 1198368000.0}],
                        "submissionDate": "2018-11-02T10:02:12.191000Z",
                        "updateDate": "2018-11-02T10:07:39.622000Z",
                    }
                ],
                "entryId": "0c5ac7c0-817e-40d4-b1b1-34c3d5cfecdb",
                "files": [
                    {
                        "contentDescription": [None],
                        "format": "fastq.gz",
                        "matrix_cell_count": None,
                        "matrixCellCount": None,
                        "isIntermediate": None,
                        "name": "SRR3562915_1.fastq.gz",
                        "sha256": "77337cb51b2e584b5ae1b99db6c163b988cbc5b894dda2f5d22424978c3bfc7a",
                        "size": 195142097,
                        "source": None,
                        "fileSource": None,
                        "submissionDate": "2018-11-02T10:03:39.600000Z",
                        "updateDate": "2018-11-02T10:35:07.705000Z",
                        "url": None,
                        "uuid": "7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb",
                        "version": "2018-11-02T113344.698028Z"
                    }
                ],
                "organoids": [

                ],
                "projects": [
                    {
                        "laboratory": ["John Dear"],
                        "projectId": ["e8642221-4c2c-4fd7-b926-a68bce363c88"],
                        "projectShortname": ["Single of human pancreas"],
                        "projectTitle": ["Single cell transcriptome patterns."],
                        "submissionDate": "2018-11-02T10:02:12.133000Z",
                        "updateDate": "2018-11-02T10:07:39.499000Z",
                    }
                ],
                "protocols": [
                    {
                        "libraryConstructionApproach": ["Smart-seq2"],
                        "nucleicAcidSource": ["single cell"],
                        "submissionDate": "2018-11-02T10:05:05.547000Z",
                        "updateDate": "2018-11-02T10:05:10.360000Z",
                    },
                    {
                        "instrumentManufacturerModel": ["Illumina NextSeq 500"],
                        "pairedEnd": [True],
                        "submissionDate": "2018-11-02T10:05:05.555000Z",
                        "updateDate": "2018-11-02T10:05:10.376000Z",
                    }
                ],
                "samples": [
                    {
                        "sampleEntityType": ["specimens"],
                        "effectiveOrgan": ['pancreas'],
                        "disease": ["normal"],
                        "id": ["DID_scRSq06_pancreas"],
                        "organ": ["pancreas"],
                        "organPart": ["islet of Langerhans"],
                        "preservationMethod": [None],
                        "source": [
                            "specimen_from_organism",
                        ],
                        "submissionDate": "2018-11-02T10:02:12.298000Z",
                        "updateDate": "2018-11-02T10:09:26.517000Z",
                    }
                ],
                "sources": [{
                    "sourceId": "4b737739-4dc9-5d4b-9989-a4942047c91c",
                    "sourceSpec": "test:/2"
                }],
                "specimens": [
                    {
                        "disease": ["normal"],
                        "id": ["DID_scRSq06_pancreas"],
                        "organ": ["pancreas"],
                        "organPart": ["islet of Langerhans"],
                        "preservationMethod": [None],
                        "source": [
                            "specimen_from_organism",
                        ],
                        "submissionDate": "2018-11-02T10:02:12.298000Z",
                        "updateDate": "2018-11-02T10:09:26.517000Z",
                    }
                ]
            }
        ]
        responses = [
            {
                "hits": hits,
                "pagination": {
                    "count": 2,
                    "order": "desc",
                    "pages": 1,
                    "next": None,
                    "previous": None,
                    "size": 5,
                    "sort": "entryId",
                    "total": 2
                },
                "termFacets": {}
            },
            {
                "hits": hits,
                "pagination": {
                    "count": 2,
                    "order": "desc",
                    "pages": 1,
                    "next": str(self.base_url.set(path='/index/files',
                                                  args=dict(size=5,
                                                            search_after='cbb998ce-ddaf-34fa-e163-d14b399c6b34',
                                                            search_after_uid='meta%2332'))),
                    "previous": None,
                    "size": 5,
                    "sort": "entryId",
                    "total": 2
                },
                "termFacets": {}
            }
        ]
        for n in 0, 1:
            with self.subTest(n=n):
                # FIXME: Use response from `/index/files` to validate
                #        https://github.com/DataBiosphere/azul/issues/2970
                filesearch_response = FileSearchResponse(
                    hits=self.get_hits('files', '0c5ac7c0-817e-40d4-b1b1-34c3d5cfecdb'),
                    pagination=self.paginations[n],
                    facets={},
                    entity_type='files',
                    catalog=self.catalog
                ).return_response().to_json()
                self.assertElasticsearchResultsEqual(filesearch_response, responses[n])

    def test_file_search_response_file_summaries(self):
        """
        Test non-'files' entity type passed to FileSearchResponse will give file summaries
        """
        # FIXME: Use response from `/index/files` to validate
        #        https://github.com/DataBiosphere/azul/issues/2970
        filesearch_response = FileSearchResponse(
            hits=self.get_hits('samples', 'a21dc760-a500-4236-bcff-da34a0e873d2'),
            pagination=self.paginations[0],
            facets={},
            entity_type='samples',
            catalog=self.catalog
        ).return_response().to_json()

        for hit in filesearch_response['hits']:
            self.assertTrue('fileTypeSummaries' in hit)
            self.assertFalse('files' in hit)

    facets_populated = {
        "organ": {
            "doc_count": 21,
            "untagged": {
                "doc_count": 0
            },
            "myTerms": {
                "doc_count_error_upper_bound": 0,
                "sum_other_doc_count": 0,
                "buckets": [
                    {
                        "key": "silver",
                        "doc_count": 11
                    },
                    {
                        "key": "teal",
                        "doc_count": 10
                    }
                ]
            }
        },
        "disease": {
            "doc_count": 21,
            "untagged": {
                "doc_count": 12
            },
            "myTerms": {
                "doc_count_error_upper_bound": 0,
                "sum_other_doc_count": 0,
                "buckets": [
                    {
                        "key": "silver",
                        "doc_count": 9
                    }
                ]
            }
        }
    }

    def test_file_search_response_add_facets(self):
        """
        Test adding facets to FileSearchResponse with missing values in one facet
        and no missing values in the other

        null term should not appear if there are no missing values
        """
        # FIXME: Use response from `/index/files` to validate
        #        https://github.com/DataBiosphere/azul/issues/2970
        facets = FileSearchResponse.add_facets(self.facets_populated)
        expected_output = {
            "organ": {
                "terms": [
                    {
                        "term": "silver",
                        "count": 11
                    },
                    {
                        "term": "teal",
                        "count": 10
                    }
                ],
                "total": 21,
                "type": "terms"
            },
            "disease": {
                "terms": [
                    {
                        "term": "silver",
                        "count": 9
                    },
                    {
                        "term": None,
                        "count": 12
                    }
                ],
                "total": 21,
                "type": "terms"
            }
        }
        self.assertElasticsearchResultsEqual(facets, expected_output)

    def _params(self, filters: Optional[JSON] = None, **params: Any) -> Dict[str, Any]:
        return {
            **({} if filters is None else {'filters': json.dumps(filters)}),
            'catalog': self.catalog,
            **params
        }

    def test_sorting_details(self):
        for entity_type in 'files', 'samples', 'projects', 'bundles':
            with self.subTest(entity_type=entity_type):
                response = requests.get(str(self.base_url.set(path=('index', entity_type),
                                                              args=self._params())))
                response.raise_for_status()
                response_json = response.json()
                # Verify default sort field is set correctly
                self.assertEqual(response_json['pagination']["sort"], self.app_module.sort_defaults[entity_type][0])
                # Verify all fields in the response that are lists of primitives are sorted
                for hit in response_json['hits']:
                    self._verify_sorted_lists(hit)

    def test_transform_request_with_file_url(self):
        for entity_type in ('files', 'bundles'):
            with self.subTest(entity_type=entity_type):
                url = self.base_url.set(path=('index', entity_type), args=self._params())
                response = requests.get(str(url))
                response.raise_for_status()
                response_json = response.json()
                for hit in response_json['hits']:
                    if entity_type == 'files':
                        self.assertEqual(len(hit['files']), 1)
                    else:
                        self.assertGreater(len(hit['files']), 0)
                    for file in hit['files']:
                        self.assertIn('url', file.keys())
                        actual_url = urllib.parse.urlparse(file['url'])
                        actual_query_vars = {k: one(v) for k, v in urllib.parse.parse_qs(actual_url.query).items()}
                        self.assertEqual(url.netloc, actual_url.netloc)
                        self.assertEqual(url.scheme, actual_url.scheme)
                        self.assertIsNotNone(actual_url.path)
                        self.assertEqual(self.catalog, actual_query_vars['catalog'])
                        self.assertIsNotNone(actual_query_vars['version'])

    def test_projects_key_search_response(self):
        """
        Test building response for projects
        Response should include project detail fields that do not appear for other entity type responses
        """
        # FIXME: Use response from `/index/files` to validate
        #        https://github.com/DataBiosphere/azul/issues/2970
        keyword_response = KeywordSearchResponse(
            hits=self.get_hits('projects', 'e8642221-4c2c-4fd7-b926-a68bce363c88'),
            entity_type='projects',
            catalog=self.catalog
        ).return_response().to_json()

        expected_response = {
            "hits": [
                {
                    "cellLines": [

                    ],
                    "cellSuspensions": [
                        {
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "selectedCellType": [None],
                            "totalCells": 1,
                            "submissionDate": "2018-11-02T10:02:28.599000Z",
                            "updateDate": "2018-11-02T10:10:10.908000Z",
                        }
                    ],
                    "donorOrganisms": [
                        {
                            "biologicalSex": ["female"],
                            "disease": ['normal'],
                            "developmentStage": [None],
                            "genusSpecies": ["Australopithecus"],
                            "id": ["DID_scRSq06"],
                            "donorCount": 1,
                            "organismAge": [{"value": "38", "unit": "year"}],
                            "organismAgeRange": [{"gte": 1198368000.0, "lte": 1198368000.0}],
                            "submissionDate": "2018-11-02T10:02:12.191000Z",
                            "updateDate": "2018-11-02T10:07:39.622000Z",
                        }
                    ],
                    "entryId": "e8642221-4c2c-4fd7-b926-a68bce363c88",
                    "fileTypeSummaries": [
                        {
                            "contentDescription": [None],
                            "count": 2,
                            "fileType": "fastq.gz",
                            "format": "fastq.gz",
                            "matrixCellCount": None,
                            "isIntermediate": None,
                            "source": [None],
                            "fileSource": [None],
                            "totalSize": 385472253
                        }
                    ],
                    "organoids": [
                    ],
                    "projects": [
                        {
                            "arrayExpressAccessions": [None],
                            "geoSeriesAccessions": [None],
                            "insdcProjectAccessions": [None],
                            "insdcStudyAccessions": [None],
                            "contributors": [
                                {
                                    "contactName": "Martin, Enge",
                                    "correspondingContributor": None,
                                    "email": "martin.enge@gmail.com",
                                    "institution": "University",
                                    "laboratory": None,
                                    "projectRole": None
                                },
                                {
                                    "contactName": "Matthew,,Green",
                                    "correspondingContributor": False,
                                    "email": "hewgreen@ebi.ac.uk",
                                    "institution": "Farmers Trucks",
                                    "laboratory": "John Dear",
                                    "projectRole": "Human Cell Atlas wrangler"
                                },
                                {
                                    "contactName": "Laura,,Huerta",
                                    "correspondingContributor": False,
                                    "email": "lauhuema@ebi.ac.uk",
                                    "institution": "Farmers Trucks",
                                    "laboratory": "John Dear",
                                    "projectRole": "external curator"
                                }
                            ],
                            "laboratory": ["John Dear"],
                            "projectDescription": "As organisms age, cells accumulate genetic and epigenetic changes "
                                                  "that eventually lead to impaired organ function or catastrophic "
                                                  "failure such as cancer. Here we describe a single-cell "
                                                  "transcriptome analysis of 2544 human pancreas cells from donors, "
                                                  "spanning six decades of life. We find that islet cells from older "
                                                  "donors have increased levels of disorder as measured both by noise "
                                                  "in the transcriptome and by the number of cells which display "
                                                  "inappropriate hormone expression, revealing a transcriptional "
                                                  "instability associated with aging. By analyzing the spectrum of "
                                                  "somatic mutations in single cells from previously-healthy donors, "
                                                  "we find a specific age-dependent mutational signature "
                                                  "characterized by C to A and C to G transversions, indicators of "
                                                  "oxidative stress, which is absent in single cells from human brain "
                                                  "tissue or in a tumor cell line. Cells carrying a high load of such "
                                                  "mutations also express higher levels of stress and senescence "
                                                  "markers, including FOS, JUN, and the cytoplasmic superoxide "
                                                  "dismutase SOD1, markers previously linked to pancreatic diseases "
                                                  "with substantial age-dependent risk, such as type 2 diabetes "
                                                  "mellitus and adenocarcinoma. Thus, our single-cell approach "
                                                  "unveils gene expression changes and somatic mutations acquired in "
                                                  "aging human tissue, and identifies molecular pathways induced by "
                                                  "these genetic changes that could influence human disease. Also, "
                                                  "our results demonstrate the feasibility of using single-cell "
                                                  "RNA-seq data from primary cells to derive meaningful insights into "
                                                  "the genetic processes that operate on aging human tissue and to "
                                                  "determine which molecular mechanisms are coordinated with these "
                                                  "processes. Examination of single cells from primary human pancreas "
                                                  "tissue",
                            "projectId": "e8642221-4c2c-4fd7-b926-a68bce363c88",
                            "projectShortname": "Single of human pancreas",
                            "projectTitle": "Single cell transcriptome patterns.",
                            "publications": [
                                {
                                    "publicationTitle": "Single-Cell Analysis of Human Pancreas Reveals "
                                                        "Transcriptional Signatures of Aging and Somatic Mutation "
                                                        "Patterns.",
                                    "publicationUrl": "https://www.ncbi.nlm.nih.gov/pubmed/28965763"
                                }
                            ],
                            "supplementaryLinks": [
                                "https://www.ebi.ac.uk/gxa/sc/experiments/E-GEOD-81547/Results"
                            ],
                            "submissionDate": "2018-11-02T10:02:12.133000Z",
                            "updateDate": "2018-11-02T10:07:39.499000Z",
                            "matrices": {},
                            "contributorMatrices": {}
                        }
                    ],
                    "protocols": [
                        {
                            "libraryConstructionApproach": ["Smart-seq2"],
                            "nucleicAcidSource": ["single cell"],
                            "submissionDate": "2018-11-02T10:05:05.547000Z",
                            "updateDate": "2018-11-02T10:05:10.360000Z",
                        },
                        {
                            "instrumentManufacturerModel": ["Illumina NextSeq 500"],
                            "pairedEnd": [True],
                            "submissionDate": "2018-11-02T10:05:05.555000Z",
                            "updateDate": "2018-11-02T10:05:10.376000Z",
                        }
                    ],
                    "samples": [
                        {
                            "sampleEntityType": ["specimens"],
                            "effectiveOrgan": ["pancreas"],
                            "disease": ["normal"],
                            "id": ["DID_scRSq06_pancreas"],
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "preservationMethod": [None],
                            "source": [
                                "specimen_from_organism"
                            ],
                            "submissionDate": "2018-11-02T10:02:12.298000Z",
                            "updateDate": "2018-11-02T10:09:26.517000Z",
                        }
                    ],
                    "sources": [{
                        "sourceId": "4b737739-4dc9-5d4b-9989-a4942047c91c",
                        "sourceSpec": "test:/2"
                    }],
                    "specimens": [
                        {
                            "disease": ["normal"],
                            "id": ["DID_scRSq06_pancreas"],
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "preservationMethod": [None],
                            "source": [
                                "specimen_from_organism"
                            ],
                            "submissionDate": "2018-11-02T10:02:12.298000Z",
                            "updateDate": "2018-11-02T10:09:26.517000Z",
                        }
                    ]
                }
            ]
        }
        self.assertElasticsearchResultsEqual(keyword_response, expected_response)

    def test_projects_file_search_response(self):
        """
        Test building response for projects
        Response should include project detail fields that do not appear for other entity type responses
        """
        # FIXME: Use response from `/index/files` to validate
        #        https://github.com/DataBiosphere/azul/issues/2970
        keyword_response = FileSearchResponse(
            hits=self.get_hits('projects', 'e8642221-4c2c-4fd7-b926-a68bce363c88'),
            pagination=self.paginations[0],
            facets=self.facets_populated,
            entity_type='projects',
            catalog=self.catalog
        ).return_response().to_json()

        expected_response = {
            "hits": [
                {
                    "cellLines": [

                    ],
                    "cellSuspensions": [
                        {
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "selectedCellType": [None],
                            "totalCells": 1,
                            "submissionDate": "2018-11-02T10:02:28.599000Z",
                            "updateDate": "2018-11-02T10:10:10.908000Z",
                        }
                    ],
                    "donorOrganisms": [
                        {
                            "biologicalSex": ["female"],
                            "disease": ['normal'],
                            "developmentStage": [None],
                            "genusSpecies": ["Australopithecus"],
                            "id": ["DID_scRSq06"],
                            "donorCount": 1,
                            "organismAge": [{"value": "38", "unit": "year"}],
                            "organismAgeRange": [{"gte": 1198368000.0, "lte": 1198368000.0}],
                            "submissionDate": "2018-11-02T10:02:12.191000Z",
                            "updateDate": "2018-11-02T10:07:39.622000Z",
                        }
                    ],
                    "entryId": "e8642221-4c2c-4fd7-b926-a68bce363c88",
                    "fileTypeSummaries": [
                        {
                            "contentDescription": [None],
                            "count": 2,
                            "fileType": "fastq.gz",
                            "format": "fastq.gz",
                            "matrixCellCount": None,
                            "isIntermediate": None,
                            "source": [None],
                            "fileSource": [None],
                            "totalSize": 385472253
                        }
                    ],
                    "organoids": [
                    ],
                    "projects": [
                        {
                            "arrayExpressAccessions": [None],
                            "geoSeriesAccessions": [None],
                            "insdcProjectAccessions": [None],
                            "insdcStudyAccessions": [None],
                            "contributors": [
                                {
                                    "contactName": "Matthew,,Green",
                                    "correspondingContributor": False,
                                    "email": "hewgreen@ebi.ac.uk",
                                    "institution": "Farmers Trucks",
                                    "laboratory": "John Dear",
                                    "projectRole": "Human Cell Atlas wrangler"
                                },
                                {
                                    "contactName": "Martin, Enge",
                                    "correspondingContributor": None,
                                    "email": "martin.enge@gmail.com",
                                    "institution": "University",
                                    "laboratory": None,
                                    "projectRole": None
                                },
                                {
                                    "contactName": "Laura,,Huerta",
                                    "correspondingContributor": False,
                                    "email": "lauhuema@ebi.ac.uk",
                                    "institution": "Farmers Trucks",
                                    "laboratory": "John Dear",
                                    "projectRole": "external curator"
                                }
                            ],
                            "laboratory": ["John Dear"],
                            "projectDescription": "As organisms age, cells accumulate genetic and epigenetic changes "
                                                  "that eventually lead to impaired organ function or catastrophic "
                                                  "failure such as cancer. Here we describe a single-cell "
                                                  "transcriptome analysis of 2544 human pancreas cells from donors, "
                                                  "spanning six decades of life. We find that islet cells from older "
                                                  "donors have increased levels of disorder as measured both by noise "
                                                  "in the transcriptome and by the number of cells which display "
                                                  "inappropriate hormone expression, revealing a transcriptional "
                                                  "instability associated with aging. By analyzing the spectrum of "
                                                  "somatic mutations in single cells from previously-healthy donors, "
                                                  "we find a specific age-dependent mutational signature "
                                                  "characterized by C to A and C to G transversions, indicators of "
                                                  "oxidative stress, which is absent in single cells from human brain "
                                                  "tissue or in a tumor cell line. Cells carrying a high load of such "
                                                  "mutations also express higher levels of stress and senescence "
                                                  "markers, including FOS, JUN, and the cytoplasmic superoxide "
                                                  "dismutase SOD1, markers previously linked to pancreatic diseases "
                                                  "with substantial age-dependent risk, such as type 2 diabetes "
                                                  "mellitus and adenocarcinoma. Thus, our single-cell approach "
                                                  "unveils gene expression changes and somatic mutations acquired in "
                                                  "aging human tissue, and identifies molecular pathways induced by "
                                                  "these genetic changes that could influence human disease. Also, "
                                                  "our results demonstrate the feasibility of using single-cell "
                                                  "RNA-seq data from primary cells to derive meaningful insights into "
                                                  "the genetic processes that operate on aging human tissue and to "
                                                  "determine which molecular mechanisms are coordinated with these "
                                                  "processes. Examination of single cells from primary human pancreas "
                                                  "tissue",
                            "projectId": "e8642221-4c2c-4fd7-b926-a68bce363c88",
                            "projectShortname": "Single of human pancreas",
                            "projectTitle": "Single cell transcriptome patterns.",
                            "publications": [
                                {
                                    "publicationTitle": "Single-Cell Analysis of Human Pancreas Reveals "
                                                        "Transcriptional Signatures of Aging and Somatic Mutation "
                                                        "Patterns.",
                                    "publicationUrl": "https://www.ncbi.nlm.nih.gov/pubmed/28965763"
                                }
                            ],
                            "supplementaryLinks": [
                                'https://www.ebi.ac.uk/gxa/sc/experiments/E-GEOD-81547/Results'
                            ],
                            "matrices": {},
                            "contributorMatrices": {},
                            "submissionDate": "2018-11-02T10:02:12.133000Z",
                            "updateDate": "2018-11-02T10:07:39.499000Z",
                        }
                    ],
                    "protocols": [
                        {
                            "libraryConstructionApproach": ["Smart-seq2"],
                            "nucleicAcidSource": ["single cell"],
                            "submissionDate": "2018-11-02T10:05:05.547000Z",
                            "updateDate": "2018-11-02T10:05:10.360000Z",
                        },
                        {
                            "instrumentManufacturerModel": ["Illumina NextSeq 500"],
                            "pairedEnd": [True],
                            "submissionDate": "2018-11-02T10:05:05.555000Z",
                            "updateDate": "2018-11-02T10:05:10.376000Z",
                        }
                    ],
                    "samples": [
                        {
                            "sampleEntityType": ["specimens"],
                            "effectiveOrgan": ["pancreas"],
                            "disease": ["normal"],
                            "id": ["DID_scRSq06_pancreas"],
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "preservationMethod": [None],
                            "source": [
                                "specimen_from_organism"
                            ],
                            "submissionDate": "2018-11-02T10:02:12.298000Z",
                            "updateDate": "2018-11-02T10:09:26.517000Z",
                        }
                    ],
                    "sources": [{
                        "sourceId": "4b737739-4dc9-5d4b-9989-a4942047c91c",
                        "sourceSpec": "test:/2"
                    }],
                    "specimens": [
                        {
                            "disease": ["normal"],
                            "id": ["DID_scRSq06_pancreas"],
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "preservationMethod": [None],
                            "source": [
                                "specimen_from_organism"
                            ],
                            "submissionDate": "2018-11-02T10:02:12.298000Z",
                            "updateDate": "2018-11-02T10:09:26.517000Z",
                        }
                    ]
                }
            ],
            "pagination": {
                "count": 2,
                "order": "desc",
                "pages": 1,
                "next": None,
                "previous": None,
                "size": 5,
                "sort": "entryId",
                "total": 2
            },
            "termFacets": {
                "disease": {
                    "terms": [
                        {
                            "count": 9,
                            "term": "silver"
                        },
                        {
                            "count": 12,
                            "term": None
                        }
                    ],
                    "total": 21,
                    "type": "terms"
                },
                "organ": {
                    "terms": [
                        {
                            "count": 11,
                            "term": "silver"
                        },
                        {
                            "count": 10,
                            "term": "teal"
                        }
                    ],
                    "total": 21,
                    "type": "terms"
                }
            }
        }

        self.assertElasticsearchResultsEqual(keyword_response, expected_response)

    def test_project_accessions_response(self):
        """
        This method tests the KeywordSearchResponse object for the projects entity type,
        specifically making sure the accessions fields are present in the response.
        """
        # FIXME: Use response from `/index/files` to validate
        #        https://github.com/DataBiosphere/azul/issues/2970
        keyword_response = KeywordSearchResponse(
            hits=self.get_hits('projects', '627cb0ba-b8a1-405a-b58f-0add82c3d635'),
            entity_type='projects',
            catalog=self.catalog
        ).return_response().to_json()
        expected_response = {
            "hits": [
                {
                    "cellLines": [

                    ],
                    "cellSuspensions": [
                        {
                            "organ": ["brain"],
                            "organPart": ["amygdala"],
                            "selectedCellType": [None],
                            "totalCells": 10000,
                            "submissionDate": "2019-02-14T18:29:42.561000Z",
                            "updateDate": "2019-02-14T18:29:49.098000Z",
                        }
                    ],
                    "donorOrganisms": [
                        {
                            "biologicalSex": ["male"],
                            "disease": ['H syndrome'],
                            "developmentStage": ["human adult stage"],
                            "genusSpecies": ["Homo sapiens"],
                            "id": ["donor_ID_1"],
                            "donorCount": 1,
                            "organismAge": [{"value": "20", "unit": "year"}],
                            "organismAgeRange": [{"gte": 630720000.0, "lte": 630720000.0}],
                            "submissionDate": "2019-02-14T18:29:42.540000Z",
                            "updateDate": "2019-02-14T18:29:48.962000Z",
                        }
                    ],
                    "entryId": "627cb0ba-b8a1-405a-b58f-0add82c3d635",
                    "fileTypeSummaries": [
                        {
                            "contentDescription": [None],
                            "count": 1,
                            "fileType": "bai",
                            "format": "bai",
                            "matrixCellCount": None,
                            "isIntermediate": None,
                            "source": ['DCP/2 Analysis'],
                            "fileSource": ['DCP/2 Analysis'],
                            "totalSize": 2395616
                        },
                        {
                            "contentDescription": [None],
                            "count": 1,
                            "fileType": "bam",
                            "format": "bam",
                            "matrixCellCount": None,
                            "isIntermediate": None,
                            "source": ['DCP/2 Analysis'],
                            "fileSource": ['DCP/2 Analysis'],
                            "totalSize": 55840108
                        },
                        {
                            "contentDescription": [None],
                            "count": 1,
                            "fileType": "csv",
                            "format": "csv",
                            "matrixCellCount": None,
                            "isIntermediate": None,
                            "source": ['DCP/2 Analysis'],
                            "fileSource": ['DCP/2 Analysis'],
                            "totalSize": 665
                        },
                        {
                            "contentDescription": [None],
                            "count": 1,
                            "fileType": "unknown",
                            "format": "unknown",
                            "matrixCellCount": None,
                            "isIntermediate": None,
                            "source": ['DCP/2 Analysis'],
                            "fileSource": ['DCP/2 Analysis'],
                            "totalSize": 2645006
                        },
                        {
                            "contentDescription": [None],
                            "count": 2,
                            "fileType": "mtx",
                            "format": "mtx",
                            "matrixCellCount": None,
                            "isIntermediate": None,
                            "source": ['DCP/2 Analysis'],
                            "fileSource": ['DCP/2 Analysis'],
                            "totalSize": 6561141
                        },
                        {
                            "contentDescription": [None],
                            "count": 3,
                            "fileType": "fastq.gz",
                            "format": "fastq.gz",
                            "matrixCellCount": None,
                            "isIntermediate": None,
                            "source": [None],
                            "fileSource": [None],
                            "totalSize": 44668092
                        },
                        {
                            "contentDescription": [None],
                            "count": 3,
                            "fileType": "h5",
                            "format": "h5",
                            "matrixCellCount": None,
                            "isIntermediate": None,
                            "source": ['DCP/2 Analysis'],
                            "fileSource": ['DCP/2 Analysis'],
                            "totalSize": 5573714
                        },
                        {
                            "contentDescription": [None],
                            "count": 4,
                            "fileType": "tsv",
                            "format": "tsv",
                            "matrixCellCount": None,
                            "isIntermediate": None,
                            "source": ['DCP/2 Analysis'],
                            "fileSource": ['DCP/2 Analysis'],
                            "totalSize": 15872628
                        }
                    ],
                    "organoids": [

                    ],
                    "projects": [
                        {
                            "contributors": [
                                {
                                    "contactName": "John,D,Doe. ",
                                    "correspondingContributor": False,
                                    "email": "dummy@email.com",
                                    "institution": "EMBL-EBI",
                                    "laboratory": "Department of Biology",
                                    "projectRole": "principal investigator"
                                }
                            ],
                            "arrayExpressAccessions": ["E-AAAA-00"],
                            "geoSeriesAccessions": ["GSE00000"],
                            "insdcProjectAccessions": ["SRP000000"],
                            "insdcStudyAccessions": ["PRJNA000000"],
                            "laboratory": ["Department of Biology"],
                            "projectDescription": "Contains a small file set from the dataset: 4k PBMCs from a "
                                                  "Healthy Donor, a Single Cell Gene Expression Dataset by Cell "
                                                  "Ranger 2.1.0. Peripheral blood mononuclear cells (PBMCs) were "
                                                  "taken from a healthy donor (same donor as pbmc8k). PBMCs are "
                                                  "primary cells with relatively small amounts of RNA (~1pg "
                                                  "RNA/cell). Data/Analysis can be found here "
                                                  "https://support.10xgenomics.com/single-cell-gene-expression/datasets"
                                                  "/2.1.0/pbmc4k and all data is licensed under the creative commons "
                                                  "attribution license (https://creativecommons.org/licenses/by/4.0/). "
                                                  "This test also contains extensive metadata for browser testing. "
                                                  "Metadata is fabricated.",
                            "projectId": "627cb0ba-b8a1-405a-b58f-0add82c3d635",
                            "projectShortname": "staging/10x/2019-02-14T18:29:38Z",
                            "projectTitle": "10x 1 Run Integration Test",
                            "publications": [
                                {
                                    "publicationTitle": "A title of a publication goes here.",
                                    "publicationUrl": "https://europepmc.org"
                                }
                            ],
                            "supplementaryLinks": [None],
                            "matrices": {},
                            "contributorMatrices": {},
                            "submissionDate": "2019-02-14T18:29:42.531000Z",
                            "updateDate": "2019-02-14T18:29:48.555000Z",
                        }
                    ],
                    "protocols": [
                        {
                            "workflow": ['cellranger_v1.0.2'],
                            "submissionDate": "2019-02-14T19:15:10.720000Z",
                            "updateDate": "2019-02-14T19:15:15.466000Z",
                        },
                        {
                            "libraryConstructionApproach": ["10X v2 sequencing"],
                            "nucleicAcidSource": [None],
                            "submissionDate": "2019-02-14T18:29:42.625000Z",
                            "updateDate": "2019-02-14T18:29:48.684000Z",
                        },
                        {
                            "instrumentManufacturerModel": ["Illumina HiSeq 2500"],
                            "pairedEnd": [False],
                            "submissionDate": "2019-02-14T18:29:42.630000Z",
                            "updateDate": "2019-02-14T18:29:48.777000Z",
                        }
                    ],
                    "samples": [
                        {
                            "sampleEntityType": ["specimens"],
                            "effectiveOrgan": ["brain"],
                            "disease": ["H syndrome"],
                            "id": ["specimen_ID_1"],
                            "organ": ["brain"],
                            "organPart": ["amygdala"],
                            "preservationMethod": [None],
                            "source": [
                                "specimen_from_organism"
                            ],
                            "submissionDate": "2019-02-14T18:29:42.550000Z",
                            "updateDate": "2019-02-14T18:29:49.006000Z",
                        }
                    ],
                    "sources": [{
                        "sourceId": "4b737739-4dc9-5d4b-9989-a4942047c91c",
                        "sourceSpec": "test:/2"
                    }],
                    "specimens": [
                        {
                            "disease": ["H syndrome"],
                            "id": ["specimen_ID_1"],
                            "organ": ["brain"],
                            "organPart": ["amygdala"],
                            "preservationMethod": [None],
                            "source": [
                                "specimen_from_organism"
                            ],
                            "submissionDate": "2019-02-14T18:29:42.550000Z",
                            "updateDate": "2019-02-14T18:29:49.006000Z",
                        }
                    ]
                }
            ]
        }
        self.assertElasticsearchResultsEqual(keyword_response, expected_response)

    def test_cell_suspension_response(self):
        """
        Test KeywordSearchResponse contains the correct selectedCellType value
        """
        # FIXME: Use response from `/index/files` to validate
        #        https://github.com/DataBiosphere/azul/issues/2970
        keyword_response = KeywordSearchResponse(
            hits=self.get_hits('projects', '250aef61-a15b-4d97-b8b4-54bb997c1d7d'),
            entity_type='projects',
            catalog=self.catalog
        ).return_response().to_json()

        cell_suspension = one(keyword_response['hits'][0]['cellSuspensions'])
        self.assertEqual(["Plasma cells"], cell_suspension['selectedCellType'])

    def test_cell_line_response(self):
        """
        Test KeywordSearchResponse contains the correct cell_line and sample field values
        """
        # FIXME: Use response from `/index/files` to validate
        #        https://github.com/DataBiosphere/azul/issues/2970
        keyword_response = KeywordSearchResponse(
            hits=self.get_hits('projects', 'c765e3f9-7cfc-4501-8832-79e5f7abd321'),
            entity_type='projects',
            catalog=self.catalog
        ).return_response().to_json()
        expected_cell_lines = {
            'id': ['cell_line_Day7_hiPSC-CM_BioRep2', 'cell_line_GM18517'],
            'cellLineType': ['primary', 'stem cell-derived'],
            'modelOrgan': ['blood (parent_cell_line)', 'blood (child_cell_line)'],
            'submissionDate': '2018-12-04T16:22:45.467000Z',
            'updateDate': None
        }
        cell_lines = one(one(keyword_response['hits'])['cellLines'])
        self.assertElasticsearchResultsEqual(expected_cell_lines, cell_lines)
        expected_samples = {
            'sampleEntityType': ['cellLines'],
            'effectiveOrgan': ['blood (child_cell_line)'],
            'id': ['cell_line_Day7_hiPSC-CM_BioRep2'],
            'cellLineType': ['stem cell-derived'],
            'modelOrgan': ['blood (child_cell_line)'],
            'submissionDate': '2018-12-04T16:22:45.625000Z',
            'updateDate': None
        }
        samples = one(one(keyword_response['hits'])['samples'])
        self.assertElasticsearchResultsEqual(samples, expected_samples)

    def test_file_response(self):
        """
        Test KeywordSearchResponse contains the correct file field values
        """
        # FIXME: Use response from `/index/files` to validate
        #        https://github.com/DataBiosphere/azul/issues/2970
        keyword_response = KeywordSearchResponse(
            hits=self.get_hits('files', '4015da8b-18d8-4f3c-b2b0-54f0b77ae80a'),
            entity_type='files',
            catalog=self.catalog
        ).return_response().to_json()
        expected_file = {
            'contentDescription': ['RNA sequence'],
            'format': 'fastq.gz',
            'matrix_cell_count': None,
            'matrixCellCount': None,
            'isIntermediate': None,
            'name': 'Cortex2.CCJ15ANXX.SM2_052318p4_D8.unmapped.1.fastq.gz',
            'sha256': '709fede4736213f0f71ae4d76719fd51fa402a9112582a4c52983973cb7d7e47',
            'size': 22819025,
            'source': None,
            'fileSource': None,
            'submissionDate': '2019-10-09T15:31:58.607000Z',
            'updateDate': '2019-10-09T15:52:46.609000Z',
            'url': None,
            'uuid': 'a8b8479d-cfa9-4f74-909f-49552439e698',
            'version': '2019-10-09T172251.560099Z'
        }
        file = one(one(keyword_response['hits'])['files'])
        self.assertElasticsearchResultsEqual(file, expected_file)

    def test_filter_with_none(self):
        """
        Test response when using a filter with a None value
        """
        test_data_values = [["year"], [None], ["year", None]]
        for test_data in test_data_values:
            with self.subTest(test_data=test_data):
                params = self._params(size=10, filters={'organismAgeUnit': {'is': test_data}})
                url = self.base_url.set(path='/index/samples', args=params)
                response = requests.get(str(url))
                response.raise_for_status()
                response_json = response.json()
                organism_age_units = {
                    None if oa is None else oa['unit']
                    for hit in response_json['hits']
                    for donor in hit['donorOrganisms']
                    for oa in donor['organismAge']
                }
                # Assert that the organismAge unit values found in the response only match what was filtered for
                self.assertEqual(organism_age_units, set(test_data))

    def test_filter_by_projectId(self):
        """
        Test response when using a projectId filter
        """
        test_data_sets = [
            {
                'id': '627cb0ba-b8a1-405a-b58f-0add82c3d635',
                'title': '10x 1 Run Integration Test'
            },
            {
                'id': '250aef61-a15b-4d97-b8b4-54bb997c1d7d',
                'title': 'Bone marrow plasma cells from hip replacement surgeries'
            }
        ]
        for test_data in test_data_sets:
            for entity_type in 'files', 'samples', 'projects', 'bundles':
                with self.subTest(entity_type=entity_type):
                    params = self._params(size=2, filters={'projectId': {'is': [test_data['id']]}})
                    url = self.base_url.set(path=('index', entity_type), args=params)
                    response = requests.get(str(url))
                    response.raise_for_status()
                    response_json = response.json()
                    for hit in response_json['hits']:
                        for project in hit['projects']:
                            if entity_type == 'projects':
                                self.assertEqual(test_data['title'], project['projectTitle'])
                                self.assertEqual(test_data['id'], project['projectId'])
                            else:
                                self.assertIn(test_data['title'], project['projectTitle'])
                                self.assertIn(test_data['id'], project['projectId'])
                    for term in response_json['termFacets']['project']['terms']:
                        self.assertEqual(term['projectId'], [test_data['id']])

    def test_filter_by_contentDescription(self):
        url = self.base_url.set(path='/index/files')
        params = self._params(size=3,
                              filters={'contentDescription': {'is': ['RNA sequence']}},
                              sort='fileName',
                              order='asc')
        response = requests.get(str(url), params=params)
        response.raise_for_status()
        response_json = response.json()
        expected = [
            'Cortex2.CCJ15ANXX.SM2_052318p4_D8.unmapped.1.fastq.gz',
            'Cortex2.CCJ15ANXX.SM2_052318p4_D8.unmapped.2.fastq.gz'
        ]
        actual = [file['name']
                  for hit in response_json['hits']
                  for file in hit['files']]
        self.assertEqual(actual, expected)

    def test_translated_facets(self):
        """
        Test that response facets values are correctly translated back to the
        correct data types and that the translated None value is not present.
        """
        url = self.base_url.set(path='/index/samples',
                                args=(self._params(size=10)))
        response = requests.get(str(url))
        response.raise_for_status()
        response_json = response.json()
        facets = response_json['termFacets']

        paired_end_terms = {term['term'] for term in facets['pairedEnd']['terms']}
        self.assertEqual(paired_end_terms, {'true', 'false'})

        preservation_method_terms = {term['term'] for term in facets['preservationMethod']['terms']}
        self.assertEqual(preservation_method_terms, {None})

        model_organ_part_terms = {term['term'] for term in facets['modelOrganPart']['terms']}
        self.assertEqual(model_organ_part_terms, {None})

        for facet in facets.values():
            for term in facet['terms']:
                self.assertNotEqual(term['term'], null_str.to_index(None))

    def test_sample(self):
        """
        Test that sample(s) in the response contain values matching values in the source cellLine/organoid/specimen
        """
        for entity_type in 'projects', 'samples', 'files', 'bundles':
            with self.subTest(entity_type=entity_type):
                url = self.base_url.set(path=('index', entity_type), args=self._params())
                response = requests.get(str(url))
                response.raise_for_status()
                response_json = response.json()
                if entity_type == 'samples':
                    for hit in response_json['hits']:
                        for sample in hit['samples']:
                            sample_entity_type = sample['sampleEntityType']
                            for key, val in sample.items():
                                if key not in [
                                    'sampleEntityType',
                                    'effectiveOrgan',
                                    'accessible',
                                    'submissionDate',
                                    'updateDate'
                                ]:
                                    if isinstance(val, list):
                                        for one_val in val:
                                            self.assertIn(one_val, hit[sample_entity_type][0][key])
                                    else:
                                        self.assertIn(val, hit[sample_entity_type][0][key])

    def test_bundles_outer_entity(self):
        entity_type = 'bundles'
        url = self.base_url.set(path=('index', entity_type), args=self._params())
        response = requests.get(str(url))
        response.raise_for_status()
        response = response.json()
        indexed_bundles = set(self.bundles())
        self.assertEqual(len(self.bundles()), len(indexed_bundles))
        actual_bundles = {
            self.bundle_fqid(uuid=bundle['bundleUuid'],
                             version=bundle['bundleVersion'])
            for hit in response['hits']
            for bundle in hit['bundles']
        }
        self.assertEqual(len(response['hits']), len(actual_bundles))
        self.assertSetEqual(indexed_bundles, actual_bundles)

    def test_ranged_values(self):
        test_hits = [
            [
                {
                    "biologicalSex": [
                        "male",
                        "female"
                    ],
                    "developmentStage": [None],
                    "disease": ['normal'],
                    "genusSpecies": [
                        "Homo sapiens"
                    ],
                    "id": [
                        "HPSI0314i-hoik",
                        "HPSI0214i-wibj",
                        "HPSI0314i-sojd",
                        "HPSI0214i-kucg"
                    ],
                    "donorCount": 4,
                    "organismAge": [
                        {"value": "45-49", "unit": "year"},
                        {"value": "65-69", "unit": "year"}
                    ],
                    "organismAgeRange": [
                        {
                            "gte": 2049840000.0,
                            "lte": 2175984000.0
                        },
                        {
                            "gte": 1419120000.0,
                            "lte": 1545264000.0
                        }
                    ],
                    "submissionDate": "2018-10-11T21:18:02.456000Z",
                    "updateDate": "2018-10-11T21:18:06.768000Z",
                }
            ],
            [
                {
                    "biologicalSex": [
                        "male",
                        "female"
                    ],
                    "developmentStage": [None],
                    "disease": ['normal'],
                    "genusSpecies": [
                        "Homo sapiens"
                    ],
                    "id": [
                        "HPSI0314i-hoik",
                        "HPSI0214i-wibj",
                        "HPSI0314i-sojd",
                        "HPSI0214i-kucg"
                    ],
                    "donorCount": 4,
                    "organismAge": [
                        {"value": "40-44", "unit": "year"},
                        {"value": "55-59", "unit": "year"}
                    ],
                    "organismAgeRange": [
                        {
                            "gte": 1734480000.0,
                            "lte": 1860624000.0
                        },
                        {
                            "gte": 1261440000.0,
                            "lte": 1387584000.0
                        }
                    ],
                    "submissionDate": "2018-10-11T21:18:02.456000Z",
                    "updateDate": "2018-10-11T21:18:06.768000Z",
                }
            ]
        ]

        for relation, range_value, expected_hits in [('contains', (1419130000, 1545263000), test_hits[:1]),
                                                     ('within', (1261430000, 1545265000), test_hits),
                                                     ('intersects', (1860623000, 1900000000), test_hits[1:]),
                                                     ('contains', (1860624000, 2049641000), []),
                                                     ('within', (1734490000, 1860623000), []),
                                                     ('intersects', (1860624100, 2049641000), [])]:
            with self.subTest(relation=relation, value=range_value):
                params = self._params(filters={'organismAgeRange': {relation: [range_value]}},
                                      order='desc',
                                      sort='entryId')
                url = self.base_url.set(path='/index/projects', args=params)
                response = requests.get(str(url))
                actual_value = [hit['donorOrganisms'] for hit in response.json()['hits']]
                self.assertElasticsearchResultsEqual(expected_hits, actual_value)

    def test_ordering(self):
        sort_fields = [
            ('cellCount', lambda hit: hit['cellSuspensions'][0]['totalCells']),
            ('donorCount', lambda hit: hit['donorOrganisms'][0]['donorCount'])
        ]

        for sort_field, accessor in sort_fields:
            responses = {
                order: requests.get(str(self.base_url.set(path='/index/projects',
                                                          args=self._params(order=order, sort=sort_field))))
                for order in ['asc', 'desc']
            }
            hit_sort_values = {}
            for order, response in responses.items():
                response.raise_for_status()
                hit_sort_values[order] = [accessor(hit) for hit in response.json()['hits']]

            self.assertEqual(hit_sort_values['asc'],
                             sorted(hit_sort_values['asc'], key=none_safe_key()))
            self.assertEqual(hit_sort_values['desc'],
                             sorted(hit_sort_values['desc'], key=none_safe_key(), reverse=True))

    def test_missing_field_sorting(self):
        """
        Test that sorting by a field that doesn't exist in all hits produces
        results with the hits missing the field placed at the end of a
        ascending sort and the beginning of a descending sort.
        """
        ascending_values = [
            ['induced pluripotent'],
            ['induced pluripotent'],
            ['primary', 'stem cell-derived'],
            None,  # The last 4 hits don't have any 'cellLines' inner entities
            None,  # so for purposes of this test we use None to represent
            None,  # that there is a hit however it has no 'cellLineType'.
            None
        ]

        def extract_cell_line_types(response_json):
            # For each hit yield the 'cellLineType' value or None if not present
            for hit in response_json['hits']:
                if hit['cellLines']:
                    yield one(hit['cellLines'])['cellLineType']
                else:
                    yield None

        for ascending in (True, False):
            with self.subTest(ascending=ascending):
                params = self._params(size=15,
                                      sort='cellLineType',
                                      order='asc' if ascending else 'desc')
                url = self.base_url.set(path='/index/projects', args=params)
                response = requests.get(str(url))
                response.raise_for_status()
                response_json = response.json()
                actual_values = list(extract_cell_line_types(response_json))
                expected = ascending_values if ascending else list(reversed(ascending_values))
                self.assertEqual(actual_values, expected)

    def test_multivalued_field_sorting(self):
        """
        Test that sorting by a multi-valued field responds with hits that are
        correctly sorted based on the first value from each multi-valued field, and
        that each multi-valued field itself is sorted low to high regardless of the search sort
        """
        for order, reverse in (('asc', False), ('desc', True)):
            with self.subTest(order=order, reverse=reverse):
                params = self._params(size=15, sort='laboratory', order=order)
                url = self.base_url.set(path='/index/projects', args=params)
                response = requests.get(str(url))
                response.raise_for_status()
                response_json = response.json()
                laboratories = []
                for hit in response_json['hits']:
                    laboratory = one(hit['projects'])['laboratory']
                    self.assertEqual(laboratory, sorted(laboratory))
                    laboratories.append(laboratory[0])
                self.assertGreater(len(laboratories), 1)
                self.assertEqual(laboratories, sorted(laboratories, reverse=reverse))

    def test_disease_facet(self):
        """
        Verify the values of the different types of disease facets
        """
        test_data = {
            # disease specified in donor, specimen, and sample (the specimen)
            '627cb0ba-b8a1-405a-b58f-0add82c3d635': {
                'sampleDisease': [{'term': 'H syndrome', 'count': 1}],
                'donorDisease': [{'term': 'H syndrome', 'count': 1}],
                'specimenDisease': [{'term': 'H syndrome', 'count': 1}],
            },
            # disease specified in donor only
            '250aef61-a15b-4d97-b8b4-54bb997c1d7d': {
                'sampleDisease': [{'term': None, 'count': 1}],
                'donorDisease': [{'term': 'isolated hip osteoarthritis', 'count': 1}],
                'specimenDisease': [{'term': None, 'count': 1}],
            },
            # disease specified in donor and specimen, not in sample (the cell line)
            'c765e3f9-7cfc-4501-8832-79e5f7abd321': {
                'sampleDisease': [{'term': None, 'count': 1}],
                'donorDisease': [{'term': 'normal', 'count': 1}],
                'specimenDisease': [{'term': 'normal', 'count': 1}]
            }
        }
        self._assert_term_facets(test_data,
                                 str(self.base_url.set(path='/index/projects')))

    def test_contentDescription_facet(self):
        url = self.base_url.set(path='/index/projects')
        test_data = {
            '88ec040b-8705-4f77-8f41-f81e57632f7d': {
                'contentDescription': [{'term': 'RNA sequence', 'count': 1}]
            }
        }
        self._assert_term_facets(test_data, str(url))

    def _assert_term_facets(self, project_term_facets: JSON, url: str) -> None:
        for project_id, term_facets in project_term_facets.items():
            with self.subTest(project_id=project_id):
                params = self._params(filters={'projectId': {'is': [project_id]}})
                response = requests.get(url, params=params)
                response.raise_for_status()
                response_json = response.json()
                actual_term_facets = response_json['termFacets']
                for facet, terms in term_facets.items():
                    self.assertEqual(actual_term_facets[facet]['terms'], terms)

    def test_organism_age_facet(self):
        """
        Verify the terms of the organism age facet
        """
        test_data = {
            # This project has one donor organism
            '627cb0ba-b8a1-405a-b58f-0add82c3d635': {
                'organismAge': [
                    {
                        'term': {
                            'value': '20',
                            'unit': 'year'
                        },
                        'count': 1
                    }
                ],
                'organismAgeUnit': [
                    {
                        'term': 'year',
                        'count': 1
                    }
                ],
                'organismAgeValue': [
                    {
                        'term': '20',
                        'count': 1
                    }
                ],

            },
            # This project has multiple donor organisms
            '2c4724a4-7252-409e-b008-ff5c127c7e89': {
                'organismAge': [
                    {
                        'term': {
                            'value': '40-44',
                            'unit': 'year'
                        },
                        'count': 1
                    },
                    {
                        'term': {
                            'value': '55-59',
                            'unit': 'year'
                        },
                        'count': 1
                    }
                ],
                'organismAgeUnit': [
                    {
                        'term': 'year',
                        'count': 1
                    }
                ],
                'organismAgeValue': [
                    {
                        'term': '40-44',
                        'count': 1
                    },
                    {
                        'term': '55-59',
                        'count': 1
                    }
                ]
            },
            # This project has one donor but donor has no age
            'c765e3f9-7cfc-4501-8832-79e5f7abd321': {
                'organismAge': [
                    {
                        'term': None,
                        'count': 1
                    }
                ],
                'organismAgeUnit': [
                    {
                        'term': None,
                        'count': 1
                    }
                ],
                'organismAgeValue': [
                    {
                        'term': None,
                        'count': 1
                    }
                ],
            }
        }
        self._assert_term_facets(test_data,
                                 str(self.base_url.set(path='/index/projects')))

    def test_organism_age_facet_search(self):
        """
        Verify filtering by organism age
        """
        test_cases = [
            (
                '627cb0ba-b8a1-405a-b58f-0add82c3d635',
                {
                    'is': [
                        {
                            'value': '20',
                            'unit': 'year'
                        }
                    ]
                }
            ),
            (
                'c765e3f9-7cfc-4501-8832-79e5f7abd321',
                {
                    'is': [
                        None
                    ]
                }
            ),
            (
                None,
                {
                    'is': [
                        {}
                    ]
                }
            ),
            (
                None,
                {
                    'is': [
                        {
                            'value': None,
                            'unit': 'weeks'
                        }
                    ]
                }
            )
        ]
        for project_id, filters in test_cases:
            with self.subTest(filters=filters):
                url = self.base_url.set(path='/index/projects',
                                        args=dict(catalog=self.catalog,
                                                  filters=json.dumps({'organismAge': filters})))
                response = requests.get(str(url))
                if project_id is None:
                    self.assertTrue(response.status_code, 400)
                else:
                    response.raise_for_status()
                    response = response.json()
                    hit = one(response['hits'])
                    self.assertEqual(hit['entryId'], project_id)
                    donor_organism = one(hit['donorOrganisms'])
                    age = one(one(filters.values()))
                    self.assertEqual(donor_organism['organismAge'],
                                     [None if age is None else age])

    def test_pagination_search_after_search_before(self):
        """
        Test search_after and search_before values when using sorting on a field containing None values
        """
        params = self._params(size=3, sort='workflow', order='asc')
        url = self.base_url.set(path='/index/samples', args=params)
        response = requests.get(str(url))
        response.raise_for_status()
        response_json = response.json()
        first_page_next = parse_url_qs(response_json['pagination']['next'])

        expected_entry_ids = [
            '58c60e15-e07c-4875-ac34-f026d6912f1c',
            '195b2621-ec05-4618-9063-c56048de97d1',
            '2d8282f0-6cbb-4d5a-822c-4b01718b4d0d',
        ]
        self.assertEqual(expected_entry_ids, [h['entryId'] for h in response_json['hits']])

        # NOTE: The sort field `workflow` is an `analysis_protocol` field and
        # does not exist in all bundles. This is why the `search_after` field
        # has the value `null` (JSON representation of `None`) because the last
        # row in this page of results does not have an `analysis_protocol` or
        # `workflow` field. If the last row did have a `workflow` field with a
        # value `None`, `search_after` would be a translated `None` (`"~null"`)
        self.assertIsNotNone(response_json['pagination']['next'])
        self.assertIsNone(response_json['pagination']['previous'])
        self.assertEqual(first_page_next['search_after'], 'null')
        self.assertEqual(first_page_next['search_after_uid'], 'doc#2d8282f0-6cbb-4d5a-822c-4b01718b4d0d')

        response = requests.get(response_json['pagination']['next'])
        response.raise_for_status()
        response_json = response.json()
        second_page_next = parse_url_qs(response_json['pagination']['next'])
        second_page_previous = parse_url_qs(response_json['pagination']['previous'])

        expected_entry_ids = [
            '308eea51-d14b-4036-8cd1-cfd81d7532c3',
            '73f10dad-afc5-4d1d-a71c-4a8b6fff9172',
            '79682426-b813-4f69-8c9c-2764ffac5dc1',
        ]
        self.assertEqual(expected_entry_ids, [h['entryId'] for h in response_json['hits']])

        self.assertEqual(second_page_next['search_after'], 'null')
        self.assertEqual(second_page_next['search_after_uid'], 'doc#79682426-b813-4f69-8c9c-2764ffac5dc1')
        self.assertEqual(second_page_previous['search_before'], 'null')
        self.assertEqual(second_page_previous['search_before_uid'], 'doc#308eea51-d14b-4036-8cd1-cfd81d7532c3')

    def test_filter_by_publication_title(self):
        cases = [
            (
                'Systematic comparative analysis of single cell RNA-sequencing methods',
                {
                    'a8b8479d-cfa9-4f74-909f-49552439e698',
                    '7338932d-edc9-49a9-8dbf-e459a465800f'
                }

            ),
            (
                'Palantir characterizes cell fate continuities in human hematopoiesis',
                set()
            )
        ]
        for title, expected_files in cases:
            with self.subTest(title=title):
                filters = {
                    'publicationTitle': {
                        'is': [title]
                    }
                }
                expected_terms = {
                    'terms': [
                        {
                            'term': None,
                            'count': 25
                        },
                        {
                            'term': 'A title of a publication goes here.',
                            'count': 16
                        },
                        {
                            'term': 'Single-Cell Analysis of Human Pancreas Reveals Transcriptional '
                                    'Signatures of Aging and Somatic Mutation Patterns.',
                            'count': 2
                        },
                        {
                            'term': 'Systematic comparative analysis of single cell RNA-sequencing methods',
                            'count': 2
                        }
                    ],
                    'total': 45,
                    'type': 'terms'
                }
                url = self.base_url.set(path='/index/files',
                                        args=dict(filters=json.dumps(filters)))
                response = requests.get(str(url))
                self.assertEqual(200, response.status_code)
                self.assertEqual(expected_terms,
                                 response.json()['termFacets']['publicationTitle'])
                files = {
                    one(hit['files'])['uuid']
                    for hit in response.json()['hits']
                }
                self.assertEqual(expected_files, files)

    def test_access(self):
        filtered_entity_types = {
            'bundles': True,
            'files': True,
            'samples': True,
            'projects': False
        }

        def _test(entity_type: str, expect_empty: bool, expect_accessible: bool):
            with self.subTest(entity_type=entity_type, access=expect_accessible):
                url = str(self.base_url.set(path=('index', entity_type)))
                response = requests.get(url)
                self.assertEqual(200, response.status_code)
                hits = response.json()['hits']
                if expect_empty:
                    self.assertEqual([], hits)
                else:
                    self.assertGreater(len(hits), 0)
                    for hit in hits:
                        entity = one(hit[entity_type])
                        self.assertEqual(expect_accessible, entity['accessible'])

        for entity_type in filtered_entity_types:
            _test(entity_type, expect_empty=False, expect_accessible=True)

        with mock.patch('azul.plugins.repository.dss.Plugin.sources', return_value=[]):
            for entity_type, is_filtered in filtered_entity_types.items():
                _test(entity_type, expect_empty=is_filtered, expect_accessible=False)


@patch_dss_endpoint
@patch_source_cache
class TestFileTypeSummaries(WebServiceTestCase):

    @classmethod
    def bundles(cls) -> List[BundleFQID]:
        return super().bundles() + [
            cls.bundle_fqid(uuid='fce68057-b0f0-5d11-b9a7-30e8fa3259a8',
                            version='2021-02-09T01:30:00.000000Z'),
        ]

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    def test_grouping(self):
        url = self.base_url.set(path='/index/projects')
        filters = {'projectId': {'is': ['f2fe82f0-4454-4d84-b416-a885f3121e59']}}
        params = {
            'catalog': self.catalog,
            'filters': json.dumps(filters)
        }
        response = requests.get(str(url), params=params)
        response.raise_for_status()
        response_json = response.json()
        file_type_summaries = one(response_json['hits'])['fileTypeSummaries']
        expected = [
            {
                'fileType': 'fastq.gz',
                'format': 'fastq.gz',
                'count': 117,
                'totalSize': 1670420872710,
                'matrixCellCount': None,
                'isIntermediate': None,
                'contentDescription': ['DNA sequence'],
                'source': [None],
                'fileSource': [None],
            },
            {
                'fileType': 'fastq.gz',
                'format': 'fastq.gz',
                'count': 3,
                'totalSize': 128307505318,
                'matrixCellCount': None,
                'isIntermediate': None,
                'contentDescription': ['Cellular Genetics'],
                'source': [None],
                'fileSource': [None],
            },
            {
                'fileType': 'loom',
                'format': 'loom',
                'count': 40,
                'totalSize': 59207580244,
                'matrixCellCount': None,
                'isIntermediate': True,
                'contentDescription': ['Count Matrix'],
                'source': ['DCP/2 Analysis'],
                'fileSource': ['DCP/2 Analysis'],
            },
            {
                'fileType': 'loom',
                'format': 'loom',
                'count': 1,
                'totalSize': 5389602923,
                'matrixCellCount': None,
                'isIntermediate': False,
                'contentDescription': ['Count Matrix'],
                'source': ['DCP/2 Analysis'],
                'fileSource': ['DCP/2 Analysis'],
            },
            {
                'fileType': 'bam',
                'format': 'bam',
                'count': 40,
                'totalSize': 1659270110045,
                'matrixCellCount': None,
                'isIntermediate': None,
                'contentDescription': [None],
                'source': ['DCP/2 Analysis'],
                'fileSource': ['DCP/2 Analysis'],
            },
        ]
        self.assertElasticsearchResultsEqual(file_type_summaries, expected)


@patch_dss_endpoint
@patch_source_cache
class TestResponseInnerEntitySamples(WebServiceTestCase):
    maxDiff = None

    @classmethod
    def bundles(cls) -> List[BundleFQID]:
        return super().bundles() + [
            # A bundle with 1 specimen and 1 cell line sample entities
            cls.bundle_fqid(uuid='1b6d8348-d6e9-406a-aa6a-7ee886e52bf9',
                            version='2019-10-03T105524.911627Z'),
            # A bundle with 4 organoid sample entities
            cls.bundle_fqid(uuid='411cd8d5-5990-43cd-84cc-6c7796b8a76d',
                            version='2018-10-18T204655.866661Z'),
        ]

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    def test_inner_entity_samples(self):
        """
        Verify aggregated 'samples' inner entities are grouped by sample entity
        type and that results can be filtered by sampleEntityType.
        """
        # Note that this test is against the /index/projects endpoint, so when
        # a filter such as {'sampleEntityType':{'is':['cell_lines']}} is used
        # and finds a project with both cell line and specimen samples, the
        # response hit 'samples' inner entity (an aggregate grouped by entity
        # type) will contain both samples.
        expected_filter_hits = {
            'cell_lines': [
                # One hit for a project with 2 different type samples
                [
                    {
                        'sampleEntityType': ['cellLines'],
                        'effectiveOrgan': ['immune system'],
                        'id': ['Cell_line_2'],
                        'cellLineType': ['primary'],
                        'modelOrgan': ['immune system'],
                        'submissionDate': '2019-09-20T13:43:45.344000Z',
                        'updateDate': '2019-09-20T13:43:52.455000Z',
                    },
                    {
                        'sampleEntityType': ['specimens'],
                        'effectiveOrgan': ['embryo'],
                        'id': ['Specimen1'],
                        'organ': ['embryo'],
                        'organPart': ['skin epidermis'],
                        'disease': ['normal'],
                        'preservationMethod': [None],
                        'source': ['specimen_from_organism'],
                        'submissionDate': '2019-09-20T13:43:45.329000Z',
                        'updateDate': '2019-09-20T13:43:52.654000Z',
                    },
                ]
            ],
            'organoids': [
                # One hit for a project with 4 samples of the same type
                [
                    {
                        'sampleEntityType': ['organoids'],
                        'effectiveOrgan': ['Brain'],
                        'id': [
                            'Org_HPSI0214i-kucg_2_2',
                            'Org_HPSI0214i-wibj_2_2',
                            'Org_HPSI0314i-hoik_1_2',
                            'Org_HPSI0314i-sojd_3_2',
                        ],
                        'modelOrgan': ['Brain'],
                        'modelOrganPart': [None],
                        'submissionDate': '2018-10-11T21:18:02.654000Z',
                        'updateDate': '2018-10-11T21:18:12.864000Z',
                    }
                ]
            ],
            'specimens': [
                # Two hits, one for a project with 2 different type samples and
                # one for a project with 1 sample
                [
                    {
                        'sampleEntityType': ['cellLines'],
                        'effectiveOrgan': ['immune system'],
                        'id': ['Cell_line_2'],
                        'cellLineType': ['primary'],
                        'modelOrgan': ['immune system'],
                        'submissionDate': '2019-09-20T13:43:45.344000Z',
                        'updateDate': '2019-09-20T13:43:52.455000Z',
                    },
                    {
                        'sampleEntityType': ['specimens'],
                        'effectiveOrgan': ['embryo'],
                        'id': ['Specimen1'],
                        'organ': ['embryo'],
                        'organPart': ['skin epidermis'],
                        'disease': ['normal'],
                        'preservationMethod': [None],
                        'source': ['specimen_from_organism'],
                        'submissionDate': '2019-09-20T13:43:45.329000Z',
                        'updateDate': '2019-09-20T13:43:52.654000Z',
                    },
                ],
                [
                    {
                        'sampleEntityType': ['specimens'],
                        'effectiveOrgan': ['pancreas'],
                        'id': ['DID_scRSq06_pancreas'],
                        'organ': ['pancreas'],
                        'organPart': ['islet of Langerhans'],
                        'disease': ['normal'],
                        'preservationMethod': [None],
                        'source': ['specimen_from_organism'],
                        'submissionDate': '2018-11-02T10:02:12.298000Z',
                        'updateDate': '2018-11-02T10:09:26.517000Z',
                    }
                ],
            ],
        }
        for entity_type, expected_hits in expected_filter_hits.items():
            with self.subTest(entity_type=entity_type):
                params = {
                    'filters': json.dumps({'sampleEntityType': {'is': [entity_type]}}),
                    'catalog': self.catalog,
                    'size': 5,
                    'sort': 'projectTitle',
                    'order': 'asc',
                }
                url = self.base_url.set(path='/index/projects', args=params)
                response = requests.get(str(url))
                response.raise_for_status()
                response_json = response.json()
                hits = response_json['hits']
                self.assertEqual(expected_hits, [hit['samples'] for hit in hits])


@patch_dss_endpoint
@patch_source_cache
class TestSortAndFilterByCellCount(WebServiceTestCase):
    maxDiff = None

    @classmethod
    def bundles(cls) -> List[BundleFQID]:
        return super().bundles() + [
            # 2 bundles from 1 project with 7738 total cells across 2 cell suspensions
            cls.bundle_fqid(uuid='97f0cc83-f0ac-417a-8a29-221c77debde8',
                            version='2019-10-14T195415.397406Z'),
            cls.bundle_fqid(uuid='8c90d4fe-9a5d-4e3d-ada2-0414b666b880',
                            version='2019-10-14T195415.397546Z'),
            # other bundles
            cls.bundle_fqid(uuid='fa5be5eb-2d64-49f5-8ed8-bd627ac9bc7a',
                            version='2019-02-14T192438.034764Z'),
            cls.bundle_fqid(uuid='411cd8d5-5990-43cd-84cc-6c7796b8a76d',
                            version='2018-10-18T204655.866661Z'),
            cls.bundle_fqid(uuid='ffac201f-4b1c-4455-bd58-19c1a9e863b4',
                            version='2019-10-09T170735.528600Z'),
        ]

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    def _count_total_cells(self, response_json):
        """
        Return the number of cell suspension inner entities and total cell count
        per hit.
        """
        return [
            (
                len(hit['cellSuspensions']),
                sum([cs['totalCells'] for cs in hit['cellSuspensions']])
            )
            for hit in response_json['hits']
        ]

    def test_sorting_by_cell_count(self):
        """
        Verify sorting by 'cellCount' sorts the documents based on the total
        number of cells in each document, using the sum of total cells when a
        document contains more than one cell suspension inner entity.
        """
        ascending_results = [
            (1, 1),
            (1, 349),
            (1, 6210),
            (2, 7738),
            (1, 10000)
        ]
        for ascending in (True, False):
            with self.subTest(ascending=ascending):
                params = {
                    'catalog': self.catalog,
                    'sort': 'cellCount',
                    'order': 'asc' if ascending else 'desc'
                }
                url = self.base_url.set(path='/index/projects', args=params)
                response = requests.get(str(url))
                response.raise_for_status()
                response_json = response.json()
                actual_results = self._count_total_cells(response_json)
                expected = ascending_results if ascending else list(reversed(ascending_results))
                self.assertEqual(actual_results, expected)

    def test_filter_by_cell_count(self):
        """
        Verify filtering by 'cellCount' filters the documents based on the total
        number of cells in each document, using the sum of total cells when a
        document contains more than one cell suspension inner entity.
        """
        params = {
            'catalog': self.catalog,
            'filters': json.dumps({
                'cellCount': {
                    'within': [
                        [
                            6000,
                            9000
                        ]
                    ]
                }
            })
        }
        url = self.base_url.set(path='/index/projects', args=params)
        response = requests.get(str(url))
        response.raise_for_status()
        response_json = response.json()
        actual_results = self._count_total_cells(response_json)
        expected_results = [
            (1, 6210),
            (2, 7738)
        ]
        self.assertEqual(actual_results, expected_results)


@patch_dss_endpoint
@patch_source_cache
class TestProjectMatrices(WebServiceTestCase):
    maxDiff = None

    @classmethod
    def bundles(cls) -> List[BundleFQID]:
        return super().bundles() + [
            # A hacky CGM subgraph (project 8185730f)
            # 8 supplementary file CGMs each with a 'submitter_id'
            cls.bundle_fqid(uuid='4b03c1ce-9df1-5cd5-a8e4-48a2fe095081',
                            version='2021-02-10T16:56:40.419579Z'),
            # A hacky DCP/1 matrix service subgraph (project 8185730f)
            # 3 supplementary file matrices each with a 'submitter_id'
            cls.bundle_fqid(uuid='8338b891-f3fa-5e7b-885f-e4ee5689ee15',
                            version='2020-12-03T10:39:17.144517Z'),
            # An intermediate DCP/2 analysis subgraph (project 8185730f)
            # 1 intermediate analysis file matrix
            cls.bundle_fqid(uuid='7eb74d9f-8346-5420-b7e4-b486f99451a8',
                            version='2020-02-03T10:30:00Z'),
            # A top-level DCP/2 analysis subgraph (project 8185730f)
            # 1 analysis file matrix with a 'submitter_id'
            cls.bundle_fqid(uuid='00f48893-5e9d-52cd-b32d-af88edccabfa',
                            version='2020-02-03T10:30:00Z'),
            # An organic CGM subgraph (project bd400331)
            # 2 analysis file CGMs each with a 'file_source'
            cls.bundle_fqid(uuid='04836733-0449-4e57-be2e-6f3b8fbdfb12',
                            version='2021-05-10T23:25:12.412Z')
        ]

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    def params(self,
               project_id: str,
               facet: Optional[str] = None,
               value: Optional[str] = None) -> JSON:
        return {
            'filters': json.dumps(
                {
                    'projectId': {'is': [project_id]},
                    **({facet: {'is': [value]}} if facet else {})
                }
            ),
            'catalog': self.catalog,
            'size': 500
        }

    def test_file_source_facet(self):
        """
        Verify the 'fileSource' facet is populated with the human-readable
        versions of the name used to generate the 'submitter_id' UUID, and that
        the facet values match the hits[].files[].fileSource values.
        """
        params = self.params(project_id='8185730f-4113-40d3-9cc3-929271784c2b')
        url = self.base_url.set(path='/index/files', args=params)
        response = requests.get(str(url))
        response.raise_for_status()
        response_json = response.json()
        facets = response_json['termFacets']
        expected_counts = {
            'DCP/2 Analysis': 3,
            'DCP/1 Matrix Service': 3,
            'HCA Release': 1,
            'ArrayExpress': 7
        }
        expected_facets = [
            {'term': key, 'count': val}
            for key, val in expected_counts.items()
        ]
        self.assertElasticsearchResultsEqual(expected_facets,
                                             facets['fileSource']['terms'])
        actual_counts = Counter()
        for hit in response_json['hits']:
            file = one(hit['files'])
            # FIXME: Remove deprecated field 'source'
            #        https://github.com/DataBiosphere/azul/issues/3180
            self.assertEqual(file['source'], file['fileSource'])
            actual_counts[file['fileSource']] += 1
        self.assertEqual(expected_counts, actual_counts)

    def test_is_intermediate_facet(self):
        """
        Verify the 'isIntermediate' facet.
        """
        params = self.params(project_id='8185730f-4113-40d3-9cc3-929271784c2b')
        url = self.base_url.set(path='/index/files', args=params)
        response = requests.get(str(url))
        response.raise_for_status()
        response_json = response.json()
        facets = response_json['termFacets']
        expected = [
            {'term': None, 'count': 1},
            {'term': 'false', 'count': 12},
            {'term': 'true', 'count': 1}
        ]
        self.assertElasticsearchResultsEqual(expected, facets['isIntermediate']['terms'])

    def test_contributor_matrix_files(self):
        """
        Verify the files endpoint returns all the files from both the analysis
        and CGM bundles, and that supplementary file matrices can be found by
        their stratification values.
        """
        expected = {
            (None, None): [
                # 8 supplementary files from bundle 4b03c1ce
                'E-MTAB-7316.processed.1.zip',
                'E-MTAB-7316.processed.2.zip',
                'E-MTAB-7316.processed.3.zip',
                'E-MTAB-7316.processed.4.zip',
                'E-MTAB-7316.processed.5.zip',
                'E-MTAB-7316.processed.6.zip',
                'E-MTAB-7316.processed.7.zip',
                'WongRetinaCelltype.csv',
                # 3 supplementary files from bundle 8338b891
                '8185730f-4113-40d3-9cc3-929271784c2b.homo_sapiens.csv.zip',
                '8185730f-4113-40d3-9cc3-929271784c2b.homo_sapiens.loom',
                '8185730f-4113-40d3-9cc3-929271784c2b.homo_sapiens.mtx.zip',
                # 2 analysis files from bundle 7eb74d9f
                '1116b396-448e-4dd1-b9c9-78357c511e15.bam',
                '1116b396-448e-4dd1-b9c9-78357c511e15.loom',
                # 1 analysis file from bundle 00f48893
                'wong-retina-human-eye-10XV2.loom'
            ],
            ('developmentStage', 'adult'): [
                'E-MTAB-7316.processed.1.zip',
                'E-MTAB-7316.processed.2.zip',
                'E-MTAB-7316.processed.3.zip',
                'E-MTAB-7316.processed.4.zip',
                'E-MTAB-7316.processed.5.zip',
                'E-MTAB-7316.processed.6.zip',
                'E-MTAB-7316.processed.7.zip',
                'WongRetinaCelltype.csv',
            ],
            ('developmentStage', 'human adult stage'): [
                '1116b396-448e-4dd1-b9c9-78357c511e15.bam',
                '1116b396-448e-4dd1-b9c9-78357c511e15.loom',
                'wong-retina-human-eye-10XV2.loom'
            ],
            ('developmentStage', None): [
                '8185730f-4113-40d3-9cc3-929271784c2b.homo_sapiens.csv.zip',
                '8185730f-4113-40d3-9cc3-929271784c2b.homo_sapiens.loom',
                '8185730f-4113-40d3-9cc3-929271784c2b.homo_sapiens.mtx.zip'
            ],
        }
        for (facet, value), expected_files in expected.items():
            with self.subTest(facet=facet, value=value):
                params = self.params(project_id='8185730f-4113-40d3-9cc3-929271784c2b',
                                     facet=facet,
                                     value=value)
                url = self.base_url.set(path='/index/files', args=params)
                response = requests.get(str(url))
                response.raise_for_status()
                response_json = response.json()
                actual_files = [one(hit['files'])['name'] for hit in response_json['hits']]
                self.assertEqual(sorted(expected_files), sorted(actual_files))

    def test_matrices_tree(self):
        """
        Verify the projects endpoint includes a valid 'matrices' and
        'contributorMatrices' tree inside the projects inner-entity.
        """
        params = self.params(project_id='8185730f-4113-40d3-9cc3-929271784c2b')
        url = self.base_url.set(path='/index/projects', args=params)
        response = requests.get(str(url))
        response.raise_for_status()
        response_json = response.json()
        hit = one(response_json['hits'])
        self.assertEqual('8185730f-4113-40d3-9cc3-929271784c2b', hit['entryId'])
        matrices = {
            'genusSpecies': {
                'Homo sapiens': {
                    'developmentStage': {
                        'human adult stage': {
                            'organ': {
                                'eye': {
                                    'libraryConstructionApproach': {
                                        '10X v2 sequencing': [
                                            {
                                                # Analysis file, source from submitter_id
                                                'name': 'wong-retina-human-eye-10XV2.loom',
                                                'size': 255471211,
                                                'source': 'DCP/2 Analysis',
                                                'fileSource': 'DCP/2 Analysis',
                                                'matrix_cell_count': None,
                                                'matrixCellCount': None,
                                                'uuid': 'bd98f428-881e-501a-ac16-24f27a68ce2f',
                                                'version': '2021-02-11T23:11:45.000000Z',
                                                'url': str(self.base_url.set(
                                                    path='/repository/files/bd98f428-881e-501a-ac16-24f27a68ce2f',
                                                    args=dict(catalog='test', version='2021-02-11T23:11:45.000000Z')
                                                ))
                                            }
                                        ]
                                    }
                                }
                            }
                        },
                        'Unspecified': {
                            'organ': {
                                'eye': {
                                    'libraryConstructionApproach': {
                                        '10X v2 sequencing': [
                                            {
                                                # Supplementary file, source from submitter_id
                                                'name': '8185730f-4113-40d3-9cc3-929271784c2b.homo_sapiens.csv.zip',
                                                'size': 76742835,
                                                'source': 'DCP/1 Matrix Service',
                                                'fileSource': 'DCP/1 Matrix Service',
                                                'matrix_cell_count': None,
                                                'matrixCellCount': None,
                                                'uuid': '538faa28-3235-5e4b-a998-5672e2d964e8',
                                                'version': '2020-12-03T10:39:17.144517Z',
                                                'url': str(self.base_url.set(
                                                    path='/repository/files/538faa28-3235-5e4b-a998-5672e2d964e8',
                                                    args=dict(catalog='test', version='2020-12-03T10:39:17.144517Z')
                                                ))
                                            },
                                            {
                                                # Supplementary file, source from submitter_id
                                                'name': '8185730f-4113-40d3-9cc3-929271784c2b.homo_sapiens.mtx.zip',
                                                'size': 124022765,
                                                'source': 'DCP/1 Matrix Service',
                                                'fileSource': 'DCP/1 Matrix Service',
                                                'matrix_cell_count': None,
                                                'matrixCellCount': None,
                                                'uuid': '6c142250-567c-5b63-bd4f-0d78499863f8',
                                                'version': '2020-12-03T10:39:17.144517Z',
                                                'url': str(self.base_url.set(
                                                    path='/repository/files/6c142250-567c-5b63-bd4f-0d78499863f8',
                                                    args=dict(catalog='test', version='2020-12-03T10:39:17.144517Z')
                                                ))
                                            },
                                            {
                                                # Supplementary file, source from submitter_id
                                                'name': '8185730f-4113-40d3-9cc3-929271784c2b.homo_sapiens.loom',
                                                'size': 154980798,
                                                'source': 'DCP/1 Matrix Service',
                                                'fileSource': 'DCP/1 Matrix Service',
                                                'matrix_cell_count': None,
                                                'matrixCellCount': None,
                                                'uuid': '8d2ba1c1-bc9f-5c2a-a74d-fe5e09bdfb18',
                                                'version': '2020-12-03T10:39:17.144517Z',
                                                'url': str(self.base_url.set(
                                                    path='/repository/files/8d2ba1c1-bc9f-5c2a-a74d-fe5e09bdfb18',
                                                    args=dict(catalog='test', version='2020-12-03T10:39:17.144517Z')
                                                ))
                                            }
                                        ]
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        self.assertElasticsearchResultsEqual(matrices, one(hit['projects'])['matrices'])
        contributor_matrices = {
            'genusSpecies': {
                'Homo sapiens': {
                    'developmentStage': {
                        'adult': {
                            'organ': {
                                'eye': {
                                    'libraryConstructionApproach': {
                                        '10X v2 sequencing': [
                                            {
                                                # Supplementary file, source from submitter_id
                                                'name': 'E-MTAB-7316.processed.1.zip',
                                                'size': 69813802,
                                                'source': 'ArrayExpress',
                                                'fileSource': 'ArrayExpress',
                                                'matrix_cell_count': None,
                                                'matrixCellCount': None,
                                                'uuid': '87f31102-ebbc-5875-abdf-4fa5cea48e8d',
                                                'version': '2021-02-10T16:56:40.419579Z',
                                                'url': str(self.base_url.set(
                                                    path='/repository/files/87f31102-ebbc-5875-abdf-4fa5cea48e8d',
                                                    args=dict(catalog='test', version='2021-02-10T16:56:40.419579Z')
                                                ))
                                            },
                                            {
                                                # Supplementary file, source from submitter_id
                                                'name': 'E-MTAB-7316.processed.2.zip',
                                                'size': 118250749,
                                                'source': 'ArrayExpress',
                                                'fileSource': 'ArrayExpress',
                                                'matrix_cell_count': None,
                                                'matrixCellCount': None,
                                                'uuid': '733318e0-19c2-51e8-9ad6-d94ad562dd46',
                                                'version': '2021-02-10T16:56:40.419579Z',
                                                'url': str(self.base_url.set(
                                                    path='/repository/files/733318e0-19c2-51e8-9ad6-d94ad562dd46',
                                                    args=dict(catalog='test', version='2021-02-10T16:56:40.419579Z')
                                                ))
                                            },
                                            {
                                                # Supplementary file, source from submitter_id
                                                'name': 'E-MTAB-7316.processed.3.zip',
                                                'size': 187835236,
                                                'source': 'ArrayExpress',
                                                'fileSource': 'ArrayExpress',
                                                'matrix_cell_count': None,
                                                'matrixCellCount': None,
                                                'uuid': 'c59e2de5-01fe-56eb-be56-679ed14161bf',
                                                'version': '2021-02-10T16:56:40.419579Z',
                                                'url': str(self.base_url.set(
                                                    path='/repository/files/c59e2de5-01fe-56eb-be56-679ed14161bf',
                                                    args=dict(catalog='test', version='2021-02-10T16:56:40.419579Z')
                                                ))
                                            },
                                            {
                                                # Supplementary file, source from submitter_id
                                                'name': 'E-MTAB-7316.processed.4.zip',
                                                'size': 38722784,
                                                'source': 'ArrayExpress',
                                                'fileSource': 'ArrayExpress',
                                                'matrix_cell_count': None,
                                                'matrixCellCount': None,
                                                'uuid': '68bda896-3b3e-5f2a-9212-f4030a0f37e2',
                                                'version': '2021-02-10T16:56:40.419579Z',
                                                'url': str(self.base_url.set(
                                                    path='/repository/files/68bda896-3b3e-5f2a-9212-f4030a0f37e2',
                                                    args=dict(catalog='test', version='2021-02-10T16:56:40.419579Z')
                                                ))
                                            },
                                            {
                                                # Supplementary file, source from submitter_id
                                                'name': 'E-MTAB-7316.processed.5.zip',
                                                'size': 15535233,
                                                'source': 'ArrayExpress',
                                                'fileSource': 'ArrayExpress',
                                                'matrix_cell_count': None,
                                                'matrixCellCount': None,
                                                'uuid': '0c5ab869-da2d-5c11-b4ae-f978a052899f',
                                                'version': '2021-02-10T16:56:40.419579Z',
                                                'url': str(self.base_url.set(
                                                    path='/repository/files/0c5ab869-da2d-5c11-b4ae-f978a052899f',
                                                    args=dict(catalog='test', version='2021-02-10T16:56:40.419579Z')
                                                ))
                                            },
                                            {
                                                # Supplementary file, source from submitter_id
                                                'name': 'E-MTAB-7316.processed.6.zip',
                                                'size': 17985905,
                                                'source': 'ArrayExpress',
                                                'fileSource': 'ArrayExpress',
                                                'matrix_cell_count': None,
                                                'matrixCellCount': None,
                                                'uuid': 'cade4593-bfba-56ed-80ab-080d0de7d5a4',
                                                'version': '2021-02-10T16:56:40.419579Z',
                                                'url': str(self.base_url.set(
                                                    path='/repository/files/cade4593-bfba-56ed-80ab-080d0de7d5a4',
                                                    args=dict(catalog='test', version='2021-02-10T16:56:40.419579Z')
                                                ))
                                            },
                                            {
                                                # Supplementary file, source from submitter_id
                                                'name': 'E-MTAB-7316.processed.7.zip',
                                                'size': 7570475,
                                                'source': 'ArrayExpress',
                                                'fileSource': 'ArrayExpress',
                                                'matrix_cell_count': None,
                                                'matrixCellCount': None,
                                                'uuid': '5b465aad-0981-5152-b468-e615e20f5884',
                                                'version': '2021-02-10T16:56:40.419579Z',
                                                'url': str(self.base_url.set(
                                                    path='/repository/files/5b465aad-0981-5152-b468-e615e20f5884',
                                                    args=dict(catalog='test', version='2021-02-10T16:56:40.419579Z')
                                                ))
                                            },
                                            {
                                                # Supplementary file, source from submitter_id
                                                'name': 'WongRetinaCelltype.csv',
                                                'size': 2300969,
                                                'source': 'HCA Release',
                                                'fileSource': 'HCA Release',
                                                'matrix_cell_count': None,
                                                'matrixCellCount': None,
                                                'uuid': 'b905c8be-2e2d-592c-8481-3eb7a87c6484',
                                                'version': '2021-02-10T16:56:40.419579Z',
                                                'url': str(self.base_url.set(
                                                    path='/repository/files/b905c8be-2e2d-592c-8481-3eb7a87c6484',
                                                    args=dict(catalog='test', version='2021-02-10T16:56:40.419579Z')
                                                ))
                                            }
                                        ]
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        self.assertElasticsearchResultsEqual(contributor_matrices,
                                             one(hit['projects'])['contributorMatrices'])

    def test_matrix_cell_count(self):
        """
        Verify analysis file matrixCellCount values are correctly reported
        on all endpoints
        """
        params = self.params(project_id='bd400331-54b9-4fcc-bff6-6bb8b079ee1f')

        # Verify matrix cell counts in each hit from the non-files endpoints
        expected_counts = {
            'Rds': 54140,
            'fastq.gz': None
        }
        for endpoint in ('projects', 'samples'):
            with self.subTest(endpoint=endpoint):
                url = self.base_url.set(path=('index', endpoint), args=params)
                response = requests.get(str(url))
                response.raise_for_status()
                response_json = response.json()
                for hit in response_json['hits']:
                    actual_counts = {
                        s['fileType']: s['matrixCellCount']
                        for s in hit['fileTypeSummaries']
                    }
                    self.assertEqual(expected_counts, actual_counts)

        # Verify matrix cell counts across all hits in the 'files' endpoint
        expected_counts = {
            'Rds': 54140 * 3,  # 3 analysis files
        }
        actual_counts = Counter()
        url = self.base_url.set(path='/index/files', args=params)
        response = requests.get(str(url))
        response.raise_for_status()
        response_json = response.json()
        for hit in response_json['hits']:
            file = one(hit['files'])
            file_format = file['format']
            count = file['matrixCellCount']
            if count is not None:
                actual_counts[file_format] += count
        self.assertEqual(expected_counts, actual_counts)


class TestResponseSummary(WebServiceTestCase):
    maxDiff = None

    @classmethod
    def bundles(cls) -> List[BundleFQID]:
        return super().bundles() + [
            cls.bundle_fqid(uuid='dcccb551-4766-4210-966c-f9ee25d19190',
                            version='2018-10-18T204655.866661Z'),
            cls.bundle_fqid(uuid='94f2ba52-30c8-4de0-a78e-f95a3f8deb9c',
                            version='2019-04-03T103426.471000Z')
            # an imaging bundle
        ]

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    def test_summary_response(self):
        """
        Verify the /index/summary response with two sequencing bundles and
        one imaging bundle that has no cell suspension.

        - bundle=aaa96233…, fileCount=2, donorCount=1, totalCellCount=1.0, organType=pancreas, labCount=1
        - bundle=dcccb551…, fileCount=19, donorCount=4, totalCellCount=6210.0, organType=Brain, labCount=1
        - bundle=94f2ba52…, fileCount=227, donorCount=1, totalCellCount=0, organType=brain, labCount=(None counts as 1)
        """
        url = self.base_url.set(path='/index/summary',
                                args=dict(catalog=self.catalog))
        response = requests.get(str(url))
        response.raise_for_status()
        summary_object = response.json()
        self.assertEqual(summary_object['fileCount'], 2 + 19 + 227)
        self.assertEqual(summary_object['labCount'], 1 + 1 + 1)
        self.assertEqual(summary_object['donorCount'], 1 + 4 + 1)
        self.assertEqual(summary_object['totalCellCount'], 1.0 + 6210.0 + 0)
        file_counts_expected = {
            'tiff': 221,
            'json': 6,
            'fastq.gz': 5,
            'tsv': 4,
            'h5': 3,
            'pdf': 3,
            'mtx': 2,
            'bai': 1,
            'bam': 1,
            'csv': 1,
            'unknown': 1
        }
        file_counts_actual = {summary['format']: summary['count'] for summary in summary_object['fileTypeSummaries']}
        self.assertEqual(file_counts_actual, file_counts_expected)
        self.assertEqual(set(summary_object['organTypes']), {'Brain', 'brain', 'pancreas'})
        self.assertEqual(summary_object['cellCountSummaries'], [
            # 'brain' from the imaging bundle is not represented in cellCountSummaries as these values are tallied
            # from the cell suspensions and the imaging bundle does not have any cell suspensions
            {'organType': ['Brain'], 'countOfDocsWithOrganType': 1, 'totalCellCountByOrgan': 6210.0},
            {'organType': ['pancreas'], 'countOfDocsWithOrganType': 1, 'totalCellCountByOrgan': 1.0},
        ])

    def test_summary_filter_none(self):
        for use_filter, labCount in [(False, 3), (True, 2)]:
            with self.subTest(use_filter=use_filter, labCount=labCount):
                params = dict(catalog=self.catalog)
                if use_filter:
                    params['filters'] = json.dumps({"organPart": {"is": [None]}})
                url = self.base_url.set(path='/index/summary', args=params)
                response = requests.get(str(url))
                response.raise_for_status()
                summary_object = response.json()
                self.assertEqual(summary_object['labCount'], labCount)


@patch_dss_endpoint
@patch_source_cache
class TestUnpopulatedIndexResponse(WebServiceTestCase):

    @classmethod
    def bundles(cls) -> List[BundleFQID]:
        return []

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()
        cls.maxDiff = None

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    def facets(self) -> Sequence[str]:
        return self.app_module.app.service_config.facets

    def entity_types(self) -> List[str]:
        return [
            entity_type
            for entity_type in self.index_service.entity_types(self.catalog)
            if entity_type != 'cell_suspensions'
        ]

    def test_empty_response(self):
        for entity_type in self.entity_types():
            with self.subTest(entity_type=entity_type):
                url = self.base_url.set(path=('index', entity_type),
                                        args=dict(order='asc'))
                response = requests.get(str(url))
                response.raise_for_status()
                sort_field, _ = self.app_module.sort_defaults[entity_type]
                expected_response = {
                    'hits': [],
                    'pagination': {
                        'count': 0,
                        'total': 0,
                        'size': 10,
                        'next': None,
                        'previous': None,
                        'pages': 0,
                        'sort': sort_field,
                        'order': 'asc'
                    },
                    'termFacets': {
                        facet: {'terms': [], 'total': 0, 'type': 'terms'}
                        for facet in self.facets()
                    }}
                self.assertEqual(expected_response, response.json())

    def test_sorted_responses(self):
        # We can't test some facets as they are known to not work correctly
        # at this time. https://github.com/DataBiosphere/azul/issues/2621
        sortable_facets = {
            facet
            for facet in self.facets()
            if facet not in {'assayType', 'organismAgeRange'}
        }

        for entity_type, facet in product(self.entity_types(), sortable_facets):
            with self.subTest(entity=entity_type, facet=facet):
                url = self.base_url.set(path=('index', entity_type),
                                        args=dict(sort=facet))
                response = requests.get(str(url))
                self.assertEqual(200, response.status_code)


class TestPortalIntegrationResponse(LocalAppTestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return "service"

    maxDiff = None

    # Mocked DB content for the tests
    _portal_integrations_db = [
        {
            "portal_id": "9852dece-443d-42e8-869c-17b9a86d447e",
            "integrations": [
                {
                    "integration_id": "b87b7f30-2e60-4ca5-9a6f-00ebfcd35f35",
                    "integration_type": "get_manifest",
                    "entity_type": "file",
                    "manifest_type": "full",
                },
                {
                    "integration_id": "977854a0-2eea-4fec-9459-d4807fe79f0c",
                    "integration_type": "get",
                    "entity_type": "project",
                    "entity_ids": ["c4077b3c-5c98-4d26-a614-246d12c2e5d7"]
                }
            ]
        },
        {
            "portal_id": "f58bdc5e-98cd-4df4-80a4-7372dc035e87",
            "integrations": [
                {
                    "integration_id": "e8b3ca4f-bcf5-42eb-b58c-de6d7e0fe138",
                    "integration_type": "get",
                    "entity_type": "project",
                    "entity_ids": ["c4077b3c-5c98-4d26-a614-246d12c2e5d7"]
                },
                {
                    "integration_id": "dbfe9394-a326-4574-9632-fbadb51a7b1a",
                    "integration_type": "get",
                    "entity_type": "project",
                    "entity_ids": ["90bd6933-40c0-48d4-8d76-778c103bf545"]
                },
                {
                    "integration_id": "f13ddf2d-d913-492b-9ea8-2de4b1881c26",
                    "integration_type": "get",
                    "entity_type": "project",
                    "entity_ids": ["cddab57b-6868-4be4-806f-395ed9dd635a"]
                },
                {
                    "integration_id": "224b1d42-b939-4d10-8a8f-2b2ac304b813",
                    "integration_type": "get",
                    "entity_type": "project",
                    # NO entity_ids field
                }
            ]
        }
    ]

    def _mock_portal_crud(self, operation):
        operation(self._portal_integrations_db)

    def _get_integrations(self, params: dict) -> dict:
        url = self.base_url.set(path='/integrations', args=params)
        response = requests.get(str(url))
        response.raise_for_status()
        return response.json()

    @classmethod
    def _extract_integration_ids(cls, response_json):
        return [
            integration['integration_id']
            for portal in response_json
            for integration in portal['integrations']
        ]

    @mock.patch('azul.portal_service.PortalService._crud')
    def test_integrations(self, portal_crud):
        """
        Verify requests specifying `integration_type` and `entity_type` only return integrations matching those types
        """
        test_cases = [
            ('get_manifest', 'file', ['b87b7f30-2e60-4ca5-9a6f-00ebfcd35f35']),
            ('get', 'bundle', []),
            (
                'get',
                'project',
                [
                    '977854a0-2eea-4fec-9459-d4807fe79f0c',
                    'e8b3ca4f-bcf5-42eb-b58c-de6d7e0fe138',
                    'dbfe9394-a326-4574-9632-fbadb51a7b1a',
                    'f13ddf2d-d913-492b-9ea8-2de4b1881c26',
                    '224b1d42-b939-4d10-8a8f-2b2ac304b813'
                ]
            )
        ]
        portal_crud.side_effect = self._mock_portal_crud
        with mock.patch.object(type(config), 'dss_deployment_stage', 'prod'):
            for integration_type, entity_type, expected_integration_ids in test_cases:
                params = dict(integration_type=integration_type, entity_type=entity_type)
                with self.subTest(**params):
                    response_json = self._get_integrations(params)
                    found_integration_ids = self._extract_integration_ids(response_json)
                    self.assertEqual(len(expected_integration_ids), len(found_integration_ids))
                    self.assertEqual(set(expected_integration_ids), set(found_integration_ids))
                    self.assertTrue(all(isinstance(integration.get('entity_ids', []), list)
                                        for portal in response_json
                                        for integration in portal['integrations']))

    @mock.patch('azul.portal_service.PortalService._crud')
    def test_integrations_by_entity_ids(self, portal_crud):
        """
        Verify requests specifying `entity_ids` only return integrations matching those entity_ids
        """

        # 224b1d42-b939-4d10-8a8f-2b2ac304b813 must appear in every test since it lacks the entity_ids field
        test_cases = [
            # One project entity id specified by one integration
            (
                'cddab57b-6868-4be4-806f-395ed9dd635a',
                [
                    'f13ddf2d-d913-492b-9ea8-2de4b1881c26',
                    '224b1d42-b939-4d10-8a8f-2b2ac304b813'
                ]
            ),
            # Two project entity ids specified by two different integrations
            (
                'cddab57b-6868-4be4-806f-395ed9dd635a, 90bd6933-40c0-48d4-8d76-778c103bf545',
                [
                    'f13ddf2d-d913-492b-9ea8-2de4b1881c26',
                    'dbfe9394-a326-4574-9632-fbadb51a7b1a',
                    '224b1d42-b939-4d10-8a8f-2b2ac304b813'
                ]
            ),
            # One project entity id specified by two different integrations
            (
                'c4077b3c-5c98-4d26-a614-246d12c2e5d7',
                [
                    '977854a0-2eea-4fec-9459-d4807fe79f0c',
                    'e8b3ca4f-bcf5-42eb-b58c-de6d7e0fe138',
                    '224b1d42-b939-4d10-8a8f-2b2ac304b813'
                ]
            ),
            # Blank entity id, to match integrations lacking the entity_id field
            (
                '',
                [
                    '224b1d42-b939-4d10-8a8f-2b2ac304b813'
                ]
            ),
            # No entity id, accepting all integrations
            (
                None,
                [
                    'f13ddf2d-d913-492b-9ea8-2de4b1881c26',
                    'dbfe9394-a326-4574-9632-fbadb51a7b1a',
                    '977854a0-2eea-4fec-9459-d4807fe79f0c',
                    'e8b3ca4f-bcf5-42eb-b58c-de6d7e0fe138',
                    '224b1d42-b939-4d10-8a8f-2b2ac304b813'
                ]
            )
        ]

        portal_crud.side_effect = self._mock_portal_crud
        with mock.patch.object(type(config), 'dss_deployment_stage', 'prod'):
            for entity_ids, integration_ids in test_cases:
                params = dict(integration_type='get', entity_type='project')
                if entity_ids is not None:
                    params['entity_ids'] = entity_ids
                with self.subTest(**params):
                    response_json = self._get_integrations(params)
                    found_integration_ids = self._extract_integration_ids(response_json)
                    self.assertEqual(set(integration_ids), set(found_integration_ids))


class TestListCatalogsResponse(LocalAppTestCase, DSSUnitTestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return 'service'

    def test(self):
        response = requests.get(str(self.base_url.set(path='/index/catalogs')))
        self.assertEqual(200, response.status_code)
        self.assertEqual({
            'default_catalog': 'test',
            'catalogs': {
                'test': {
                    'internal': False,
                    'atlas': 'hca',
                    'plugins': {
                        'metadata': {
                            'name': 'hca',
                        },
                        'repository': {
                            'name': 'dss',
                            'sources': [
                                'https://dss.data.humancellatlas.org/v1:/2'
                            ],
                        }
                    }
                }
            }
        }, response.json())


if __name__ == '__main__':
    unittest.main()
