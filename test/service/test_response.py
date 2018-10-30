#!/usr/bin/python
from functools import partial
import json
import unittest
import os

import requests

from azul.service.responseobjects.hca_response_v5 import (FileSearchResponse,
                                                          KeywordSearchResponse,
                                                          ProjectSummaryResponse)
from service import WebServiceTestCase
from service.data_generator.fake_data_utils import ElasticsearchFakeDataLoader


class TestResponse(WebServiceTestCase):
    maxDiff = None

    def input(self, entity_type):
        def specimen_value(v):
            return v if entity_type == 'specimens' else [v]

        def project_value(v):
            return v if entity_type == 'projects' else [v]

        template = {
            "contents": {
                "files": [
                    {
                        "_type": "fuchsia",
                        "content-type": "green",
                        "file_format": "csv",
                        "document_id": "a350b29c-2609-7b92-0c49-23971a4f9371",
                        "indexed": True,
                        "lane_index": 5108,
                        "name": "billion.key",
                        "read_index": "blue",
                        "sha1": "fc5923256fb9dd349698d29228246a5c94653e80",
                        "size": 6667,
                        "uuid": "e9772583-4240-4757-6357-32bef0e51150",
                        "version": "2001-03-16T05:26:40"
                    } if entity_type == 'files' else {
                        "file_format": ["csv"],
                        "size": 6667,
                        "count": 1,
                    }
                ],
                "processes": [
                    {
                        "_type": ["navy"],
                        "document_id": ["4188ddbe-8865-a433-a1f6-213d49aa5719"],
                        "instrument_manufacturer_model": ["green"],
                        "library_construction_approach": ["fuchsia"],
                        "process_id": ["maroon"],
                        "process_name": ["olive"],
                        "protocol_name": ["olive"],
                        "protocol_id": ["green"]
                    }
                ],
                "projects": [
                    {
                        "_type": project_value("maroon"),
                        "document_id": project_value("37a92077-530f-fdbb-df14-2926665cc697"),
                        "project_title": project_value("purple"),
                        "project_description": project_value("navy"),
                        "laboratory": ["silver"],
                        "project_shortname": project_value("blue"),
                        "contributors": [
                            {
                                "contact_name": "yellow",
                                "corresponding_contributor": False,
                                "email": "gray"
                            },
                            {
                                "contact_name": "teal",
                                "corresponding_contributor": True,
                                "email": "purple",
                                "institution": "yellow",
                                "laboratory": "silver"
                            }
                        ],
                        "publications": [
                            {
                                "authors": [
                                    "green",
                                    "maroon",
                                    "gray"
                                ],
                                "publication_title": "gray",
                                "doi": "green",
                                "pmid": 5331933,
                                "publication_url": "black"
                            }
                        ]
                    }
                ],
                "specimens": [
                    {
                        "_type": specimen_value("teal"),
                        "organism_age": ["purple"],
                        "organism_age_unit": ["navy"],
                        "biomaterial_id": specimen_value("6e7d782e-44a2-0d3f-2bf1-337468f62467"),
                        "disease": ["yellow"],
                        "id": specimen_value("1cae440e-3be6-ce39-49e9-74721f0066e0"),
                        "organ": specimen_value("purple"),
                        "organ_part": specimen_value("black"),
                        "parent": specimen_value("aqua"),
                        "biological_sex": ["silver"],
                        "_source": ["purple"],
                        "genus_species": ["teal"],
                        "storage_method": specimen_value("aqua")
                    }
                ],
                "cell_suspensions": [
                    {
                        "organ": ["purple"],
                        "organ_part": ["black"],
                        "total_estimated_cells": 5306
                    }
                ]
            },
            "bundles": [
                {
                    "uuid": "cfc75555-f551-ba6c-2e62-0bf0ee01313c",
                    "version": "2003-08-12T00:52:21"
                }
            ],
            "entity_id": "08d3440a-7481-41c5-5140-e15ed269ea63"
        }
        return [ElasticsearchFakeDataLoader.fix_canned_document(entity_type, template)]

    def test_key_search_files_response(self):
        """
        This method tests the KeywordSearchResponse object for the files entity type.
        It will make sure the functionality works as appropriate by asserting the
        apiResponse attribute is the same as expected.
        """
        # Still need a way to test the response.
        keyword_response = KeywordSearchResponse(
            hits=self.input('files'),
            entity_type='files'
        ).return_response().to_json()

        expected_response = {
            "hits": [
                {
                    "bundles": [
                        {
                            "bundleUuid": "cfc75555-f551-ba6c-2e62-0bf0ee01313c",
                            "bundleVersion": "2003-08-12T00:52:21"
                        }
                    ],
                    "entryId": "08d3440a-7481-41c5-5140-e15ed269ea63",
                    "files": [
                        {
                            "format": "csv",
                            "name": "billion.key",
                            "sha1": "fc5923256fb9dd349698d29228246a5c94653e80",
                            "size": 6667,
                            "uuid": "e9772583-4240-4757-6357-32bef0e51150",
                            "version": "2001-03-16T05:26:40"
                        }
                    ],
                    "processes": [
                        {
                            "instrumentManufacturerModel": ["green"],
                            "libraryConstructionApproach": ["fuchsia"],
                            "processId": ["maroon"],
                            "processName": ["olive"],
                            "protocol": ["olive"],
                            "protocolId": ["green"]
                        }
                    ],
                    "projects": [
                        {
                            "projectTitle": ["purple"],
                            "laboratory": ["silver"],
                            "projectShortname": ["blue"]
                        }
                    ],
                    "specimens": [
                        {
                            "biologicalSex": ["silver"],
                            "disease": ["yellow"],
                            "genusSpecies": ["teal"],
                            "id": ["6e7d782e-44a2-0d3f-2bf1-337468f62467"],
                            "organ": ["purple"],
                            "organPart": ["black"],
                            "organismAge": ["purple"],
                            "organismAgeUnit": ["navy"],
                            "source": ["purple"],
                            "storageMethod": ["aqua"],
                        }
                    ],
                    "cellSuspensions": [
                        {
                            "organ": ["purple"],
                            "organPart": ["black"],
                            "totalCells": 5306
                        }
                    ]
                }
            ]
        }

        self.assertEqual(json.dumps(keyword_response, sort_keys=True, indent=4),
                         json.dumps(expected_response, sort_keys=True, indent=4))

    def test_key_search_specimens_response(self):
        """
        KeywordSearchResponse for the specimens endpoint should return file type summaries instead of files
        """
        # Still need a way to test the response.
        keyword_response = KeywordSearchResponse(
            hits=self.input('specimens'),
            entity_type='specimens'
        ).return_response().to_json()

        expected_response = {
            "hits": [
                {
                    "bundles": [
                        {
                            "bundleUuid": "cfc75555-f551-ba6c-2e62-0bf0ee01313c",
                            "bundleVersion": "2003-08-12T00:52:21"
                        }
                    ],
                    "entryId": "08d3440a-7481-41c5-5140-e15ed269ea63",
                    "fileTypeSummaries": [
                        {
                            "count": 1,
                            "fileType": "csv",
                            "totalSize": 6667
                        }
                    ],
                    "processes": [
                        {
                            "instrumentManufacturerModel": ["green"],
                            "libraryConstructionApproach": ["fuchsia"],
                            "processId": ["maroon"],
                            "processName": ["olive"],
                            "protocol": ["olive"],
                            "protocolId": ["green"]
                        }
                    ],
                    "projects": [
                        {
                            "projectTitle": ["purple"],
                            "laboratory": ["silver"],
                            "projectShortname": ["blue"]
                        }
                    ],
                    "specimens": [
                        {
                            "biologicalSex": ["silver"],
                            "disease": ["yellow"],
                            "genusSpecies": ["teal"],
                            "id": "6e7d782e-44a2-0d3f-2bf1-337468f62467",
                            "organ": "purple",
                            "organPart": "black",
                            "organismAge": ["purple"],
                            "organismAgeUnit": ["navy"],
                            "source": ["purple"],
                            "storageMethod": "aqua"
                        }
                    ],
                    "cellSuspensions": [
                        {
                            "organ": ["purple"],
                            "organPart": ["black"],
                            "totalCells": 5306
                        }
                    ]
                }
            ]
        }
        self.assertEqual(json.dumps(keyword_response, sort_keys=True, indent=4),
                         json.dumps(expected_response, sort_keys=True, indent=4))

    paginations = [
        {
            "count": 2,
            "order": "desc",
            "pages": 1,
            "size": 5,
            "sort": "entryId",
            "total": 2
        }, {
            "count": 2,
            "order": "desc",
            "pages": 1,
            "search_after": "cbb998ce-ddaf-34fa-e163-d14b399c6b34",
            "search_after_uid": "meta#32",
            "size": 5,
            "sort": "entryId",
            "total": 2
        }
    ]

    def test_file_search_response(self):
        """
        n=0: Test the FileSearchResponse object, making sure the functionality works as appropriate by asserting the
        apiResponse attribute is the same as expected.

        n=1: Tests the FileSearchResponse object, using 'search_after' pagination.
        """
        responses = [
            {
                "hits": [
                    {
                        "bundles": [
                            {
                                "bundleUuid": "cfc75555-f551-ba6c-2e62-0bf0ee01313c",
                                "bundleVersion": "2003-08-12T00:52:21"
                            }
                        ],
                        "entryId": "08d3440a-7481-41c5-5140-e15ed269ea63",
                        "files": [
                            {
                                "format": "csv",
                                "name": "billion.key",
                                "sha1": "fc5923256fb9dd349698d29228246a5c94653e80",
                                "size": 6667,
                                "uuid": "e9772583-4240-4757-6357-32bef0e51150",
                                "version": "2001-03-16T05:26:40"
                            }
                        ],
                        "processes": [
                            {
                                "instrumentManufacturerModel": ["green"],
                                "libraryConstructionApproach": ["fuchsia"],
                                "processId": ["maroon"],
                                "processName": ["olive"],
                                "protocol": ["olive"],
                                "protocolId": ["green"]
                            }
                        ],
                        "projects": [
                            {
                                "projectTitle": ["purple"],
                                "laboratory": ["silver"],
                                "projectShortname": ["blue"]
                            }
                        ],
                        "specimens": [
                            {
                                "biologicalSex": ["silver"],
                                "disease": ["yellow"],
                                "genusSpecies": ["teal"],
                                "id": ["6e7d782e-44a2-0d3f-2bf1-337468f62467"],
                                "organ": ["purple"],
                                "organPart": ["black"],
                                "organismAge": ["purple"],
                                "organismAgeUnit": ["navy"],
                                "source": ["purple"],
                                "storageMethod": ["aqua"]
                            }
                        ],
                        "cellSuspensions": [
                            {
                                "organ": ["purple"],
                                "organPart": ["black"],
                                "totalCells": 5306
                            }
                        ]
                    }
                ],
                "pagination": {
                    "count": 2,
                    "order": "desc",
                    "pages": 1,
                    "search_after": None,
                    "search_after_uid": None,
                    "search_before": None,
                    "search_before_uid": None,
                    "size": 5,
                    "sort": "entryId",
                    "total": 2
                },
                "termFacets": {

                }
            }, {
                "hits": [
                    {
                        "bundles": [
                            {
                                "bundleUuid": "cfc75555-f551-ba6c-2e62-0bf0ee01313c",
                                "bundleVersion": "2003-08-12T00:52:21"
                            }
                        ],
                        "entryId": "08d3440a-7481-41c5-5140-e15ed269ea63",
                        "files": [
                            {
                                "format": "csv",
                                "name": "billion.key",
                                "sha1": "fc5923256fb9dd349698d29228246a5c94653e80",
                                "size": 6667,
                                "uuid": "e9772583-4240-4757-6357-32bef0e51150",
                                "version": "2001-03-16T05:26:40"
                            }
                        ],
                        "processes": [
                            {
                                "instrumentManufacturerModel": ["green"],
                                "libraryConstructionApproach": ["fuchsia"],
                                "processId": ["maroon"],
                                "processName": ["olive"],
                                "protocol": ["olive"],
                                "protocolId": ["green"]
                            }
                        ],
                        "projects": [
                            {
                                "projectTitle": ["purple"],
                                "laboratory": ["silver"],
                                "projectShortname": ["blue"]
                            }
                        ],
                        "specimens": [
                            {
                                "biologicalSex": ["silver"],
                                "disease": ["yellow"],
                                "genusSpecies": ["teal"],
                                "id": ["6e7d782e-44a2-0d3f-2bf1-337468f62467"],
                                "organ": ["purple"],
                                "organPart": ["black"],
                                "organismAge": ["purple"],
                                "organismAgeUnit": ["navy"],
                                "source": ["purple"],
                                "storageMethod": ["aqua"]
                            }
                        ],
                        "cellSuspensions": [
                            {
                                "organ": ["purple"],
                                "organPart": ["black"],
                                "totalCells": 5306
                            }
                        ]
                    }
                ],
                "pagination": {
                    "count": 2,
                    "order": "desc",
                    "pages": 1,
                    "search_after": "cbb998ce-ddaf-34fa-e163-d14b399c6b34",
                    "search_after_uid": "meta#32",
                    "search_before": None,
                    "search_before_uid": None,
                    "size": 5,
                    "sort": "entryId",
                    "total": 2
                },
                "termFacets": {

                }
            }

        ]
        for n in 0, 1:
            with self.subTest(n=n):
                filesearch_response = FileSearchResponse(
                    hits=self.input('files'),
                    pagination=self.paginations[n],
                    facets={},
                    entity_type="files"
                ).return_response().to_json()

                self.assertEqual(json.dumps(filesearch_response, sort_keys=True, indent=4),
                                 json.dumps(responses[n], sort_keys=True, indent=4))

    def test_file_search_response_file_summaries(self):
        """
        Test non-'files' entity type passed to FileSearchResponse will give file summaries
        """
        filesearch_response = FileSearchResponse(
            hits=self.input('specimens'),
            pagination=self.paginations[0],
            facets={},
            entity_type="specimens"
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
        self.assertEqual(json.dumps(facets, sort_keys=True, indent=4),
                         json.dumps(expected_output, sort_keys=True, indent=4))

    def test_summary_endpoint(self):
        for entity_type in 'specimens', 'files':
            with self.subTest(entity_type=entity_type):
                url = self.base_url + "repository/summary"
                response = requests.get(url)
                response.raise_for_status()
                summary_object = response.json()
                self.assertGreater(summary_object['fileCount'], 0)
                self.assertGreater(summary_object['organCount'], 0)
                self.assertIsNotNone(summary_object['organSummaries'])

    def test_default_sorting_parameter(self):
        base_url = self.base_url
        url = base_url + "repository/files"
        response = requests.get(url)
        response.raise_for_status()
        summary_object = response.json()
        self.assertEqual(summary_object['pagination']["sort"], "specimenId")

    def test_project_summary_cell_count(self):
        """
        Test per organ and total cell counter in ProjectSummaryResponse. Should return a correct total cell count and
        per organ cell count. Should not double count cell count from cell suspensions with an already counted id (
        i.e. each unique cell suspension counted exactly once).
        """
        es_hit = {
            "_id": "a",
            "_source": {
                "entity_id": "a",
                "contents": {
                    "cell_suspensions": [
                        {
                            "organ": ["organ1"],
                            "total_estimated_cells": 6,
                        },
                        {
                            "organ": ["organ2"],
                            "total_estimated_cells": 3,
                        }

                    ],
                    "files": [],
                    "processes": [],
                    "project": {
                        "document_id": "a"
                    }
                }
            }
        }

        expected_output = [
            {
                "key": "organ1",
                "value": 6
            },
            {
                "key": "organ2",
                "value": 3
            }
        ]

        total_cell_count, organ_cell_count = ProjectSummaryResponse.get_cell_count(es_hit)

        self.assertEqual(total_cell_count,
                         sum([cell_count['value'] for cell_count in expected_output]))
        self.assertEqual(json.dumps(organ_cell_count, sort_keys=True, indent=4),
                         json.dumps(expected_output, sort_keys=True, indent=4))

    project_buckets = {
        "buckets": [
            {
                "key": "project1",
                "term_bucket": {
                    "buckets": [
                        {
                            "key": "term1"
                        },
                        {
                            "key": "term2"
                        },
                        {
                            "key": "term3"
                        }
                    ]
                },
                "value_bucket": {
                    "value": 2
                }
            },
            {
                "key": "project2",
                "term_bucket": {
                    "buckets": []
                },
                "value_bucket": {
                    "value": 4
                }
            }
        ]
    }

    def test_project_get_bucket_terms(self):
        """
        Test getting all unique terms of a given facet of a given project
        Should only return values of the given project
        Should return an empty list if project has no values in the term or if project does not exist
        """

        bucket_terms_1 = ProjectSummaryResponse.get_bucket_terms('project1', self.project_buckets, 'term_bucket')
        self.assertEqual(bucket_terms_1, ['term1', 'term2', 'term3'])

        bucket_terms_2 = ProjectSummaryResponse.get_bucket_terms('project2', self.project_buckets, 'term_bucket')
        self.assertEqual(bucket_terms_2, [])

        bucket_terms_3 = ProjectSummaryResponse.get_bucket_terms('project3', self.project_buckets, 'term_bucket')
        self.assertEqual(bucket_terms_3, [])

    def test_project_get_bucket_values(self):
        """
        Test getting value of a given aggregation of a given project
        Should only value of the given project
        Should return -1 if project is not found
        """
        bucket_terms_1 = ProjectSummaryResponse.get_bucket_value('project1', self.project_buckets, 'value_bucket')
        self.assertEqual(bucket_terms_1, 2)

        bucket_terms_2 = ProjectSummaryResponse.get_bucket_value('project2', self.project_buckets, 'value_bucket')
        self.assertEqual(bucket_terms_2, 4)

        bucket_terms_3 = ProjectSummaryResponse.get_bucket_value('project3', self.project_buckets, 'value_bucket')
        self.assertEqual(bucket_terms_3, -1)

    def test_projects_key_search_response(self):
        """
        Test building response for projects
        Response should include project detail fields that do not appear for other entity type repsponses
        """
        keyword_response = KeywordSearchResponse(
            hits=self.input('projects'),
            entity_type='projects'
        ).return_response().to_json()

        expected_output = {
            "hits": [
                {
                    "bundles": [
                        {
                            "bundleUuid": "cfc75555-f551-ba6c-2e62-0bf0ee01313c",
                            "bundleVersion": "2003-08-12T00:52:21"
                        }
                    ],
                    "entryId": "08d3440a-7481-41c5-5140-e15ed269ea63",
                    "fileTypeSummaries": [
                        {
                            "count": 1,
                            "fileType": "csv",
                            "totalSize": 6667
                        }
                    ],
                    "processes": [
                        {
                            "instrumentManufacturerModel": ["green"],
                            "libraryConstructionApproach": ["fuchsia"],
                            "processId": ["maroon"],
                            "processName": ["olive"],
                            "protocol": ["olive"],
                            "protocolId": ["green"]
                        }
                    ],
                    "projects": [
                        {
                            "projectTitle": "purple",
                            "projectDescription": "navy",
                            "laboratory": ["silver"],
                            "projectShortname": "blue",
                            "contributors": [
                                {
                                    "contactName": "yellow",
                                    "correspondingContributor": False,
                                    "email": "gray"
                                },
                                {
                                    "contactName": "teal",
                                    "correspondingContributor": True,
                                    "email": "purple",
                                    "institution": "yellow",
                                    "laboratory": "silver"
                                }
                            ],
                            "publications": [
                                {
                                    "authors": [
                                        "green",
                                        "maroon",
                                        "gray"
                                    ],
                                    "publicationTitle": "gray",
                                    "doi": "green",
                                    "pmid": 5331933,
                                    "publicationUrl": "black"
                                }
                            ]
                        }
                    ],
                    "specimens": [
                        {
                            "biologicalSex": ["silver"],
                            "disease": ["yellow"],
                            "genusSpecies": ["teal"],
                            "id": ["6e7d782e-44a2-0d3f-2bf1-337468f62467"],
                            "organ": ["purple"],
                            "organPart": ["black"],
                            "organismAge": ["purple"],
                            "organismAgeUnit": ["navy"],
                            "source": ["purple"],
                            "storageMethod": ["aqua"]
                        }
                    ],
                    "cellSuspensions": [
                        {
                            "organ": ["purple"],
                            "organPart": ["black"],
                            "totalCells": 5306
                        }
                    ]
                }
            ]
        }
        self.assertEqual(json.dumps(keyword_response, sort_keys=True, indent=4),
                         json.dumps(expected_output, sort_keys=True, indent=4))

    def test_projects_file_search_response(self):
        """
        Test building response for projects
        Response should include project detail fields that do not appear for other entity type repsponses
        """
        keyword_response = FileSearchResponse(
            hits=self.input('projects'),
            pagination=self.paginations[0],
            facets=self.facets_populated,
            entity_type='projects'
        ).return_response().to_json()

        expected_output = {
            "hits": [
                {
                    "bundles": [
                        {
                            "bundleUuid": "cfc75555-f551-ba6c-2e62-0bf0ee01313c",
                            "bundleVersion": "2003-08-12T00:52:21"
                        }
                    ],
                    "entryId": "08d3440a-7481-41c5-5140-e15ed269ea63",
                    "fileTypeSummaries": [
                        {
                            "count": 1,
                            "fileType": "csv",
                            "totalSize": 6667
                        }
                    ],
                    "processes": [
                        {
                            "instrumentManufacturerModel": ["green"],
                            "libraryConstructionApproach": ["fuchsia"],
                            "processId": ["maroon"],
                            "processName": ["olive"],
                            "protocol": ["olive"],
                            "protocolId": ["green"]
                        }
                    ],
                    "projects": [
                        {
                            "projectTitle": "purple",
                            "projectDescription": "navy",
                            "laboratory": ["silver"],
                            "projectShortname": "blue",
                            "contributors": [
                                {
                                    "contactName": "yellow",
                                    "correspondingContributor": False,
                                    "email": "gray"
                                },
                                {
                                    "contactName": "teal",
                                    "correspondingContributor": True,
                                    "email": "purple",
                                    "institution": "yellow",
                                    "laboratory": "silver"
                                }
                            ],
                            "publications": [
                                {
                                    "authors": [
                                        "green",
                                        "maroon",
                                        "gray"
                                    ],
                                    "publicationTitle": "gray",
                                    "doi": "green",
                                    "pmid": 5331933,
                                    "publicationUrl": "black"
                                }
                            ]
                        }
                    ],
                    "specimens": [
                        {
                            "biologicalSex": ["silver"],
                            "disease": ["yellow"],
                            "genusSpecies": ["teal"],
                            "id": ["6e7d782e-44a2-0d3f-2bf1-337468f62467"],
                            "organ": ["purple"],
                            "organPart": ["black"],
                            "organismAge": ["purple"],
                            "organismAgeUnit": ["navy"],
                            "source": ["purple"],
                            "storageMethod": ["aqua"]
                        }
                    ],
                    "cellSuspensions": [
                        {
                            "organ": ["purple"],
                            "organPart": ["black"],
                            "totalCells": 5306
                        }
                    ]
                }
            ],
            "pagination": {
                "count": 2,
                "order": "desc",
                "pages": 1,
                "size": 5,
                "sort": "entryId",
                "total": 2,
                "search_after": None,
                "search_after_uid": None,
                "search_before": None,
                "search_before_uid": None
            },
            "termFacets": {
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
                },
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
                }
            }
        }

        self.assertEqual(json.dumps(keyword_response, sort_keys=True, indent=4),
                         json.dumps(expected_output, sort_keys=True, indent=4))

    def _load(self, filename):
        data_folder_filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
        with open(os.path.join(data_folder_filepath, filename)) as fp:
            return json.load(fp)


if __name__ == '__main__':
    unittest.main()
