#!/usr/bin/python

import json
import os
import unittest
import urllib.parse

from more_itertools import one
import requests

from azul import config
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
                        "sha256": "fc5923256fb9dd349698d29228246a5c94653e80",
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
                        "preservation_method": specimen_value("aqua")
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
                            "sha256": "fc5923256fb9dd349698d29228246a5c94653e80",
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
                            "preservationMethod": ["aqua"],
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
                            "preservationMethod": "aqua"
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
                                "sha256": "fc5923256fb9dd349698d29228246a5c94653e80",
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
                                "preservationMethod": ["aqua"]
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
                                "sha256": "fc5923256fb9dd349698d29228246a5c94653e80",
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
                                "preservationMethod": ["aqua"]
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
                url = self.base_url + "/repository/summary"
                response = requests.get(url)
                response.raise_for_status()
                summary_object = response.json()
                self.assertGreater(summary_object['fileCount'], 0)
                self.assertGreater(summary_object['organCount'], 0)
                self.assertIsNotNone(summary_object['organSummaries'])

    def test_default_sorting_parameter(self):
        base_url = self.base_url
        url = base_url + "/repository/files"
        response = requests.get(url)
        response.raise_for_status()
        summary_object = response.json()
        self.assertEqual(summary_object['pagination']["sort"], "specimenId")

    def test_transform_request_with_file_url(self):
        base_url = self.base_url
        url = base_url + "/repository/files"
        response = requests.get(url)
        response.raise_for_status()
        response_json = response.json()
        bundle_files = [file_data for hit in response_json['hits'] for file_data in hit['files']]
        for file_data in bundle_files:
            self.assertIn('url', file_data.keys())
            actual_url = urllib.parse.urlparse(file_data['url'])
            actual_query_vars = {k: one(v) for k, v in urllib.parse.parse_qs(actual_url.query).items()}
            expected_base_url = urllib.parse.urlparse(config.service_endpoint())
            self.assertEquals(expected_base_url.netloc, actual_url.netloc)
            self.assertEquals(expected_base_url.scheme, actual_url.scheme)
            self.assertIsNotNone(actual_url.path)
            self.assertEquals('aws', actual_query_vars['replica'])
            self.assertIsNotNone(actual_query_vars['version'])

    def test_project_summary_cell_count(self):
        """
        Test per organ and total cell counter in ProjectSummaryResponse. Should return a correct total cell count and
        per organ cell count. Should not double count cell count from cell suspensions with an already counted id (
        i.e. each unique cell suspension counted exactly once).
        """
        es_hit = {
            "specimens": [
                {
                    "biomaterial_id": ["specimen1", "specimen3"],
                    "disease": ["disease1"],
                    "donor_biomaterial_id": ["donor1"],
                    "genus_species": ["species1"]
                },
                {
                    "biomaterial_id": ["specimen2"],
                    "disease": ["disease1"],
                    "donor_biomaterial_id": ["donor1"],
                    "genus_species": ["species1"]
                }
            ],
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

    def test_projects_key_search_response(self):
        """
        Test building response for projects
        Response should include project detail fields that do not appear for other entity type responses
        """
        keyword_response = KeywordSearchResponse(
            hits=self.input('projects'),
            entity_type='projects'
        ).return_response().to_json()

        expected_output = {
            "hits": [
                {
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
                            "preservationMethod": ["aqua"]
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
        Response should include project detail fields that do not appear for other entity type responses
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
                            "preservationMethod": ["aqua"]
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

    def test_project_summary_response(self):
        """
        Test that ProjectSummaryResponse will correctly do the per-project aggregations

        Should only return values associated with the given project id
        Should sum cell counts per-organ per-project and return an organ summary
        Should correctly get distinct values for diseases, species, library construction approaches for each project
        Should correctly count donor ids within a project
        """
        # Stripped down response from ES partially based on real data
        hits = [
            {
                "_id": "bae45747-546a-4aed-9377-08e9115a8fb8",
                "_source": {
                    "entity_id": "bae45747-546a-4aed-9377-08e9115a8fb8",
                    "contents": {
                        "entryId": "bae45747-546a-4aed-9377-08e9115a8fb8",
                        "specimens": [
                            {
                                "disease": ["glioblastoma"],
                                "_type": ["specimen"],
                                "donor_biomaterial_id": ["Q4_DEMO-donor_MGH30"],
                                "genus_species": ["Homo sapiens"]
                            }
                        ],
                        "cell_suspensions": [
                            {
                                "organ": ["brain"],
                                "total_estimated_cells": 0
                            }
                        ],
                        "processes": [
                            {
                                "_type": ["process"],
                                "library_construction_approach": ["Smart-seq2"]
                            },
                            {
                                "_type": ["process"],
                                "library_construction_approach": ["Smart-seq2"]
                            }
                        ]
                    }
                }
            },
            {
                "_id": "6ec8e247-2eb0-42d1-823f-75facd03988d",
                "_source": {
                    "entity_id": "6ec8e247-2eb0-42d1-823f-75facd03988d",
                    "contents": {
                        "entryId": "6ec8e247-2eb0-42d1-823f-75facd03988d",
                        "specimens": [
                            {
                                "disease": ["normal"],
                                "_type": ["specimen"],
                                "donor_biomaterial_id": ["284C-A1"],
                                "genus_species": ["Homo sapiens"]
                            },
                            {
                                "disease": ["normal"],
                                "_type": ["specimen"],
                                "donor_biomaterial_id": ["284C-A1"],
                                "genus_species": ["Homo sapiens"]
                            },
                            {
                                "disease": ["not normal"],
                                "organ": ["brain"],
                                "_type": ["specimen"],
                                "total_estimated_cells": 10,
                                "donor_biomaterial_id": ["284C-A2"],
                                "genus_species": ["Homo sapiens"]
                            }
                        ],
                        "cell_suspensions": [
                            {
                                "organ": ["spleen"],
                                "total_estimated_cells": 39300000
                            },
                            {
                                "organ": ["spleen"],
                                "total_estimated_cells": 1
                            },
                            {
                                "organ": ["brain"],
                                "total_estimated_cells": 10
                            }
                        ],
                        "processes": [
                            {
                                "_type": ["process"],
                                "library_construction_approach": ["10x_v2"]
                            },
                            {
                                "_type": ["process"]
                            }
                        ]
                    }
                }
            },
            {
                "_id": "6504d48c-1610-43aa-8cf8-214a960e110c",
                "_source": {
                    "entity_id": "6504d48c-1610-43aa-8cf8-214a960e110c",
                    "contents": {
                        "entryId": "6504d48c-1610-43aa-8cf8-214a960e110c",
                        "specimens": [
                            {
                                "disease": [],
                                "_type": ["specimen"],
                                "donor_biomaterial_id": [
                                    "CB8",
                                    "CB6",
                                    "CB2",
                                    "BM8",
                                    "BM6",
                                    "BM5",
                                    "BM4",
                                    "CB1",
                                    "CB5",
                                    "CB7",
                                    "BM2",
                                    "BM3",
                                    "BM7",
                                    "CB4",
                                    "CB3",
                                    "BM1"
                                ],
                                "genus_species": ["Homo sapiens"]
                            }
                        ],
                        "cell_suspensions": [
                            {
                                "organ": ["hematopoietic system"],
                                "total_estimated_cells": 528092
                            }
                        ],
                        "processes": [
                            {
                                "_type": ["process"]
                            },
                            {
                                "_type": ["process"],
                                "library_construction_approach": ["10x_v2"]
                            }
                        ]
                    }
                }
            }
        ]

        project_summary1 = ProjectSummaryResponse(hits[0]['_source']['contents']).apiResponse.to_json()
        self.assertEqual(1, project_summary1['donorCount'])
        self.assertEqual(0, project_summary1['totalCellCount'])
        self.assertEqual(['Homo sapiens'], sorted(project_summary1['genusSpecies']))
        self.assertEqual(['Smart-seq2'], sorted(project_summary1['libraryConstructionApproach']))
        self.assertEqual(['glioblastoma'], sorted(project_summary1['disease']))
        expected_organ_summary1 = [
            {
                "organType": "brain",
                "countOfDocsWithOrganType": 1,
                "totalCellCountByOrgan": 0.0
            }
        ]
        self.assertEqual(json.dumps(expected_organ_summary1, sort_keys=True),
                         json.dumps(project_summary1['organSummaries'], sort_keys=True))

        project_summary2 = ProjectSummaryResponse(hits[1]['_source']['contents']).apiResponse.to_json()
        self.assertEqual(2, project_summary2['donorCount'])
        self.assertEqual(39300011, project_summary2['totalCellCount'])
        self.assertEqual(['Homo sapiens'], sorted(project_summary2['genusSpecies']))
        self.assertEqual(['10x_v2'], sorted(project_summary2['libraryConstructionApproach']))
        self.assertEqual(['normal', 'not normal'], sorted(project_summary2['disease']))
        expected_organ_summary2 = [
            {
                "organType": "spleen",
                "countOfDocsWithOrganType": 1,
                "totalCellCountByOrgan": 39300001.0
            },
            {
                "organType": "brain",
                "countOfDocsWithOrganType": 1,
                "totalCellCountByOrgan": 10.0
            }
        ]
        self.assertEqual(json.dumps(expected_organ_summary2, sort_keys=True),
                         json.dumps(project_summary2['organSummaries'], sort_keys=True))

        project_summary3 = ProjectSummaryResponse(hits[2]['_source']['contents']).apiResponse.to_json()
        self.assertEqual(16, project_summary3['donorCount'])
        self.assertEqual(528092, project_summary3['totalCellCount'])
        self.assertEqual(['Homo sapiens'], sorted(project_summary3['genusSpecies']))
        self.assertEqual(['10x_v2'], sorted(project_summary3['libraryConstructionApproach']))
        self.assertEqual([], sorted(project_summary3['disease']))
        expected_organ_summary3 = [
            {
                "organType": "hematopoietic system",
                "countOfDocsWithOrganType": 1,
                "totalCellCountByOrgan": 528092.0
            }
        ]
        self.assertEqual(json.dumps(expected_organ_summary3, sort_keys=True),
                         json.dumps(project_summary3['organSummaries'], sort_keys=True))

    def _load(self, filename):
        data_folder_filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
        with open(os.path.join(data_folder_filepath, filename)) as fp:
            return json.load(fp)


if __name__ == '__main__':
    unittest.main()
