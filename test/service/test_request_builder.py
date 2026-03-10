from collections.abc import (
    Mapping,
    Sequence,
)

import attr
from opensearchpy import (
    Search,
)

from azul import (
    CatalogName,
    JSON,
)
from azul.indexer.field import (
    FieldTypes,
    null_str,
)
from azul.logging import (
    configure_test_logging,
)
from azul.plugins import (
    FieldPath,
    InverseFieldMapping,
    ManifestConfig,
    MetadataPlugin,
)
from azul.plugins.metadata.hca import (
    Plugin,
)
from azul.plugins.metadata.hca.service.aggregation import (
    HCAAggregationStage,
)
from azul.service import (
    Filters,
)
from azul.service.query_service import (
    QueryService,
    ToDictStage,
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


class TestRequestBuilder(DCP1CannedBundleTestCase, WebServiceTestCase):
    # Subclass the class under test so we can inject a mock plugin
    @attr.s(frozen=True, auto_attribs=True)
    class Service(QueryService):
        plugin: MetadataPlugin

        def metadata_plugin(self, catalog: CatalogName) -> MetadataPlugin:
            return self.plugin

    class MockPlugin(Plugin):

        @property
        def _field_mapping(self) -> InverseFieldMapping:
            return {
                'default_factory': {},
                'entity_id': 'entity_id',
                'sources': {
                    'id': 'sourceId'
                },
                'contents': {
                    'projects': {
                        'document_id': 'projectId',
                        'institutions': 'institution',
                        'laboratory': 'laboratory'
                    },
                    'library_preparation_protocols': {
                        'library_construction_approach': 'libraryConstructionApproach'
                    },
                    'specimens': {
                        'disease': 'specimenDisease',
                        'donor_biomaterial_id': 'donorId',
                        'genus_species': 'genusSpecies'
                    }
                }
            }

        @property
        def manifest_config(self) -> ManifestConfig:
            return {}

        @property
        def facets(self) -> Sequence[str]:
            return []

    sources_filter = {
        'constant_score': {
            'filter': {
                'terms': {
                    'sources.id.keyword': []
                }
            }
        }
    }

    def test_create_request(self):
        """
        Tests creation of a simple request
        """
        expected_output = {
            'post_filter': {
                'bool': {
                    'must': [
                        {
                            'constant_score': {
                                'filter': {
                                    'terms': {
                                        'entity_id.keyword': [
                                            'cbb998ce-ddaf-34fa-e163-d14b399c6b34'
                                        ]
                                    }
                                }
                            }
                        },
                        self.sources_filter
                    ]
                }
            }
        }
        sample_filter = {'entity_id': {'is': ['cbb998ce-ddaf-34fa-e163-d14b399c6b34']}}
        # Need to work on a couple cases:
        # - The empty case
        # - The 1 filter case
        # - The complex multiple filters case
        self._test_create_request(expected_output, sample_filter)

    def test_create_request_empty(self):
        """
        Tests creation of an empty request. That is, no filter
        """
        expected_output = {
            'query': {
                'bool': {
                    'must': [
                        self.sources_filter
                    ]
                }
            }
        }
        sample_filter = {}
        self._test_create_request(expected_output, sample_filter, post_filter=False)

    def test_create_request_missing_values(self):
        """
        Tests creation of a request for facets that do not have a value
        """
        expected_output = {
            'post_filter': {
                'bool': {
                    'must': [
                        {
                            'constant_score': {
                                'filter': {
                                    'bool': {
                                        'should': [
                                            {
                                                'terms': {
                                                    'contents.'
                                                    'library_preparation_protocols.'
                                                    'library_construction_approach.keyword': [
                                                        '~null'
                                                    ]
                                                }
                                            },
                                            {
                                                'bool': {
                                                    'must_not': [
                                                        {
                                                            'exists': {
                                                                'field': 'contents.'
                                                                         'library_preparation_protocols.'
                                                                         'library_construction_approach'
                                                            }
                                                        }
                                                    ]
                                                }
                                            }
                                        ]
                                    }
                                }
                            }
                        },
                        self.sources_filter
                    ]
                }
            }
        }
        # Create a filter for missing values
        sample_filter = {'libraryConstructionApproach': {'is': [None]}}
        self._test_create_request(expected_output, sample_filter)

    def test_create_request_terms_and_missing_values(self):
        """
        Tests creation of a request for a combination of facets that do and do
        not have a value
        """
        expected_output = {
            'post_filter': {
                'bool': {
                    'must': [
                        {
                            'constant_score': {
                                'filter': {
                                    'bool': {
                                        'should': [
                                            {
                                                'terms': {
                                                    'contents.projects.laboratory.keyword': ['~null']
                                                }
                                            },
                                            {
                                                'bool': {
                                                    'must_not': [
                                                        {
                                                            'exists': {
                                                                'field': 'contents.projects.laboratory'
                                                            }
                                                        }
                                                    ]
                                                }
                                            }
                                        ]
                                    }
                                }
                            }
                        },
                        {
                            'constant_score': {
                                'filter': {
                                    'terms': {
                                        'contents.projects.institutions.keyword': ['Hogwarts']
                                    }
                                }
                            }
                        },
                        {
                            'constant_score': {
                                'filter': {
                                    'bool': {
                                        'should': [
                                            {
                                                'terms': {
                                                    'contents.specimens.disease.keyword': ['~null', 'Dragon Pox']
                                                }
                                            },
                                            {
                                                'bool': {
                                                    'must_not': [
                                                        {
                                                            'exists': {
                                                                'field': 'contents.specimens.disease'
                                                            }
                                                        }
                                                    ]
                                                }
                                            }
                                        ]
                                    }
                                }
                            }
                        },
                        self.sources_filter
                    ]
                }
            }
        }
        # Create a filter for missing values
        sample_filter = {
            'laboratory': {'is': [None]},
            'institution': {'is': ['Hogwarts']},
            'specimenDisease': {'is': [None, 'Dragon Pox']},
        }
        self._test_create_request(expected_output, sample_filter)

    def _test_create_request(self,
                             expected_output: JSON,
                             sample_filter: JSON,
                             post_filter: bool = True
                             ):
        service = self.Service(self.MockPlugin())
        self._test_create_request_with_service(service,
                                               expected_output,
                                               sample_filter,
                                               post_filter)

    def _test_create_request_with_service(self,
                                          service: Service,
                                          expected_output: JSON,
                                          sample_filter: JSON,
                                          post_filter: bool = True
                                          ):
        filters = Filters(explicit=sample_filter, source_ids=set())
        request = self._prepare_request(filters, post_filter, service)
        actual_output = request.to_dict()
        self.assertEqual(expected_output, actual_output)

    def _prepare_request(self,
                         filters: Filters,
                         post_filter: bool,
                         service: Service
                         ) -> Search:
        entity_type = 'files'
        pipeline = service.create_chain(catalog=self.catalog,
                                        entity_type=entity_type,
                                        filters=filters,
                                        post_filter=post_filter,
                                        document_slice=None)
        pipeline = ToDictStage(service=service,
                               catalog=self.catalog,
                               entity_type=entity_type).wrap(pipeline)
        pipeline = HCAAggregationStage.create_and_wrap(pipeline)
        request = pipeline.prepare_request(service.create_request(self.catalog, entity_type))
        return request

    def test_create_aggregate(self):
        """
        Tests creation of an ES aggregate
        """
        expected_output = {
            'post_filter': {
                'bool': {
                    'must': [
                        self.sources_filter
                    ]
                }
            },
            'aggs': {
                'foo': {
                    'filter': {
                        'bool': {
                            'must': [
                                self.sources_filter
                            ]
                        }
                    },
                    'aggs': {
                        'myTerms': {
                            'terms': {
                                'field': 'path.to.foo.keyword',
                                'size': 99999
                            },
                            'meta': {
                                'path': ['path', 'to', 'foo']
                            }
                        },
                        'untagged': {
                            'missing': {
                                'field': 'path.to.foo.keyword'
                            }
                        }
                    }
                }
            }
        }

        class Service(self.Service):

            def field_types(self, catalog: CatalogName) -> FieldTypes:
                return {
                    **super().field_types(catalog),
                    'path': {'to': {'foo': null_str}}
                }

        class MockPlugin(self.MockPlugin):

            @property
            def field_mapping(self) -> Mapping[str, FieldPath]:
                return {
                    'sourceId': ('sources', 'id'),
                    'foo': ('path', 'to', 'foo')
                }

            @property
            def facets(self) -> Sequence[str]:
                return ['foo']

        service = Service(MockPlugin())
        sample_filter = {}
        self._test_create_request_with_service(service, expected_output, sample_filter)
