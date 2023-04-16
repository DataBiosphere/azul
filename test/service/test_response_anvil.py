import requests

from azul import (
    JSON,
    config,
    mutable_furl,
)
from azul.logging import (
    configure_test_logging,
)
from indexer.test_anvil import (
    AnvilIndexerTestCase,
)
from service import (
    WebServiceTestCase,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class TestAnvilResponse(AnvilIndexerTestCase, WebServiceTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    @property
    def drs_uri(self) -> mutable_furl:
        return config.tdr_service_url.set(scheme='drs')

    def test_entity_indices(self):
        self.maxDiff = None

        responses_by_entity_type = {
            'activities': {
                'hits': [
                    {
                        'entryId': '1509ef40-d1ba-440d-b298-16b7c173dcd4',
                        'sources': [
                            {
                                'sourceSpec': 'tdr:test_project:snapshot/snapshot:/2',
                                'sourceId': 'cafebabe-feed-4bad-dead-beaf8badf00d'
                            }
                        ],
                        'bundles': [
                            {
                                'bundleUuid': '826dea02-e274-affe-aabc-eb3db63ad068',
                                'bundleVersion': ''
                            }
                        ],
                        'activities': [
                            {
                                'document_id': '1509ef40-d1ba-440d-b298-16b7c173dcd4',
                                'source_datarepo_row_ids': [
                                    'sequencing:d4f6c0c4-1e11-438e-8218-cfea63b8b051'
                                ],
                                'activity_id': '18b3be87-e26b-4376-0d8d-c1e370e90e07',
                                'activity_table': 'sequencingactivity',
                                'activity_type': 'Sequencing',
                                'assay_type': [
                                    None
                                ],
                                'data_modality': [
                                    None
                                ],
                                'reference_assembly': [
                                    None
                                ],
                                'date_created': None,
                                'accessible': True
                            }
                        ],
                        'biosamples': [
                            {
                                'anatomical_site': [
                                    None
                                ],
                                'biosample_type': [
                                    None
                                ],
                                'disease': [
                                    None
                                ],
                                'donor_age_at_collection_unit': [
                                    None
                                ],
                                'donor_age_at_collection': [
                                    {
                                        'gte': None,
                                        'lte': None
                                    }
                                ]
                            }
                        ],
                        'datasets': [
                            {
                                'dataset_id': [
                                    '52ee7665-7033-63f2-a8d9-ce8e32666739'
                                ],
                                'title': [
                                    'ANVIL_CMG_UWASH_DS_BDIS'
                                ]
                            }
                        ],
                        'diagnoses': [
                            {
                                'disease': [
                                    'redacted-A61iJlLx',
                                    'redacted-g50ublm/'
                                ],
                                'diagnosis_age_unit': [
                                    None
                                ],
                                'onset_age_unit': [
                                    None
                                ],
                                'phenotype': [
                                    'redacted-acSYHZUr'
                                ],
                                'phenopacket': [
                                    None
                                ]
                            }
                        ],
                        'donors': [
                            {
                                'organism_type': [
                                    'redacted-ACw+6ecI'
                                ],
                                'phenotypic_sex': [
                                    'redacted-JfQ0b3xG'
                                ],
                                'reported_ethnicity': [
                                    'redacted-NSkwDycK'
                                ],
                                'genetic_ancestry': [
                                    None
                                ]
                            }
                        ],
                        'files': [
                            {
                                'data_modality': [
                                    None
                                ],
                                'file_format': [
                                    '.vcf.gz'
                                ],
                                'reference_assembly': [
                                    None
                                ],
                                'is_supplementary': [
                                    False
                                ],
                                'count': 1
                            }
                        ]
                    },
                    {
                        'entryId': '816e364e-1193-4e5b-a91a-14e4b009157c',
                        'sources': [
                            {
                                'sourceSpec': 'tdr:test_project:snapshot/snapshot:/2',
                                'sourceId': 'cafebabe-feed-4bad-dead-beaf8badf00d'
                            }
                        ],
                        'bundles': [
                            {
                                'bundleUuid': '826dea02-e274-affe-aabc-eb3db63ad068',
                                'bundleVersion': ''
                            }
                        ],
                        'activities': [
                            {
                                'document_id': '816e364e-1193-4e5b-a91a-14e4b009157c',
                                'source_datarepo_row_ids': [
                                    'sequencing:a6c663c7-6f26-4ed2-af9d-48e9c709a22b'
                                ],
                                'activity_id': 'a60c5138-3749-f7cb-8714-52d389ad5231',
                                'activity_table': 'sequencingactivity',
                                'activity_type': 'Sequencing',
                                'assay_type': [
                                    None
                                ],
                                'data_modality': [
                                    None
                                ],
                                'reference_assembly': [
                                    None
                                ],
                                'date_created': None,
                                'accessible': True
                            }
                        ],
                        'biosamples': [
                            {
                                'anatomical_site': [
                                    None
                                ],
                                'biosample_type': [
                                    None
                                ],
                                'disease': [
                                    None
                                ],
                                'donor_age_at_collection_unit': [
                                    None
                                ],
                                'donor_age_at_collection': [
                                    {
                                        'gte': None,
                                        'lte': None
                                    }
                                ]
                            }
                        ],
                        'datasets': [
                            {
                                'dataset_id': [
                                    '52ee7665-7033-63f2-a8d9-ce8e32666739'
                                ],
                                'title': [
                                    'ANVIL_CMG_UWASH_DS_BDIS'
                                ]
                            }
                        ],
                        'diagnoses': [
                            {
                                'disease': [
                                    'redacted-A61iJlLx',
                                    'redacted-g50ublm/'
                                ],
                                'diagnosis_age_unit': [
                                    None
                                ],
                                'onset_age_unit': [
                                    None
                                ],
                                'phenotype': [
                                    'redacted-acSYHZUr'
                                ],
                                'phenopacket': [
                                    None
                                ]
                            }
                        ],
                        'donors': [
                            {
                                'organism_type': [
                                    'redacted-ACw+6ecI'
                                ],
                                'phenotypic_sex': [
                                    'redacted-JfQ0b3xG'
                                ],
                                'reported_ethnicity': [
                                    'redacted-NSkwDycK'
                                ],
                                'genetic_ancestry': [
                                    None
                                ]
                            }
                        ],
                        'files': [
                            {
                                'data_modality': [
                                    None
                                ],
                                'file_format': [
                                    '.bam'
                                ],
                                'reference_assembly': [
                                    None
                                ],
                                'is_supplementary': [
                                    False
                                ],
                                'count': 1
                            }
                        ]
                    }
                ],
                'pagination': {
                    'count': 2,
                    'total': 2,
                    'size': 10,
                    'next': None,
                    'previous': None,
                    'pages': 1,
                    'sort': 'activities.activity_id',
                    'order': 'asc'
                },
                'termFacets': {
                    'diagnoses.phenotype': {
                        'terms': [
                            {
                                'term': 'redacted-acSYHZUr',
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'biosamples.disease': {
                        'terms': [
                            {
                                'term': None,
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'diagnoses.disease': {
                        'terms': [
                            {
                                'term': 'redacted-A61iJlLx',
                                'count': 2
                            },
                            {
                                'term': 'redacted-g50ublm/',
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'diagnoses.phenopacket': {
                        'terms': [
                            {
                                'term': None,
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'files.is_supplementary': {
                        'terms': [
                            {
                                'term': 'false',
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'files.data_modality': {
                        'terms': [
                            {
                                'term': None,
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'donors.reported_ethnicity': {
                        'terms': [
                            {
                                'term': 'redacted-NSkwDycK',
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'datasets.consent_group': {
                        'terms': [
                            {
                                'term': 'DS-BDIS',
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'activities.assay_type': {
                        'terms': [
                            {
                                'term': None,
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'datasets.title': {
                        'terms': [
                            {
                                'term': 'ANVIL_CMG_UWASH_DS_BDIS',
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'biosamples.anatomical_site': {
                        'terms': [
                            {
                                'term': None,
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'donors.organism_type': {
                        'terms': [
                            {
                                'term': 'redacted-ACw+6ecI',
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'datasets.data_use_permission': {
                        'terms': [
                            {
                                'term': 'DS-BDIS',
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'donors.phenotypic_sex': {
                        'terms': [
                            {
                                'term': 'redacted-JfQ0b3xG',
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'activities.data_modality': {
                        'terms': [
                            {
                                'term': None,
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'activities.activity_type': {
                        'terms': [
                            {
                                'term': 'Sequencing',
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'biosamples.biosample_type': {
                        'terms': [
                            {
                                'term': None,
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'files.file_format': {
                        'terms': [
                            {
                                'term': '.bam',
                                'count': 1
                            },
                            {
                                'term': '.vcf.gz',
                                'count': 1
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'datasets.registered_identifier': {
                        'terms': [
                            {
                                'term': 'phs000693',
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'files.reference_assembly': {
                        'terms': [
                            {
                                'term': None,
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    }
                }
            },
            'biosamples': {
                'hits': [
                    {
                        'entryId': '826dea02-e274-4ffe-aabc-eb3db63ad068',
                        'sources': [
                            {
                                'sourceSpec': 'tdr:test_project:snapshot/snapshot:/2',
                                'sourceId': 'cafebabe-feed-4bad-dead-beaf8badf00d'
                            }
                        ],
                        'bundles': [
                            {
                                'bundleUuid': '826dea02-e274-affe-aabc-eb3db63ad068',
                                'bundleVersion': ''
                            }
                        ],
                        'activities': [
                            {
                                'activity_type': [
                                    'Sequencing'
                                ],
                                'assay_type': [
                                    None
                                ],
                                'data_modality': [
                                    None
                                ]
                            }
                        ],
                        'biosamples': [
                            {
                                'document_id': '826dea02-e274-4ffe-aabc-eb3db63ad068',
                                'source_datarepo_row_ids': [
                                    'sample:98048c3b-2525-4090-94fd-477de31f2608'
                                ],
                                'biosample_id': 'f9d40cf6-37b8-22f3-ce35-0dc614d2452b',
                                'anatomical_site': None,
                                'apriori_cell_type': [
                                    None
                                ],
                                'biosample_type': None,
                                'disease': None,
                                'donor_age_at_collection_unit': None,
                                'donor_age_at_collection': {
                                    'gte': None,
                                    'lte': None
                                },
                                'accessible': True
                            }
                        ],
                        'datasets': [
                            {
                                'dataset_id': [
                                    '52ee7665-7033-63f2-a8d9-ce8e32666739'
                                ],
                                'title': [
                                    'ANVIL_CMG_UWASH_DS_BDIS'
                                ]
                            }
                        ],
                        'diagnoses': [
                            {
                                'disease': [
                                    'redacted-A61iJlLx',
                                    'redacted-g50ublm/'
                                ],
                                'diagnosis_age_unit': [
                                    None
                                ],
                                'onset_age_unit': [
                                    None
                                ],
                                'phenotype': [
                                    'redacted-acSYHZUr'
                                ],
                                'phenopacket': [
                                    None
                                ]
                            }
                        ],
                        'donors': [
                            {
                                'organism_type': [
                                    'redacted-ACw+6ecI'
                                ],
                                'phenotypic_sex': [
                                    'redacted-JfQ0b3xG'
                                ],
                                'reported_ethnicity': [
                                    'redacted-NSkwDycK'
                                ],
                                'genetic_ancestry': [
                                    None
                                ]
                            }
                        ],
                        'files': [
                            {
                                'data_modality': [
                                    None
                                ],
                                'file_format': [
                                    '.vcf.gz'
                                ],
                                'reference_assembly': [
                                    None
                                ],
                                'is_supplementary': [
                                    False
                                ],
                                'count': 1
                            },
                            {
                                'data_modality': [
                                    None
                                ],
                                'file_format': [
                                    '.bam'
                                ],
                                'reference_assembly': [
                                    None
                                ],
                                'is_supplementary': [
                                    False
                                ],
                                'count': 1
                            }
                        ]
                    }
                ],
                'pagination': {
                    'count': 1,
                    'total': 1,
                    'size': 10,
                    'next': None,
                    'previous': None,
                    'pages': 1,
                    'sort': 'biosamples.biosample_id',
                    'order': 'asc'
                },
                'termFacets': {
                    'diagnoses.phenotype': {
                        'terms': [
                            {
                                'term': 'redacted-acSYHZUr',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'biosamples.disease': {
                        'terms': [
                            {
                                'term': None,
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'diagnoses.disease': {
                        'terms': [
                            {
                                'term': 'redacted-A61iJlLx',
                                'count': 1
                            },
                            {
                                'term': 'redacted-g50ublm/',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'diagnoses.phenopacket': {
                        'terms': [
                            {
                                'term': None,
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'files.is_supplementary': {
                        'terms': [
                            {
                                'term': 'false',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'files.data_modality': {
                        'terms': [
                            {
                                'term': None,
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'donors.reported_ethnicity': {
                        'terms': [
                            {
                                'term': 'redacted-NSkwDycK',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'datasets.consent_group': {
                        'terms': [
                            {
                                'term': 'DS-BDIS',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'activities.assay_type': {
                        'terms': [
                            {
                                'term': None,
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'datasets.title': {
                        'terms': [
                            {
                                'term': 'ANVIL_CMG_UWASH_DS_BDIS',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'biosamples.anatomical_site': {
                        'terms': [
                            {
                                'term': None,
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'donors.organism_type': {
                        'terms': [
                            {
                                'term': 'redacted-ACw+6ecI',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'datasets.data_use_permission': {
                        'terms': [
                            {
                                'term': 'DS-BDIS',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'donors.phenotypic_sex': {
                        'terms': [
                            {
                                'term': 'redacted-JfQ0b3xG',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'activities.data_modality': {
                        'terms': [
                            {
                                'term': None,
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'activities.activity_type': {
                        'terms': [
                            {
                                'term': 'Sequencing',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'biosamples.biosample_type': {
                        'terms': [
                            {
                                'term': None,
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'files.file_format': {
                        'terms': [
                            {
                                'term': '.bam',
                                'count': 1
                            },
                            {
                                'term': '.vcf.gz',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'datasets.registered_identifier': {
                        'terms': [
                            {
                                'term': 'phs000693',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'files.reference_assembly': {
                        'terms': [
                            {
                                'term': None,
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    }
                }
            },
            'datasets': {
                'hits': [
                    {
                        'entryId': '2370f948-2783-4eb6-afea-e022897f4dcf',
                        'sources': [
                            {
                                'sourceSpec': 'tdr:test_project:snapshot/snapshot:/2',
                                'sourceId': 'cafebabe-feed-4bad-dead-beaf8badf00d'
                            }
                        ],
                        'bundles': [
                            {
                                'bundleUuid': '826dea02-e274-affe-aabc-eb3db63ad068',
                                'bundleVersion': ''
                            }
                        ],
                        'activities': [
                            {
                                'activity_type': [
                                    'Sequencing'
                                ],
                                'assay_type': [
                                    None
                                ],
                                'data_modality': [
                                    None
                                ]
                            }
                        ],
                        'biosamples': [
                            {
                                'anatomical_site': [
                                    None
                                ],
                                'biosample_type': [
                                    None
                                ],
                                'disease': [
                                    None
                                ],
                                'donor_age_at_collection_unit': [
                                    None
                                ],
                                'donor_age_at_collection': [
                                    {
                                        'gte': None,
                                        'lte': None
                                    }
                                ]
                            }
                        ],
                        'datasets': [
                            {
                                'document_id': '2370f948-2783-4eb6-afea-e022897f4dcf',
                                'source_datarepo_row_ids': [
                                    'workspace_attributes:7a22b629-9d81-4e4d-9297-f9e44ed760bc'
                                ],
                                'dataset_id': '52ee7665-7033-63f2-a8d9-ce8e32666739',
                                'consent_group': [
                                    'DS-BDIS'
                                ],
                                'data_use_permission': [
                                    'DS-BDIS'
                                ],
                                'owner': [
                                    'Debbie Nickerson'
                                ],
                                'principal_investigator': [
                                    None
                                ],
                                'registered_identifier': [
                                    'phs000693'
                                ],
                                'title': 'ANVIL_CMG_UWASH_DS_BDIS',
                                'data_modality': [
                                    None
                                ],
                                'accessible': True
                            }
                        ],
                        'diagnoses': [
                            {
                                'disease': [
                                    'redacted-A61iJlLx',
                                    'redacted-g50ublm/'
                                ],
                                'diagnosis_age_unit': [
                                    None
                                ],
                                'onset_age_unit': [
                                    None
                                ],
                                'phenotype': [
                                    'redacted-acSYHZUr'
                                ],
                                'phenopacket': [
                                    None
                                ]
                            }
                        ],
                        'donors': [
                            {
                                'organism_type': [
                                    'redacted-ACw+6ecI'
                                ],
                                'phenotypic_sex': [
                                    'redacted-JfQ0b3xG'
                                ],
                                'reported_ethnicity': [
                                    'redacted-NSkwDycK'
                                ],
                                'genetic_ancestry': [
                                    None
                                ]
                            }
                        ],
                        'files': [
                            {
                                'data_modality': [
                                    None
                                ],
                                'file_format': [
                                    '.vcf.gz'
                                ],
                                'reference_assembly': [
                                    None
                                ],
                                'is_supplementary': [
                                    False
                                ],
                                'count': 1
                            },
                            {
                                'data_modality': [
                                    None
                                ],
                                'file_format': [
                                    '.bam'
                                ],
                                'reference_assembly': [
                                    None
                                ],
                                'is_supplementary': [
                                    False
                                ],
                                'count': 1
                            }
                        ]
                    }
                ],
                'pagination': {
                    'count': 1,
                    'total': 1,
                    'size': 10,
                    'next': None,
                    'previous': None,
                    'pages': 1,
                    'sort': 'datasets.dataset_id',
                    'order': 'asc'
                },
                'termFacets': {
                    'diagnoses.phenotype': {
                        'terms': [
                            {
                                'term': 'redacted-acSYHZUr',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'biosamples.disease': {
                        'terms': [
                            {
                                'term': None,
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'diagnoses.disease': {
                        'terms': [
                            {
                                'term': 'redacted-A61iJlLx',
                                'count': 1
                            },
                            {
                                'term': 'redacted-g50ublm/',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'diagnoses.phenopacket': {
                        'terms': [
                            {
                                'term': None,
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'files.is_supplementary': {
                        'terms': [
                            {
                                'term': 'false',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'files.data_modality': {
                        'terms': [
                            {
                                'term': None,
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'donors.reported_ethnicity': {
                        'terms': [
                            {
                                'term': 'redacted-NSkwDycK',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'datasets.consent_group': {
                        'terms': [
                            {
                                'term': 'DS-BDIS',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'activities.assay_type': {
                        'terms': [
                            {
                                'term': None,
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'datasets.title': {
                        'terms': [
                            {
                                'term': 'ANVIL_CMG_UWASH_DS_BDIS',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'biosamples.anatomical_site': {
                        'terms': [
                            {
                                'term': None,
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'donors.organism_type': {
                        'terms': [
                            {
                                'term': 'redacted-ACw+6ecI',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'datasets.data_use_permission': {
                        'terms': [
                            {
                                'term': 'DS-BDIS',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'donors.phenotypic_sex': {
                        'terms': [
                            {
                                'term': 'redacted-JfQ0b3xG',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'activities.data_modality': {
                        'terms': [
                            {
                                'term': None,
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'activities.activity_type': {
                        'terms': [
                            {
                                'term': 'Sequencing',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'biosamples.biosample_type': {
                        'terms': [
                            {
                                'term': None,
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'files.file_format': {
                        'terms': [
                            {
                                'term': '.bam',
                                'count': 1
                            },
                            {
                                'term': '.vcf.gz',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'datasets.registered_identifier': {
                        'terms': [
                            {
                                'term': 'phs000693',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'files.reference_assembly': {
                        'terms': [
                            {
                                'term': None,
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    }
                }
            },
            'donors': {
                'hits': [
                    {
                        'entryId': 'bfd991f2-2797-4083-972a-da7c6d7f1b2e',
                        'sources': [
                            {
                                'sourceSpec': 'tdr:test_project:snapshot/snapshot:/2',
                                'sourceId': 'cafebabe-feed-4bad-dead-beaf8badf00d'
                            }
                        ],
                        'bundles': [
                            {
                                'bundleUuid': '826dea02-e274-affe-aabc-eb3db63ad068',
                                'bundleVersion': ''
                            }
                        ],
                        'activities': [
                            {
                                'activity_type': [
                                    'Sequencing'
                                ],
                                'assay_type': [
                                    None
                                ],
                                'data_modality': [
                                    None
                                ]
                            }
                        ],
                        'biosamples': [
                            {
                                'anatomical_site': [
                                    None
                                ],
                                'biosample_type': [
                                    None
                                ],
                                'disease': [
                                    None
                                ],
                                'donor_age_at_collection_unit': [
                                    None
                                ],
                                'donor_age_at_collection': [
                                    {
                                        'gte': None,
                                        'lte': None
                                    }
                                ]
                            }
                        ],
                        'datasets': [
                            {
                                'dataset_id': [
                                    '52ee7665-7033-63f2-a8d9-ce8e32666739'
                                ],
                                'title': [
                                    'ANVIL_CMG_UWASH_DS_BDIS'
                                ]
                            }
                        ],
                        'diagnoses': [
                            {
                                'disease': [
                                    'redacted-A61iJlLx',
                                    'redacted-g50ublm/'
                                ],
                                'diagnosis_age_unit': [
                                    None
                                ],
                                'diagnosis_age': [
                                    {
                                        'gte': None,
                                        'lte': None
                                    }
                                ],
                                'onset_age_unit': [
                                    None
                                ],
                                'onset_age': [
                                    {
                                        'gte': None,
                                        'lte': None
                                    }
                                ],
                                'phenotype': [
                                    'redacted-acSYHZUr'
                                ],
                                'phenopacket': [
                                    None
                                ]
                            }
                        ],
                        'donors': [
                            {
                                'document_id': 'bfd991f2-2797-4083-972a-da7c6d7f1b2e',
                                'source_datarepo_row_ids': [
                                    'subject:c23887a0-20c1-44e4-a09e-1c5dfdc2d0ef'
                                ],
                                'donor_id': '1e2bd7e5-f45e-a391-daea-7c060be76acd',
                                'organism_type': 'redacted-ACw+6ecI',
                                'phenotypic_sex': 'redacted-JfQ0b3xG',
                                'reported_ethnicity': [
                                    'redacted-NSkwDycK'
                                ],
                                'genetic_ancestry': [
                                    None
                                ],
                                'accessible': True
                            }
                        ],
                        'files': [
                            {
                                'data_modality': [
                                    None
                                ],
                                'file_format': [
                                    '.vcf.gz'
                                ],
                                'reference_assembly': [
                                    None
                                ],
                                'is_supplementary': [
                                    False
                                ],
                                'count': 1
                            },
                            {
                                'data_modality': [
                                    None
                                ],
                                'file_format': [
                                    '.bam'
                                ],
                                'reference_assembly': [
                                    None
                                ],
                                'is_supplementary': [
                                    False
                                ],
                                'count': 1
                            }
                        ]
                    }
                ],
                'pagination': {
                    'count': 1,
                    'total': 1,
                    'size': 10,
                    'next': None,
                    'previous': None,
                    'pages': 1,
                    'sort': 'donors.donor_id',
                    'order': 'asc'
                },
                'termFacets': {
                    'diagnoses.phenotype': {
                        'terms': [
                            {
                                'term': 'redacted-acSYHZUr',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'biosamples.disease': {
                        'terms': [
                            {
                                'term': None,
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'diagnoses.disease': {
                        'terms': [
                            {
                                'term': 'redacted-A61iJlLx',
                                'count': 1
                            },
                            {
                                'term': 'redacted-g50ublm/',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'diagnoses.phenopacket': {
                        'terms': [
                            {
                                'term': None,
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'files.is_supplementary': {
                        'terms': [
                            {
                                'term': 'false',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'files.data_modality': {
                        'terms': [
                            {
                                'term': None,
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'donors.reported_ethnicity': {
                        'terms': [
                            {
                                'term': 'redacted-NSkwDycK',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'datasets.consent_group': {
                        'terms': [
                            {
                                'term': 'DS-BDIS',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'activities.assay_type': {
                        'terms': [
                            {
                                'term': None,
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'datasets.title': {
                        'terms': [
                            {
                                'term': 'ANVIL_CMG_UWASH_DS_BDIS',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'biosamples.anatomical_site': {
                        'terms': [
                            {
                                'term': None,
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'donors.organism_type': {
                        'terms': [
                            {
                                'term': 'redacted-ACw+6ecI',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'datasets.data_use_permission': {
                        'terms': [
                            {
                                'term': 'DS-BDIS',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'donors.phenotypic_sex': {
                        'terms': [
                            {
                                'term': 'redacted-JfQ0b3xG',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'activities.data_modality': {
                        'terms': [
                            {
                                'term': None,
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'activities.activity_type': {
                        'terms': [
                            {
                                'term': 'Sequencing',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'biosamples.biosample_type': {
                        'terms': [
                            {
                                'term': None,
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'files.file_format': {
                        'terms': [
                            {
                                'term': '.bam',
                                'count': 1
                            },
                            {
                                'term': '.vcf.gz',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'datasets.registered_identifier': {
                        'terms': [
                            {
                                'term': 'phs000693',
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    },
                    'files.reference_assembly': {
                        'terms': [
                            {
                                'term': None,
                                'count': 1
                            }
                        ],
                        'total': 1,
                        'type': 'terms'
                    }
                }
            },
            'files': {
                'hits': [
                    {
                        'entryId': '15b76f9c-6b46-433f-851d-34e89f1b9ba6',
                        'sources': [
                            {
                                'sourceSpec': 'tdr:test_project:snapshot/snapshot:/2',
                                'sourceId': 'cafebabe-feed-4bad-dead-beaf8badf00d'
                            }
                        ],
                        'bundles': [
                            {
                                'bundleUuid': '826dea02-e274-affe-aabc-eb3db63ad068',
                                'bundleVersion': ''
                            }
                        ],
                        'activities': [
                            {
                                'activity_type': [
                                    'Sequencing'
                                ],
                                'assay_type': [
                                    None
                                ],
                                'data_modality': [
                                    None
                                ]
                            }
                        ],
                        'biosamples': [
                            {
                                'anatomical_site': [
                                    None
                                ],
                                'biosample_type': [
                                    None
                                ],
                                'disease': [
                                    None
                                ],
                                'donor_age_at_collection_unit': [
                                    None
                                ],
                                'donor_age_at_collection': [
                                    {
                                        'gte': None,
                                        'lte': None
                                    }
                                ]
                            }
                        ],
                        'datasets': [
                            {
                                'dataset_id': [
                                    '52ee7665-7033-63f2-a8d9-ce8e32666739'
                                ],
                                'title': [
                                    'ANVIL_CMG_UWASH_DS_BDIS'
                                ]
                            }
                        ],
                        'diagnoses': [
                            {
                                'disease': [
                                    'redacted-A61iJlLx',
                                    'redacted-g50ublm/'
                                ],
                                'diagnosis_age_unit': [
                                    None
                                ],
                                'onset_age_unit': [
                                    None
                                ],
                                'phenotype': [
                                    'redacted-acSYHZUr'
                                ],
                                'phenopacket': [
                                    None
                                ]
                            }
                        ],
                        'donors': [
                            {
                                'organism_type': [
                                    'redacted-ACw+6ecI'
                                ],
                                'phenotypic_sex': [
                                    'redacted-JfQ0b3xG'
                                ],
                                'reported_ethnicity': [
                                    'redacted-NSkwDycK'
                                ],
                                'genetic_ancestry': [
                                    None
                                ]
                            }
                        ],
                        'files': [
                            {
                                'document_id': '15b76f9c-6b46-433f-851d-34e89f1b9ba6',
                                'source_datarepo_row_ids': [
                                    'file_inventory:81d16471-97ac-48fe-99a0-73d9ec62c2c0'
                                ],
                                'file_id': '1e269f04-4347-4188-b060-1dcc69e71d67',
                                'data_modality': [
                                    None
                                ],
                                'file_format': '.vcf.gz',
                                'file_size': 213021639,
                                'file_md5sum': 'vuxgbuCqKZ/fkT9CWTFmIg==',
                                'reference_assembly': [
                                    None
                                ],
                                'file_name': '307500.merged.matefixed.sorted.markeddups.recal.g.vcf.gz',
                                'is_supplementary': False,
                                'version': '2022-06-01T00:00:00.000000Z',
                                'uuid': '15b76f9c-6b46-433f-851d-34e89f1b9ba6',
                                'size': 213021639,
                                'name': '307500.merged.matefixed.sorted.markeddups.recal.g.vcf.gz',
                                'crc32': '',
                                'sha256': '',
                                'accessible': True,
                                'drs_uri': str(self.drs_uri.add(
                                    path='v1_2ae00e5c-4aef-4a1e-9eca-d8d0747b5348_1e269f04-4347-4188-b060-1dcc69e71d67'
                                )),
                                'url': str(self.base_url.set(
                                    path='/repository/files/15b76f9c-6b46-433f-851d-34e89f1b9ba6',
                                    args=dict(catalog='test', version='2022-06-01T00:00:00.000000Z')
                                ))
                            }
                        ]
                    },
                    {
                        'entryId': '3b17377b-16b1-431c-9967-e5d01fc5923f',
                        'sources': [
                            {
                                'sourceSpec': 'tdr:test_project:snapshot/snapshot:/2',
                                'sourceId': 'cafebabe-feed-4bad-dead-beaf8badf00d'
                            }
                        ],
                        'bundles': [
                            {
                                'bundleUuid': '826dea02-e274-affe-aabc-eb3db63ad068',
                                'bundleVersion': ''
                            }
                        ],
                        'activities': [
                            {
                                'activity_type': [
                                    'Sequencing'
                                ],
                                'assay_type': [
                                    None
                                ],
                                'data_modality': [
                                    None
                                ]
                            }
                        ],
                        'biosamples': [
                            {
                                'anatomical_site': [
                                    None
                                ],
                                'biosample_type': [
                                    None
                                ],
                                'disease': [
                                    None
                                ],
                                'donor_age_at_collection_unit': [
                                    None
                                ],
                                'donor_age_at_collection': [
                                    {
                                        'gte': None,
                                        'lte': None
                                    }
                                ]
                            }
                        ],
                        'datasets': [
                            {
                                'dataset_id': [
                                    '52ee7665-7033-63f2-a8d9-ce8e32666739'
                                ],
                                'title': [
                                    'ANVIL_CMG_UWASH_DS_BDIS'
                                ]
                            }
                        ],
                        'diagnoses': [
                            {
                                'disease': [
                                    'redacted-A61iJlLx',
                                    'redacted-g50ublm/'
                                ],
                                'diagnosis_age_unit': [
                                    None
                                ],
                                'onset_age_unit': [
                                    None
                                ],
                                'phenotype': [
                                    'redacted-acSYHZUr'
                                ],
                                'phenopacket': [
                                    None
                                ]
                            }
                        ],
                        'donors': [
                            {
                                'organism_type': [
                                    'redacted-ACw+6ecI'
                                ],
                                'phenotypic_sex': [
                                    'redacted-JfQ0b3xG'
                                ],
                                'reported_ethnicity': [
                                    'redacted-NSkwDycK'
                                ],
                                'genetic_ancestry': [
                                    None
                                ]
                            }
                        ],
                        'files': [
                            {
                                'document_id': '3b17377b-16b1-431c-9967-e5d01fc5923f',
                                'source_datarepo_row_ids': [
                                    'file_inventory:9658d94a-511d-4b49-82c3-d0cb07e0cff2'
                                ],
                                'file_id': '8b722e88-8103-49c1-b351-e64fa7c6ab37',
                                'data_modality': [
                                    None
                                ],
                                'file_format': '.bam',
                                'file_size': 3306845592,
                                'file_md5sum': 'fNn9e1SovzgOROk3BvH6LQ==',
                                'reference_assembly': [
                                    None
                                ],
                                'file_name': '307500.merged.matefixed.sorted.markeddups.recal.bam',
                                'is_supplementary': False,
                                'version': '2022-06-01T00:00:00.000000Z',
                                'uuid': '3b17377b-16b1-431c-9967-e5d01fc5923f',
                                'size': 3306845592,
                                'name': '307500.merged.matefixed.sorted.markeddups.recal.bam',
                                'crc32': '',
                                'sha256': '',
                                'accessible': True,
                                'drs_uri': str(self.drs_uri.add(
                                    path='v1_2ae00e5c-4aef-4a1e-9eca-d8d0747b5348_8b722e88-8103-49c1-b351-e64fa7c6ab37'
                                )),
                                'url': str(self.base_url.set(
                                    path='/repository/files/3b17377b-16b1-431c-9967-e5d01fc5923f',
                                    args=dict(catalog='test', version='2022-06-01T00:00:00.000000Z')
                                ))
                            }
                        ]
                    }
                ],
                'pagination': {
                    'count': 2,
                    'total': 2,
                    'size': 10,
                    'next': None,
                    'previous': None,
                    'pages': 1,
                    'sort': 'files.file_id',
                    'order': 'asc'
                },
                'termFacets': {
                    'diagnoses.phenotype': {
                        'terms': [
                            {
                                'term': 'redacted-acSYHZUr',
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'biosamples.disease': {
                        'terms': [
                            {
                                'term': None,
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'diagnoses.disease': {
                        'terms': [
                            {
                                'term': 'redacted-A61iJlLx',
                                'count': 2
                            },
                            {
                                'term': 'redacted-g50ublm/',
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'diagnoses.phenopacket': {
                        'terms': [
                            {
                                'term': None,
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'files.is_supplementary': {
                        'terms': [
                            {
                                'term': 'false',
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'files.data_modality': {
                        'terms': [
                            {
                                'term': None,
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'donors.reported_ethnicity': {
                        'terms': [
                            {
                                'term': 'redacted-NSkwDycK',
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'datasets.consent_group': {
                        'terms': [
                            {
                                'term': 'DS-BDIS',
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'activities.assay_type': {
                        'terms': [
                            {
                                'term': None,
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'datasets.title': {
                        'terms': [
                            {
                                'term': 'ANVIL_CMG_UWASH_DS_BDIS',
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'biosamples.anatomical_site': {
                        'terms': [
                            {
                                'term': None,
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'donors.organism_type': {
                        'terms': [
                            {
                                'term': 'redacted-ACw+6ecI',
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'datasets.data_use_permission': {
                        'terms': [
                            {
                                'term': 'DS-BDIS',
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'donors.phenotypic_sex': {
                        'terms': [
                            {
                                'term': 'redacted-JfQ0b3xG',
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'activities.data_modality': {
                        'terms': [
                            {
                                'term': None,
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'activities.activity_type': {
                        'terms': [
                            {
                                'term': 'Sequencing',
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'biosamples.biosample_type': {
                        'terms': [
                            {
                                'term': None,
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'files.file_format': {
                        'terms': [
                            {
                                'term': '.bam',
                                'count': 1
                            },
                            {
                                'term': '.vcf.gz',
                                'count': 1
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'datasets.registered_identifier': {
                        'terms': [
                            {
                                'term': 'phs000693',
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    },
                    'files.reference_assembly': {
                        'terms': [
                            {
                                'term': None,
                                'count': 2
                            }
                        ],
                        'total': 2,
                        'type': 'terms'
                    }
                }
            }
        }
        for entity_type, expected_response in responses_by_entity_type.items():
            with self.subTest(entity_type=entity_type):
                url = str(self.base_url.set(path='/index/' + entity_type))
                self._assertResponse(url, expected_response)

    def test_summary(self):
        expected_response = {
            'activityCount': 2,
            'activityTypes': [
                {
                    'count': 2,
                    'type': 'Sequencing'
                }
            ],
            'biosampleCount': 1,
            'datasetCount': 1,
            'donorCount': 1,
            'donorDiagnosisDiseases': [
                {
                    'count': 1,
                    'disease': 'redacted-A61iJlLx'
                },
                {
                    'count': 1,
                    'disease': 'redacted-g50ublm/'
                }
            ],
            'donorDiagnosisPhenotypes': [
                {
                    'count': 1,
                    'phenotype': 'redacted-acSYHZUr'
                }
            ],
            'donorSpecies': [
                {
                    'count': 1,
                    'species': 'redacted-ACw+6ecI'
                }
            ],
            'fileCount': 2,
            'fileFormats': [
                {
                    'count': 1,
                    'format': '.bam'
                },
                {
                    'count': 1,
                    'format': '.vcf.gz'
                }
            ]
        }
        url = str(self.base_url.set(path='/index/summary'))
        self._assertResponse(url, expected_response)

    def _assertResponse(self, url: str, expected_response: JSON):
        response = requests.get(url)
        response.raise_for_status()
        response = response.json()
        self.assertEqual(expected_response, response)
