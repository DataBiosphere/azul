from typing import (
    Sequence,
    Type,
)
from urllib.parse import quote

from azul.indexer import BaseIndexer
import azul.plugin
from azul.project.hca.indexer import Indexer
from azul.types import JSON


class Plugin(azul.plugin.Plugin):

    def indexer_class(self) -> Type[BaseIndexer]:
        return Indexer

    def dss_subscription_query(self, prefix: str) -> JSON:
        return {
            "query": {
                "bool": {
                    "must_not": [
                        {
                            "term": {
                                "admin_deleted": True
                            }
                        }
                    ],
                    "must": [
                        {
                            "exists": {
                                "field": "files.project_json"
                            }
                        },
                        *self._prefix_clause(prefix)
                    ]
                }
            }
        }

    def dss_deletion_subscription_query(self, prefix: str) -> JSON:
        return {
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "admin_deleted": True
                            }
                        },
                        *self._prefix_clause(prefix)
                    ]
                }
            }
        }

    def _prefix_clause(self, prefix):
        return [
            {
                'prefix': {
                    'uuid': prefix
                }
            }
        ] if prefix else []

    def service_config(self) -> azul.plugin.ServiceConfig:
        return azul.plugin.ServiceConfig(
            translation={
                "fileFormat": "contents.files.file_format",
                "fileName": "contents.files.name",
                "fileSize": "contents.files.size",
                "fileId": "contents.files.uuid",
                "fileVersion": "contents.files.version",

                "instrumentManufacturerModel": "contents.protocols.instrument_manufacturer_model",
                "libraryConstructionApproach": "contents.protocols.library_construction_approach",
                "pairedEnd": "contents.protocols.paired_end",
                "workflow": "contents.protocols.workflow",
                "assayType": "contents.protocols.assay_type",

                "contactName": "contents.projects.contact_names",
                "projectId": "contents.projects.document_id",
                "institution": "contents.projects.institutions",
                "laboratory": "contents.projects.laboratory",
                "projectDescription": "contents.projects.project_description",
                "project": "contents.projects.project_short_name",
                "projectTitle": "contents.projects.project_title",
                "publicationTitle": "contents.projects.publication_titles",
                "arrayExpressAccessions": "contents.projects.array_express_accessions",
                "geoSeriesAccessions": "contents.projects.geo_series_accessions",
                "insdcProjectAccessions": "contents.projects.insdc_project_accessions",
                "insdcStudyAccessions": "contents.projects.insdc_study_accessions",

                "biologicalSex": "contents.donors.biological_sex",
                "sampleId": "contents.samples.biomaterial_id",
                "sampleEntityType": "contents.samples.entity_type",
                "disease": "contents.samples.disease",
                "genusSpecies": "contents.donors.genus_species",
                "donorDisease": "contents.donors.diseases",
                "organ": "contents.samples.organ",
                "organPart": "contents.samples.organ_part",
                "modelOrgan": "contents.samples.model_organ",
                "modelOrganPart": "contents.samples.model_organ_part",
                "effectiveOrgan": "contents.samples.effective_organ",
                "specimenOrgan": "contents.specimens.organ",
                "specimenOrganPart": "contents.specimens.organ_part",
                "organismAge": "contents.donors.organism_age",
                "organismAgeUnit": "contents.donors.organism_age_unit",
                "organismAgeRange": "contents.donors.organism_age_range",
                "preservationMethod": "contents.specimens.preservation_method",

                "cellLineType": "contents.cell_lines.cell_line_type",

                "cellCount": "contents.cell_suspensions.total_estimated_cells",
                "donorCount": "contents.donors.donor_count",
                "selectedCellType": "contents.cell_suspensions.selected_cell_type",

                "bundleUuid": "bundles.uuid",
                "bundleVersion": "bundles.version",

                "entryId": "entity_id"
            },
            autocomplete_translation={
                "files": {
                    "entity_id": "entity_id",
                    "entity_version": "entity_version"
                },
                "donor": {
                    "donor": "donor_uuid"
                }
            },
            manifest={
                "bundles": {
                    "bundle_uuid": "uuid",
                    "bundle_version": "version"
                },
                "contents.files": {
                    "file_name": "name",
                    "file_format": "file_format",
                    "read_index": "read_index",
                    "file_size": "size",
                    "file_uuid": "uuid",
                    "file_version": "version",
                    "file_sha256": "sha256",
                    "file_content_type": "content-type"
                },
                "contents.cell_suspensions": {
                    "cell_suspension.provenance.document_id": "document_id",
                    "cell_suspension.estimated_cell_count": "total_estimated_cells",
                    "cell_suspension.selected_cell_type": "selected_cell_type"
                },
                "contents.protocols": {
                    "sequencing_protocol.instrument_manufacturer_model": "instrument_manufacturer_model",
                    "sequencing_protocol.paired_end": "paired_end",
                    "library_preparation_protocol.library_construction_approach": "library_construction_approach"
                },
                "contents.projects": {
                    "project.provenance.document_id": "document_id",
                    "project.contributors.institution": "institutions",
                    "project.contributors.laboratory": "laboratory",
                    "project.project_core.project_short_name": "project_short_name",
                    "project.project_core.project_title": "project_title"
                },
                "contents.specimens": {
                    "specimen_from_organism.provenance.document_id": "document_id",
                    "specimen_from_organism.diseases": "disease",
                    "specimen_from_organism.organ": "organ",
                    "specimen_from_organism.organ_part": "organ_part",
                    "specimen_from_organism.preservation_storage.preservation_method": "preservation_method"
                },
                "contents.donors": {
                    "donor_organism.sex": "biological_sex",
                    "donor_organism.biomaterial_core.biomaterial_id": "biomaterial_id",
                    "donor_organism.provenance.document_id": "document_id",
                    "donor_organism.genus_species": "genus_species",
                    "donor_organism.diseases": "diseases",
                    "donor_organism.organism_age": "organism_age",
                    "donor_organism.organism_age_unit": "organism_age_unit"
                },
                "contents.cell_lines": {
                    "cell_line.provenance.document_id": "document_id",
                    "cell_line.biomaterial_core.biomaterial_id": "biomaterial_id"
                },
                "contents.organoids": {
                    "organoid.provenance.document_id": "document_id",
                    "organoid.biomaterial_core.biomaterial_id": "biomaterial_id",
                    "organoid.model_organ": "model_organ",
                    "organoid.model_organ_part": "model_organ_part"
                },
                "contents.samples": {
                    "_entity_type": "entity_type",
                    "sample.provenance.document_id": "document_id",
                    "sample.biomaterial_core.biomaterial_id": "biomaterial_id"
                }
            },
            cart_item={
                "files": [
                    "contents.files.uuid",
                    "contents.files.version"
                ],
                "samples": [
                    "contents.samples.document_id",
                    "contents.samples.entity_type"
                ],
                "projects": [
                    "contents.projects.project_short_name"
                ],
                "bundles": [
                    "bundles.uuid",
                    "bundles.version"
                ]
            },
            facets=[
                "organ",
                "organPart",
                "modelOrgan",
                "modelOrganPart",
                "effectiveOrgan",
                "specimenOrgan",
                "specimenOrganPart",
                "sampleEntityType",
                "libraryConstructionApproach",
                "genusSpecies",
                "organismAge",
                "organismAgeUnit",
                "biologicalSex",
                "disease",
                "donorDisease",
                "instrumentManufacturerModel",
                "pairedEnd",
                "workflow",
                "assayType",
                "project",
                "fileFormat",
                "laboratory",
                "preservationMethod",
                "projectTitle",
                "cellLineType",
                "selectedCellType",
                "projectDescription",
                "institution",
                "contactName",
                "publicationTitle"
            ],
            autocomplete_mapping_config={
                "file": {
                    "dataType": "file_type",
                    "donorId": [
                        "donor"
                    ],
                    "fileBundleId": "repoDataBundleId",
                    "fileName": [
                        "title"
                    ],
                    "id": "file_id",
                    "projectCode": [
                        "project"
                    ]
                },
                "donor": {
                    "id": "donor_uuid"
                },
                "file-donor": {
                    "id": "donor_uuid"
                }
            },
            order_config=[
                "organ",
                "organPart",
                "biologicalSex",
                "genusSpecies",
                "protocol"
            ]
        )

    def portal_integrations_db(self) -> Sequence[JSON]:
        """
        A hardcoded example database for use during development of the integrations API implementation
        """
        return [
            {
                "portal_id": "7bc432a6-0fcf-4c19-b6e6-4cb6231279b3",
                "portal_name": "Terra Portal",
                "portal_icon": "https://app.terra.bio/static/media/logo.c5ed3676.svg",
                "contact_email": "",
                "organization_name": "Broad Institute",
                "portal_description": "Terra is a cloud-native platform for biomedical researchers to access data, "
                                      "run analysis tools, and collaborate.",
                "integrations": [
                    {
                        "integration_id": "b87b7f30-2e60-4ca5-9a6f-00ebfcd35f35",
                        "integration_type": "get_manifest",
                        "entity_type": "file",
                        "title": "Populate a Terra workspace with data files matching the current filter selection",
                        "manifest_type": "full",
                        "portal_url_template": "https://app.terra.bio/#import-data?url={manifest_url}"
                    }
                ]
            },
            {
                "portal_id": "9852dece-443d-42e8-869c-17b9a86d447e",
                "portal_name": "Single Cell Portal",
                "portal_icon": "https://singlecell.broadinstitute.org/single_cell/assets/"
                               "SCP-logo-5d31abc1f355f68c809a100d74c886af016bd0aba246bcfad92f0b50b5ce2cd8.png",
                "contact_email": "",
                "organization_name": "Broad Institute",
                "portal_description": "Reducing barriers and accelerating single-cell research.",
                "integrations": [
                    {
                        "integration_id": "977854a0-2eea-4fec-9459-d4807fe79f0c",
                        "integration_type": "get",
                        "entity_type": "project",
                        "title": "Visualize in SCP",
                        "entity_ids": {
                            "dev": ["bc2229e7-e330-435a-8c2a-4275741f2c2d"],
                            "staging": ["bc2229e7-e330-435a-8c2a-4275741f2c2d", "259f9041-b72f-45ce-894d-b645add2e620"],
                            "integration": ["bc2229e7-e330-435a-8c2a-4275741f2c2d"],
                            "prod": ["c4077b3c-5c98-4d26-a614-246d12c2e5d7"]
                        },
                        "portal_url": "https://singlecell.broadinstitute.org/single_cell/study/SCP495"
                    },
                    # https://docs.google.com/document/d/1HBOPe6h_RjxltfbPenKsNmoN3MVAtkhVKJ_LGG1DBkA/edit#
                    # https://github.com/HumanCellAtlas/data-browser/issues/545#issuecomment-528092658
                    # {
                    #     "integration_id": "f62f5202-55c3-4dfa-bedd-ba4d2c4fb6c9",
                    #     "integration_type": "get_entity",
                    #     "entity_type": "project",
                    #     "title": "",
                    #     "allow_head": False,
                    #     "portal_url_template": "https://singlecell.broadinstitute.org/hca-project/{entity_id}"
                    # }
                ]
            },
            {
                "portal_id": "f58bdc5e-98cd-4df4-80a4-7372dc035e87",
                "portal_name": "Single Cell Expression Atlas",
                "portal_icon": "https://www.ebi.ac.uk/gxa/sc/resources/images/logos/sc_atlas_logo.png",
                "contact_email": "Irene Papatheodorou irenep@ebi.ac.uk",
                "organization_name": "European Bioinformatics Institute",
                "portal_description": "Single Cell Expression Atlas annotates publicly available single cell "
                                      "RNA-Seq experiments with ontology identifiers and re-analyses them using "
                                      "standardised pipelines available through SCXA-Workflows, our collection of "
                                      "RNA-Seq analysis pipelines, which is available at "
                                      "https://github.com/ebi-gene-expression-group/scxa-workflows . The browser "
                                      "enables visualisation of clusters of cells, their annotations and supports "
                                      "searches for gene expression within and across studies.",
                "integrations": [
                    {
                        "integration_id": "dbfe9394-a326-4574-9632-fbadb51a7b1a",
                        "integration_type": "get",
                        "entity_type": "project",
                        "title": "Single-cell transcriptome analysis of precursors of human CD4+ "
                                 "cytotoxic T lymphocytes",
                        "entity_ids": {
                            "staging": ["519b58ef-6462-4ed3-8c0d-375b54f53c31"],
                            "integration": ["90bd6933-40c0-48d4-8d76-778c103bf545"],
                            "prod": ["90bd6933-40c0-48d4-8d76-778c103bf545"]
                        },
                        "portal_url": "https://www.ebi.ac.uk/gxa/sc/experiments/E-GEOD-106540/results/tsne"
                    },
                    {
                        "integration_id": "081a6a90-29b6-4100-9c42-17a50014ea03",
                        "integration_type": "get",
                        "entity_type": "project",
                        "title": "Reconstructing the human first trimester fetal-maternal interface using single cell "
                                 "transcriptomics - 10x data",
                        "entity_ids": {
                            "staging": [],
                            "integration": ["f83165c5-e2ea-4d15-a5cf-33f3550bffde"],
                            "prod": ["f83165c5-e2ea-4d15-a5cf-33f3550bffde"]
                        },
                        "portal_url": "https://www.ebi.ac.uk/gxa/sc/experiments/E-MTAB-6701/results/tsne"
                    },
                    {
                        "integration_id": "f0886c45-e339-4f22-8f6b-a715db1943e3",
                        "integration_type": "get",
                        "entity_type": "project",
                        "title": "Reconstructing the human first trimester fetal-maternal interface using single cell "
                                 "transcriptomics - Smartseq 2 data",
                        "entity_ids": {
                            "staging": [],
                            "integration": ["f83165c5-e2ea-4d15-a5cf-33f3550bffde"],
                            "prod": ["f83165c5-e2ea-4d15-a5cf-33f3550bffde"]
                        },
                        "portal_url": "https://www.ebi.ac.uk/gxa/sc/experiments/E-MTAB-6678/results/tsne"
                    },
                    {
                        "integration_id": "f13ddf2d-d913-492b-9ea8-2de4b1881c26",
                        "integration_type": "get",
                        "entity_type": "project",
                        "title": "Single cell transcriptome analysis of human pancreas",
                        "entity_ids": {
                            "staging": ["b1f3afcb-f061-4862-b6c2-ace971595d22", "08e7b6ba-5825-47e9-be2d-7978533c5f8c"],
                            "integration": ["cddab57b-6868-4be4-806f-395ed9dd635a"],
                            "prod": ["cddab57b-6868-4be4-806f-395ed9dd635a"]
                        },
                        "portal_url": "https://www.ebi.ac.uk/gxa/sc/experiments/E-GEOD-81547/results/tsne"
                    },
                    {
                        "integration_id": "5ef44133-e71f-4f52-893b-3b200d5fb99b",
                        "integration_type": "get",
                        "entity_type": "project",
                        "title": "Single-cell RNA-seq analysis of 1,732 cells throughout a 125-day differentiation "
                                 "protocol that converted H1 human embryonic stem cells to a variety of "
                                 "ventrally-derived cell types",
                        "entity_ids": {
                            "staging": ["019a935b-ea35-4d83-be75-e1a688179328"],
                            "integration": ["2043c65a-1cf8-4828-a656-9e247d4e64f1"],
                            "prod": ["2043c65a-1cf8-4828-a656-9e247d4e64f1"]
                        },
                        "portal_url": "https://www.ebi.ac.uk/gxa/sc/experiments/E-GEOD-93593/results/tsne"
                    },
                    {
                        "integration_id": "d43464c0-38c6-402d-bdec-8972d71005c5 ",
                        "integration_type": "get",
                        "entity_type": "project",
                        "title": "Single-cell RNA-seq analysis of human pancreas from healthy individuals and type 2 "
                                 "diabetes patients",
                        "entity_ids": {
                            "staging": ["a5ae0428-476c-46d2-a9f2-aad955b149aa"],
                            "integration": ["ae71be1d-ddd8-4feb-9bed-24c3ddb6e1ad"],
                            "prod": ["ae71be1d-ddd8-4feb-9bed-24c3ddb6e1ad"]
                        },
                        "portal_url": "https://www.ebi.ac.uk/gxa/sc/experiments/E-MTAB-5061/results/tsne"
                    },
                    {
                        "integration_id": "60912ae7-e88f-48bf-8b33-27daccade2b6",
                        "integration_type": "get",
                        "entity_type": "project",
                        "title": "Single-cell RNA-seq analysis of 20 organs and tissues from individual mice creates "
                                 "a Tabula muris",
                        "entity_ids": {
                            "staging": ["2cd14cf5-f8e0-4c97-91a2-9e8957f41ea8"],
                            "integration": ["e0009214-c0a0-4a7b-96e2-d6a83e966ce0"],
                            "prod": ["e0009214-c0a0-4a7b-96e2-d6a83e966ce0"]
                        },
                        "portal_url": "https://www.ebi.ac.uk/gxa/sc/experiments/E-ENAD-15/results/tsne"
                    },
                ],
            },
            {
                "portal_id": "2e05f611-16fb-4bf3-b860-aa500f0256de",
                "portal_name": "Xena",
                "portal_icon": "https://xenabrowser.net/03340e094d1f3edc51bc3d1a2a589b65.png",
                "contact_email": "",
                "organization_name": "UCSC",
                "portal_description": "",
                "integrations": [
                    {
                        "integration_id": integration_id,
                        "integration_type": "get",
                        "entity_type": "project",
                        "title": title,
                        "entity_ids": {
                            "staging": [],
                            "integration": [project_uuid],
                            "prod": [project_uuid]
                        },
                        "portal_url": "https://singlecell.xenabrowser.net/datapages/?cohort=" + quote(title)
                    } for integration_id, project_uuid, title in (
                        # @formatter:off
                        ("73aa70fe-e40a-48da-9fa4-bea4c4d2ae74", "4a95101c-9ffc-4f30-a809-f04518a23803", "HCA Human Tissue T cell Activation"),  # noqa E501
                        ("c36e46c2-34d6-4129-853b-60256bc0af8d", "8185730f-4113-40d3-9cc3-929271784c2b", "HCA Adult Retina (Wong)"),  # noqa E501
                        ("dedb2f00-b92f-4f81-8633-6f58edcbf3f7", "005d611a-14d5-4fbf-846e-571a1f874f70", "HCA HPSI human cerebral organoids"),  # noqa E501
                        ("ced58994-05c2-4a2d-87b1-fff4faf2ca93", "cc95ff89-2e68-4a08-a234-480eca21ce79", "HCA Census of Immune Cells"),  # noqa E501
                        ("65e1465e-0641-4770-abbb-fde8bc0582aa", "4d6f6c96-2a83-43d8-8fe1-0f53bffd4674", "HCA Single Cell Liver Landscape"),  # noqa E501
                        ("39c69c7d-a245-4460-a045-6bd055564cca", "c4077b3c-5c98-4d26-a614-246d12c2e5d7", "HCA Tissue stability"),  # noqa E501
                        ("9cd98133-1a86-4937-94dc-4e9a68a36192", "091cf39b-01bc-42e5-9437-f419a66c8a45", "HCA Human Hematopoietic Profiling"),  # noqa E501
                        ("0f760ab0-2f81-40c8-a838-6a6c508bdd59", "f83165c5-e2ea-4d15-a5cf-33f3550bffde", "HCA Fetal Maternal Interface"),  # noqa E501
                        ("23ce54c0-58b5-4617-9b61-53ac020c1087", "cddab57b-6868-4be4-806f-395ed9dd635a", "HCA Human Pancreas"),  # noqa E501
                        ("980577ba-02d5-4edb-9662-24f2d1dc351a", "2043c65a-1cf8-4828-a656-9e247d4e64f1", "HCA Human Interneuron Development"),  # noqa E501
                        ("1dae24a2-998e-47d3-a570-26c36f2b073e", "abe1a013-af7a-45ed-8c26-f3793c24a1f4", "HCA Kidney Single Cell Atlas"),  # noqa E501
                        ("66cff7c4-8e79-4e84-a269-2c0970b49392", "f8aa201c-4ff1-45a4-890e-840d63459ca2", "HCA Human Colonic Mesenchyme IBD")  # noqa E501
                        # @formatter:on
                    )
                ]
            }
        ]
