from typing import Type

from azul import config
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
                        *self._prefix_clause(prefix),
                        *(
                            [
                                {
                                    "range": {
                                        "manifest.version": {
                                            "gte": "2018-11-27"
                                        }
                                    }
                                }
                            ] if config.dss_endpoint == "https://dss.integration.data.humancellatlas.org/v1" else [
                                {
                                    "bool": {
                                        "should": [
                                            {
                                                "terms": {
                                                    "files.project_json.provenance.document_id": [
                                                        # CBeta Release spreadsheet as of 11/13/2018
                                                        "2c4724a4-7252-409e-b008-ff5c127c7e89",  # treutlein
                                                        "08e7b6ba-5825-47e9-be2d-7978533c5f8c",  # pancreas6decades
                                                        "019a935b-ea35-4d83-be75-e1a688179328",  # neuron_diff
                                                        "a5ae0428-476c-46d2-a9f2-aad955b149aa",  # EMTAB5061
                                                        "adabd2bd-3968-4e77-b0df-f200f7351661",  # Regev-ICA
                                                        "67bc798b-a34a-4104-8cab-cad648471f69",  # Teichmann
                                                        "81b5f43d-3c20-4575-9efa-bfb0b070a6e3",  # Meyer
                                                        "519b58ef-6462-4ed3-8c0d-375b54f53c31",  # EGEOD106540
                                                        "2cd14cf5-f8e0-4c97-91a2-9e8957f41ea8",  # tabulamuris
                                                        "1f9a699a-262c-40a0-8b2c-7ba960ca388c",  # ido_amit
                                                        "62aa3211-bf52-4873-9029-0bcc1d09e553",  # humphreys
                                                        "a71dee10-9a4d-4ea2-a6f3-ae7314112cf1",  # peer
                                                        "adb384b2-cd5e-4cf5-9205-5f066474005f",  # basu
                                                        "46c58e08-4518-4e45-acfe-bdab2434975d",  # 10x-mouse-brain
                                                        "3eaad325-3666-4b65-a4ed-e23ff71222c1",  # rsatija
                                                    ]
                                                }
                                            },
                                            {
                                                "range": {
                                                    "manifest.version": {
                                                        "gte": "2019-04-03"
                                                    }
                                                }
                                            }
                                        ]
                                    }
                                }
                            ] if config.dss_endpoint == "https://dss.staging.data.humancellatlas.org/v1" else [
                            ]
                        )
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
                "donorDisease": "contents.donors.disease",
                "organ": "contents.samples.organ",
                "organPart": "contents.samples.organ_part",
                "modelOrgan": "contents.samples.model_organ",
                "modelOrganPart": "contents.samples.model_organ_part",
                "effectiveOrgan": "contents.samples.effective_organ",
                "organismAge": "contents.donors.organism_age",
                "organismAgeUnit": "contents.donors.organism_age_unit",
                "organismAgeRange": "contents.donors.organism_age_range",
                "preservationMethod": "contents.specimens.preservation_method",

                "cellLineType": "contents.cell_lines.cell_line_type",

                "cellCount": "contents.cell_suspensions.total_estimated_cells",
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
