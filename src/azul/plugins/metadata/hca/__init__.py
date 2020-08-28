from typing import (
    Iterable,
    Type,
)

from azul.indexer.transform import (
    Transformer,
)
from azul.plugins import (
    MetadataPlugin,
    ServiceConfig,
)
from azul.plugins.metadata.hca.transform import (
    BundleTransformer,
    CellSuspensionTransformer,
    FileTransformer,
    ProjectTransformer,
    SampleTransformer,
)
from azul.types import (
    JSON,
)


class Plugin(MetadataPlugin):

    def transformers(self) -> Iterable[Type[Transformer]]:
        return (
            FileTransformer,
            CellSuspensionTransformer,
            SampleTransformer,
            ProjectTransformer,
            BundleTransformer
        )

    def mapping(self) -> JSON:
        return {
            "numeric_detection": False,
            "dynamic_templates": [
                {
                    "donor_age_range": {
                        "path_match": "contents.donors.organism_age_range",
                        "mapping": {
                            # A float (single precision IEEE-754) can represent all integers up to 16,777,216. If we
                            # used float values for organism ages in seconds, we would not be able to accurately
                            # represent an organism age of 16,777,217 seconds. That is 194 days and 15617 seconds.
                            # A double precision IEEE-754 representation loses accuracy at 9,007,199,254,740,993 which
                            # is more than 285616415 years.

                            # Note that Python's float uses double precision IEEE-754.
                            # (https://docs.python.org/3/tutorial/floatingpoint.html#representation-error)
                            "type": "double_range"
                        }
                    }
                },
                {
                    "exclude_metadata_field": {
                        "path_match": "contents.metadata",
                        "mapping": {
                            "enabled": False
                        }
                    }
                },
                {
                    "exclude_metadata_field": {
                        "path_match": "contents.files.related_files",
                        "mapping": {
                            "enabled": False
                        }
                    }
                },
                {
                    "project_nested_contributors": {
                        "path_match": "contents.projects.contributors",
                        "mapping": {
                            "enabled": False
                        }
                    }
                },
                {
                    "project_nested_publications": {
                        "path_match": "contents.projects.publications",
                        "mapping": {
                            "enabled": False
                        }
                    }
                },
                {
                    "strings_as_text": {
                        "match_mapping_type": "string",
                        "mapping": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        }
                    }
                },
                {
                    "other_types_with_keyword": {
                        "match_mapping_type": "*",
                        "mapping": {
                            "type": "{dynamic_type}",
                            "fields": {
                                "keyword": {
                                    "type": "{dynamic_type}"
                                }
                            }
                        }
                    }
                }
            ]
        }

    def service_config(self) -> ServiceConfig:
        return ServiceConfig(
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
                "sampleDisease": "contents.samples.disease",
                "specimenDisease": "contents.specimens.disease",
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
                    "file_crc32c": "crc32c",
                    "file_sha256": "sha256",
                    "file_content_type": "content-type"
                },
                "contents.cell_suspensions": {
                    "cell_suspension.provenance.document_id": "document_id",
                    "cell_suspension.biomaterial_core.biomaterial_id": "biomaterial_id",
                    "cell_suspension.estimated_cell_count": "total_estimated_cells",
                    "cell_suspension.selected_cell_type": "selected_cell_type"
                },
                "contents.sequencing_processes": {
                    "sequencing_process.provenance.document_id": "document_id"
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
                },
                "contents.sequencing_inputs": {
                    "sequencing_input.provenance.document_id": "document_id",
                    "sequencing_input.biomaterial_core.biomaterial_id": "biomaterial_id",
                    "sequencing_input_type": "sequencing_input_type"
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
                "sampleDisease",
                "specimenDisease",
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
