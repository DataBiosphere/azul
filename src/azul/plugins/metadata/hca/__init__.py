from typing import (
    Iterable,
    Type,
)

from azul.indexer.document import (
    Aggregate,
)
from azul.indexer.transform import (
    Transformer,
)
from azul.plugins import (
    MetadataPlugin,
    ServiceConfig,
)
from azul.plugins.metadata.hca.aggregate import (
    HCAAggregate,
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
            # FIXME: Detect invalid values in translation
            #        https://github.com/DataBiosphere/azul/issues/3071
            translation={
                "fileFormat": "contents.files.file_format",
                "fileName": "contents.files.name",
                "fileSize": "contents.files.size",
                "fileSource": "contents.files.file_source",
                "fileId": "contents.files.uuid",
                "fileVersion": "contents.files.version",
                "contentDescription": "contents.files.content_description",
                "matrixCellCount": "contents.files.matrix_cell_count",
                "isIntermediate": "contents.files.is_intermediate",

                "instrumentManufacturerModel": "contents.sequencing_protocols.instrument_manufacturer_model",
                "libraryConstructionApproach": "contents.library_preparation_protocols.library_construction_approach",
                "nucleicAcidSource": "contents.library_preparation_protocols.nucleic_acid_source",
                "pairedEnd": "contents.sequencing_protocols.paired_end",
                "workflow": "contents.analysis_protocols.workflow",
                "assayType": "contents.imaging_protocols.assay_type",

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
                "sampleDisease": "contents.sample_specimens.disease",
                "specimenDisease": "contents.specimens.disease",
                "genusSpecies": "contents.donors.genus_species",
                "donorDisease": "contents.donors.diseases",
                "developmentStage": "contents.donors.development_stage",
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

                "cellCount": "total_estimated_cells",
                "donorCount": "contents.donors.donor_count",
                "selectedCellType": "contents.cell_suspensions.selected_cell_type",

                "bundleUuid": "bundles.uuid",
                "bundleVersion": "bundles.version",

                "entryId": "entity_id",

                "sourceId": "sources.id",
                "sourceSpec": "sources.spec",
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
                "sources": {
                    "source_id": "id",
                    "source_spec": "spec",
                },
                "bundles": {
                    "bundle_uuid": "uuid",
                    "bundle_version": "version"
                },
                "contents.files": {
                    "file_document_id": "document_id",
                    "file_type": "file_type",
                    "file_name": "name",
                    "file_format": "file_format",
                    "read_index": "read_index",
                    "file_size": "size",
                    "file_uuid": "uuid",
                    "file_version": "version",
                    "file_crc32c": "crc32c",
                    "file_sha256": "sha256",
                    "file_content_type": "content-type",
                    # If an entry for `drs_path` is present here, manifest
                    # generators will replace it with a full DRS URI.
                    "file_drs_uri": "drs_path",
                    "file_url": "file_url"
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
                "contents.sequencing_protocols": {
                    "sequencing_protocol.instrument_manufacturer_model": "instrument_manufacturer_model",
                    "sequencing_protocol.paired_end": "paired_end"
                },
                "contents.library_preparation_protocols": {
                    "library_preparation_protocol.library_construction_approach": "library_construction_approach",
                    "library_preparation_protocol.nucleic_acid_source": "nucleic_acid_source"
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
                    "donor_organism.development_stage": "development_stage",
                    "donor_organism.diseases": "diseases",
                    "donor_organism.organism_age": "organism_age"
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
                "nucleicAcidSource",
                "genusSpecies",
                "organismAge",
                "organismAgeUnit",
                "biologicalSex",
                "sampleDisease",
                "specimenDisease",
                "donorDisease",
                "developmentStage",
                "instrumentManufacturerModel",
                "pairedEnd",
                "workflow",
                "assayType",
                "project",
                "fileFormat",
                "fileSource",
                "isIntermediate",
                "contentDescription",
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

    def aggregate_class(self) -> Type[Aggregate]:
        return HCAAggregate
