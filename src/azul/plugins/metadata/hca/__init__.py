from collections.abc import (
    Iterable,
    Sequence,
)
from typing import (
    Optional,
    TYPE_CHECKING,
    Type,
)

from azul.indexer.document import (
    Aggregate,
    DocumentType,
    EntityType,
    IndexName,
)
from azul.plugins import (
    DocumentSlice,
    ManifestConfig,
    MetadataPlugin,
    Sorting,
)
from azul.plugins.metadata.hca.bundle import (
    HCABundle,
)
from azul.plugins.metadata.hca.indexer.aggregate import (
    HCAAggregate,
)
from azul.plugins.metadata.hca.indexer.transform import (
    BaseTransformer,
    BundleTransformer,
    CellSuspensionTransformer,
    FileTransformer,
    ProjectTransformer,
    SampleTransformer,
)
from azul.plugins.metadata.hca.service.aggregation import (
    HCAAggregationStage,
    HCASummaryAggregationStage,
)
from azul.plugins.metadata.hca.service.filter import (
    HCAFilterStage,
)
from azul.plugins.metadata.hca.service.response import (
    HCASearchResponseStage,
    HCASummaryResponseStage,
)
from azul.service.manifest_service import (
    ManifestFormat,
)
from azul.types import (
    MutableJSON,
)
from humancellatlas.data.metadata import (
    api,
)


class Plugin(MetadataPlugin[HCABundle]):

    def transformer_types(self) -> Iterable[Type[BaseTransformer]]:
        return (
            FileTransformer,
            CellSuspensionTransformer,
            SampleTransformer,
            ProjectTransformer,
            BundleTransformer
        )

    def transformers(self,
                     bundle: HCABundle,
                     *,
                     delete: bool
                     ) -> Iterable[BaseTransformer]:
        api_bundle = api.Bundle(uuid=bundle.uuid,
                                version=bundle.version,
                                manifest=bundle.manifest,
                                metadata_files=bundle.metadata_files)

        def transformers():
            for transformer_cls in self.transformer_types():
                if TYPE_CHECKING:  # work around https://youtrack.jetbrains.com/issue/PY-44728
                    transformer_cls = BaseTransformer
                yield transformer_cls(bundle=bundle, api_bundle=api_bundle, deleted=delete)

        return list(transformers())

    def aggregate_class(self) -> Type[Aggregate]:
        return HCAAggregate

    def mapping(self, index_name: IndexName) -> MutableJSON:
        mapping = super().mapping(index_name)
        if index_name.doc_type in (DocumentType.contribution, DocumentType.aggregate):
            mapping['properties']['contents'] = {
                'properties': {
                    'projects': {
                        'properties': {
                            'accessions': {
                                'type': 'nested'
                            }
                        }
                    }
                }
            }
            mapping['dynamic_templates'][0:0] = [
                {
                    'donor_age_range': {
                        'path_match': 'contents.donors.organism_age_range',
                        'mapping': self.range_mapping
                    }
                },
                {
                    'exclude_metadata_field': {
                        'path_match': 'contents.metadata',
                        'mapping': {
                            'enabled': False
                        }
                    }
                },
                {
                    'exclude_metadata_field': {
                        'path_match': 'contents.files.related_files',
                        'mapping': {
                            'enabled': False
                        }
                    }
                },
                {
                    'project_nested_contributors': {
                        'path_match': 'contents.projects.contributors',
                        'mapping': {
                            'enabled': False
                        }
                    }
                },
                {
                    'project_nested_publications': {
                        'path_match': 'contents.projects.publications',
                        'mapping': {
                            'enabled': False
                        }
                    }
                }
            ]
        return mapping

    @property
    def exposed_indices(self) -> dict[EntityType, Sorting]:
        return dict(
            bundles=Sorting(field_name='bundleVersion', descending=True, max_page_size=100),
            files=Sorting(field_name='fileName'),
            projects=Sorting(field_name='projectTitle', max_page_size=100),
            samples=Sorting(field_name='sampleId')
        )

    @property
    def manifest_formats(self) -> Sequence[ManifestFormat]:
        return tuple(ManifestFormat)

    @property
    def _field_mapping(self) -> MetadataPlugin._FieldMapping:
        # FIXME: Detect invalid values in field mapping
        #        https://github.com/DataBiosphere/azul/issues/3071
        return {
            'entity_id': 'entryId',
            'bundles': {
                'uuid': 'bundleUuid',
                'version': 'bundleVersion'
            },
            'sources': {
                'id': self.source_id_field,
                'spec': 'sourceSpec'
            },
            'cell_count': 'cellCount',
            'effective_cell_count': 'effectiveCellCount',
            'contents': {
                'dates': {
                    'submission_date': 'submissionDate',
                    'update_date': 'updateDate',
                    'last_modified_date': 'lastModifiedDate',
                    'aggregate_submission_date': 'aggregateSubmissionDate',
                    'aggregate_update_date': 'aggregateUpdateDate',
                    'aggregate_last_modified_date': 'aggregateLastModifiedDate'
                },
                'files': {
                    'file_format': 'fileFormat',
                    'name': 'fileName',
                    'size': 'fileSize',
                    'file_source': 'fileSource',
                    'uuid': 'fileId',
                    'version': 'fileVersion',
                    'content_description': 'contentDescription',
                    'matrix_cell_count': 'matrixCellCount',
                    'is_intermediate': 'isIntermediate'
                },
                'projects': {
                    'contact_names': 'contactName',
                    'document_id': 'projectId',
                    'institutions': 'institution',
                    'laboratory': 'laboratory',
                    'project_description': 'projectDescription',
                    'project_short_name': 'project',
                    'project_title': 'projectTitle',
                    'publication_titles': 'publicationTitle',
                    'accessions': 'accessions',
                    'estimated_cell_count': 'projectEstimatedCellCount'
                },
                'sequencing_protocols': {
                    'instrument_manufacturer_model': 'instrumentManufacturerModel',
                    'paired_end': 'pairedEnd'
                },
                'library_preparation_protocols': {
                    'library_construction_approach': 'libraryConstructionApproach',
                    'nucleic_acid_source': 'nucleicAcidSource'
                },
                'analysis_protocols': {
                    'workflow': 'workflow'
                },
                'imaging_protocols': {
                    'assay_type': 'assayType'
                },
                'donors': {
                    'biological_sex': 'biologicalSex',
                    'genus_species': 'genusSpecies',
                    'diseases': 'donorDisease',
                    'development_stage': 'developmentStage',
                    'organism_age': 'organismAge',
                    'organism_age_range': 'organismAgeRange',
                    'donor_count': 'donorCount'
                },
                'samples': {
                    'biomaterial_id': 'sampleId',
                    'entity_type': 'sampleEntityType',
                    'organ': 'organ',
                    'organ_part': 'organPart',
                    'model_organ': 'modelOrgan',
                    'model_organ_part': 'modelOrganPart',
                    'effective_organ': 'effectiveOrgan'
                },
                'sample_specimens': {
                    'disease': 'sampleDisease'
                },
                'specimens': {
                    'disease': 'specimenDisease',
                    'organ': 'specimenOrgan',
                    'organ_part': 'specimenOrganPart',
                    'preservation_method': 'preservationMethod'
                },
                'cell_suspensions': {
                    'selected_cell_type': 'selectedCellType'
                },
                'cell_lines': {
                    'cell_line_type': 'cellLineType'
                }
            }
        }

    @property
    def source_id_field(self) -> str:
        return 'sourceId'

    @property
    def facets(self) -> Sequence[str]:
        return [
            *super().facets,
            'organ',
            'organPart',
            'modelOrgan',
            'modelOrganPart',
            'effectiveOrgan',
            'specimenOrgan',
            'specimenOrganPart',
            'sampleEntityType',
            'libraryConstructionApproach',
            'nucleicAcidSource',
            'genusSpecies',
            'organismAge',
            'biologicalSex',
            'sampleDisease',
            'specimenDisease',
            'donorDisease',
            'developmentStage',
            'instrumentManufacturerModel',
            'pairedEnd',
            'workflow',
            'assayType',
            'project',
            'fileFormat',
            'fileSource',
            'isIntermediate',
            'contentDescription',
            'laboratory',
            'preservationMethod',
            'projectTitle',
            'cellLineType',
            'selectedCellType',
            'projectDescription',
            'institution',
            'contactName',
            'publicationTitle'
        ]

    @property
    def manifest(self) -> ManifestConfig:
        return {
            ('sources',): {
                'id': 'source_id',
                'spec': 'source_spec',
            },
            ('bundles',): {
                'uuid': 'bundle_uuid',
                'version': 'bundle_version'
            },
            ('contents', 'files'): {
                'document_id': 'file_document_id',
                'file_type': 'file_type',
                'name': 'file_name',
                'file_format': 'file_format',
                'read_index': 'read_index',
                'size': 'file_size',
                'uuid': 'file_uuid',
                'version': 'file_version',
                'crc32c': 'file_crc32c',
                'sha256': 'file_sha256',
                'content-type': 'file_content_type',
                'drs_uri': 'file_drs_uri',
                'file_url': 'file_url'
            },
            ('contents', 'cell_suspensions'): {
                'document_id': 'cell_suspension.provenance.document_id',
                'biomaterial_id': 'cell_suspension.biomaterial_core.biomaterial_id',
                'total_estimated_cells': 'cell_suspension.estimated_cell_count',
                'selected_cell_type': 'cell_suspension.selected_cell_type'
            },
            ('contents', 'sequencing_processes'): {
                'document_id': 'sequencing_process.provenance.document_id'
            },
            ('contents', 'sequencing_protocols'): {
                'instrument_manufacturer_model': 'sequencing_protocol.instrument_manufacturer_model',
                'paired_end': 'sequencing_protocol.paired_end'
            },
            ('contents', 'library_preparation_protocols'): {
                'library_construction_approach': 'library_preparation_protocol.library_construction_approach',
                'nucleic_acid_source': 'library_preparation_protocol.nucleic_acid_source'
            },
            ('contents', 'projects'): {
                'document_id': 'project.provenance.document_id',
                'institutions': 'project.contributors.institution',
                'laboratory': 'project.contributors.laboratory',
                'project_short_name': 'project.project_core.project_short_name',
                'project_title': 'project.project_core.project_title',
                'estimated_cell_count': 'project.estimated_cell_count'
            },
            ('contents', 'specimens'): {
                'document_id': 'specimen_from_organism.provenance.document_id',
                'disease': 'specimen_from_organism.diseases',
                'organ': 'specimen_from_organism.organ',
                'organ_part': 'specimen_from_organism.organ_part',
                'preservation_method': 'specimen_from_organism.preservation_storage.preservation_method'
            },
            ('contents', 'donors'): {
                'biological_sex': 'donor_organism.sex',
                'biomaterial_id': 'donor_organism.biomaterial_core.biomaterial_id',
                'document_id': 'donor_organism.provenance.document_id',
                'genus_species': 'donor_organism.genus_species',
                'development_stage': 'donor_organism.development_stage',
                'diseases': 'donor_organism.diseases',
                'organism_age': 'donor_organism.organism_age'
            },
            ('contents', 'cell_lines'): {
                'document_id': 'cell_line.provenance.document_id',
                'biomaterial_id': 'cell_line.biomaterial_core.biomaterial_id'
            },
            ('contents', 'organoids'): {
                'document_id': 'organoid.provenance.document_id',
                'biomaterial_id': 'organoid.biomaterial_core.biomaterial_id',
                'model_organ': 'organoid.model_organ',
                'model_organ_part': 'organoid.model_organ_part'
            },
            ('contents', 'samples'): {
                'entity_type': '_entity_type',
                'document_id': 'sample.provenance.document_id',
                'biomaterial_id': 'sample.biomaterial_core.biomaterial_id'
            },
            ('contents', 'sequencing_inputs'): {
                'document_id': 'sequencing_input.provenance.document_id',
                'biomaterial_id': 'sequencing_input.biomaterial_core.biomaterial_id',
                'sequencing_input_type': 'sequencing_input_type'
            }
        }

    def document_slice(self, entity_type: str) -> Optional[DocumentSlice]:
        if entity_type in ('files', 'bundles'):
            return None
        else:
            return DocumentSlice(excludes=['bundles'])

    @property
    def summary_response_stage(self) -> Type[HCASummaryResponseStage]:
        return HCASummaryResponseStage

    @property
    def search_response_stage(self) -> Type[HCASearchResponseStage]:
        return HCASearchResponseStage

    @property
    def summary_aggregation_stage(self) -> Type[HCASummaryAggregationStage]:
        return HCASummaryAggregationStage

    @property
    def aggregation_stage(self) -> Type[HCAAggregationStage]:
        return HCAAggregationStage

    @property
    def filter_stage(self) -> Type[HCAFilterStage]:
        return HCAFilterStage
