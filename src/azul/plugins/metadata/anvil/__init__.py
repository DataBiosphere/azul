from typing import (
    Iterable,
    Optional,
    Sequence,
    Type,
)

from azul.indexer.document import (
    EntityType,
    IndexName,
)
from azul.plugins import (
    DocumentSlice,
    ManifestConfig,
    MetadataPlugin,
    Sorting,
)
from azul.plugins.metadata.anvil.bundle import (
    AnvilBundle,
)
from azul.plugins.metadata.anvil.indexer.transform import (
    ActivityTransformer,
    BaseTransformer,
    BiosampleTransformer,
    BundleTransformer,
    DatasetTransformer,
    DonorTransformer,
    FileTransformer,
)
from azul.plugins.metadata.anvil.service.aggregation import (
    AnvilAggregationStage,
    AnvilSummaryAggregationStage,
)
from azul.plugins.metadata.anvil.service.filter import (
    AnvilFilterStage,
)
from azul.plugins.metadata.anvil.service.response import (
    AnvilSearchResponseStage,
    AnvilSummaryResponseStage,
)
from azul.service.manifest_service import (
    ManifestFormat,
)
from azul.types import (
    MutableJSON,
)


class Plugin(MetadataPlugin[AnvilBundle]):

    @property
    def exposed_indices(self) -> dict[EntityType, Sorting]:
        return dict(
            activities=Sorting(field_name='activities.activity_id'),
            biosamples=Sorting(field_name='biosamples.biosample_id'),
            bundles=Sorting(field_name='bundleUuid'),
            datasets=Sorting(field_name='datasets.dataset_id'),
            donors=Sorting(field_name='donors.donor_id'),
            files=Sorting(field_name='files.file_id'),
        )

    @property
    def manifest_formats(self) -> Sequence[ManifestFormat]:
        return [ManifestFormat.compact, ManifestFormat.terra_pfb]

    def transformer_types(self) -> Iterable[Type[BaseTransformer]]:
        return (
            ActivityTransformer,
            BiosampleTransformer,
            BundleTransformer,
            DatasetTransformer,
            DonorTransformer,
            FileTransformer,
        )

    def transformers(self,
                     bundle: AnvilBundle,
                     *,
                     delete: bool
                     ) -> Iterable[BaseTransformer]:
        return [
            transformer_cls(bundle=bundle, deleted=delete)
            for transformer_cls in self.transformer_types()
        ]

    def mapping(self, index_name: IndexName) -> MutableJSON:
        mapping = super().mapping(index_name)

        def range_mapping(name: str, path: str) -> MutableJSON:
            return {
                name: {
                    'path_match': path,
                    'mapping': self.range_mapping
                }
            }

        mapping['dynamic_templates'].extend([
            range_mapping('biosample_age_range', 'contents.biosamples.donor_age_at_collection'),
            range_mapping('diagnosis_age_range', 'contents.diagnoses.diagnosis_age'),
            range_mapping('diagnosis_onset_age_range', 'contents.diagnoses.diagnosis_onset_age')
        ])
        return mapping

    @property
    def _field_mapping(self) -> MetadataPlugin._FieldMapping:
        common_fields = [
            'document_id',
            'source_datarepo_row_ids'
        ]
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
            'contents': {
                'activities': {
                    f: f'activities.{f}' for f in [
                        *common_fields,
                        'activity_id',
                        'activity_table',
                        'activity_type',
                        'assay_type',
                        'data_modality',
                        'reference_assembly',
                        # Not in schema
                        'date_created',
                    ]
                },
                'biosamples': {
                    f: f'biosamples.{f}' for f in [
                        *common_fields,
                        'biosample_id',
                        'anatomical_site',
                        'apriori_cell_type',
                        'biosample_type',
                        'disease',
                        'donor_age_at_collection_unit',
                        'donor_age_at_collection',
                    ]
                },
                'diagnoses': {
                    f: f'diagnoses.{f}' for f in [
                        *common_fields,
                        'diagnosis_id',
                        'disease',
                        'diagnosis_age_unit',
                        'diagnosis_age',
                        'onset_age_unit',
                        'onset_age',
                        'phenotype',
                        'phenopacket'
                    ]
                },
                'datasets': {
                    f: f'datasets.{f}' for f in [
                        *common_fields,
                        'dataset_id',
                        'consent_group',
                        'data_use_permission',
                        'owner',
                        'principal_investigator',
                        'registered_identifier',
                        'title',
                        'data_modality',
                    ]
                },
                'donors': {
                    f: f'donors.{f}' for f in [
                        *common_fields,
                        'donor_id',
                        'organism_type',
                        'phenotypic_sex',
                        'reported_ethnicity',
                        'genetic_ancestry',
                    ]
                },
                'files': {
                    **{
                        f: f'files.{f}' for f in [
                            *common_fields,
                            'file_id',
                            'data_modality',
                            'file_format',
                            'file_size',
                            'file_md5sum',
                            'reference_assembly',
                            'file_name',
                            'is_supplementary',
                            # Not in schema
                            'crc32',
                            'sha256',
                            'drs_uri',
                        ]
                    },
                    # These field names are hard-coded in the implementation of
                    # the repository service/controller.
                    **{
                        # Not in schema
                        'version': 'fileVersion',
                        'uuid': 'fileId',
                    }
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
            'activities.activity_type',
            'activities.assay_type',
            'activities.data_modality',
            'biosamples.anatomical_site',
            'biosamples.biosample_type',
            'biosamples.disease',
            'diagnoses.disease',
            'diagnoses.phenotype',
            'diagnoses.phenopacket',
            'datasets.consent_group',
            'datasets.data_use_permission',
            'datasets.registered_identifier',
            'datasets.title',
            'donors.organism_type',
            'donors.phenotypic_sex',
            'donors.reported_ethnicity',
            'files.data_modality',
            'files.file_format',
            'files.reference_assembly',
            'files.is_supplementary',
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
            ('contents', 'activities'): {
                'document_id': 'activity_document_id',
                'activity_type': 'activity_type',
            },
            ('contents', 'biosamples'): {
                'document_id': 'biosample_document_id',
                'biosample_type': 'biosample_type',
                'anatomical_site': 'anatomical_site'
            },
            ('contents', 'datasets'): {
                'document_id': 'dataset_document_id',
                'dataset_id': 'dataset_id',
                'title': 'dataset_title'
            },
            ('contents', 'donors'): {
                'phenotypic_sex': 'phenotypic_sex',
                'document_id': 'donor_document_id',
                'species': 'species',
            },
            ('contents', 'files'): {
                'document_id': 'file_document_id',
                'file_format': 'file_format',
                'reference_assembly': 'file_reference_assembly',
                'crc32': 'file_crc32',
                'sha256': 'file_sha256',
                'drs_uri': 'file_drs_uri',
                'file_url': 'file_url'
            }
        }

    def document_slice(self, entity_type: str) -> Optional[DocumentSlice]:
        return None

    @property
    def summary_response_stage(self) -> 'Type[AnvilSummaryResponseStage]':
        return AnvilSummaryResponseStage

    @property
    def search_response_stage(self) -> 'Type[AnvilSearchResponseStage]':
        return AnvilSearchResponseStage

    @property
    def summary_aggregation_stage(self) -> 'Type[AnvilSummaryAggregationStage]':
        return AnvilSummaryAggregationStage

    @property
    def aggregation_stage(self) -> 'Type[AnvilAggregationStage]':
        return AnvilAggregationStage

    @property
    def filter_stage(self) -> 'Type[AnvilFilterStage]':
        return AnvilFilterStage
