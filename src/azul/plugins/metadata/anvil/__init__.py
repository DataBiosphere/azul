from typing import (
    Iterable,
    Mapping,
    Optional,
    Sequence,
    Type,
)

from azul.indexer import (
    Bundle,
)
from azul.plugins import (
    DocumentSlice,
    ManifestConfig,
    MetadataPlugin,
    Sorting,
)
from azul.plugins.metadata.anvil.indexer.transform import (
    ActivityTransformer,
    BaseTransformer,
    BiosampleTransformer,
    DatasetTransformer,
    DonorTransformer,
    FileTransformer,
    LibraryTransformer,
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


class Plugin(MetadataPlugin):

    @property
    def exposed_indices(self) -> Mapping[str, Sorting]:
        return dict(
            activities=Sorting(field_name='activity_id'),
            biosamples=Sorting(field_name='biosample_id'),
            datasets=Sorting(field_name='dataset_id'),
            donors=Sorting(field_name='donor_id'),
            files=Sorting(field_name='file_id'),
            libraries=Sorting(field_name='library_id')
        )

    @property
    def manifest_formats(self) -> Sequence[ManifestFormat]:
        return [ManifestFormat.compact, ManifestFormat.terra_pfb]

    def transformer_types(self) -> Iterable[Type[BaseTransformer]]:
        return (
            ActivityTransformer,
            BiosampleTransformer,
            DatasetTransformer,
            DonorTransformer,
            FileTransformer,
            LibraryTransformer
        )

    def transformers(self, bundle: Bundle, *, delete: bool) -> Iterable[BaseTransformer]:
        return [
            transformer_cls(bundle=bundle, deleted=delete)
            for transformer_cls in self.transformer_types()
        ]

    def mapping(self) -> MutableJSON:
        mapping = super().mapping()
        mapping['dynamic_templates'].append({
            'biosample_age_range': {
                'path_match': 'contents.biosamples.donor_age_at_collection_age_range',
                'mapping': self.range_mapping
            }
        })
        return mapping

    @property
    def _field_mapping(self) -> MetadataPlugin._FieldMapping:
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
                    f: f for f in [
                        'activity_id',
                        'activity_type',
                        'analysis_type',
                        'assay_category',
                        'data_modality',
                        'date_submitted',
                        'document_id',
                        'xref'
                    ]
                },
                'biosamples': {
                    f: f for f in [
                        'anatomical_site',
                        'biosample_id',
                        'biosample_type',
                        'date_collected',
                        'document_id',
                        'donor_age_at_collection_age_range',
                        'donor_age_at_collection_life_stage',
                        'donor_age_at_collection_unit',
                        'disease',
                        'preservation_state',
                        'xref'
                    ]
                },
                'datasets': {
                    f: f for f in [
                        'dataset_id',
                        'contact_point',
                        'custodian',
                        'document_id',
                        'last_modified_date',
                        'entity_description',
                        'entity_title'
                    ]
                },
                'donors': {
                    f: f for f in [
                        'birth_date',
                        'document_id',
                        'donor_id',
                        'organism_type',
                        'phenotypic_sex',
                        'reported_ethnicity',
                        'xref'
                    ]
                },
                'files': {
                    **{
                        f: f for f in [
                            'data_modality',
                            'document_id',
                            'file_format',
                            'file_id',
                            'reference_assembly',
                            'crc32',
                            'sha256',
                            'drs_path',
                            'name'
                        ]
                    },
                    # These field names are hard-coded in the implementation of
                    # the repository service/controller.
                    **{
                        'version': 'fileVersion',
                        'uuid': 'fileId',
                        'byte_size': 'size'
                    }
                },
                'libraries': {
                    f: f for f in [
                        'date_created',
                        'document_id',
                        'library_id',
                        'prep_material_name',
                        'xref'
                    ]
                }
            }
        }

    @property
    def source_id_field(self) -> str:
        return 'sourceId'

    @property
    def facets(self) -> Sequence[str]:
        return [
            'activity_type',
            'analysis_type',
            'anatomical_site',
            'assay_category',
            'biosample_type',
            'data_modality',
            'entity_title',
            'donor_age_at_collection_life_stage',
            'file_format',
            'disease',
            'organism_type',
            'phenotypic_sex',
            'prep_material_name',
            'preservation_state',
            'reference_assembly',
            'reported_ethnicity',
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
                'entity_title': 'dataset_title'
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
                'drs_path': 'file_drs_uri',
                'file_url': 'file_url'
            },
            ('contents', 'libraries'): {
                'document_id': 'library_document_id',
                'library_id': 'library_id'
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
