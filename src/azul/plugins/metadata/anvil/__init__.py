from collections import (
    defaultdict,
)
from typing import (
    Iterable,
    Optional,
    Sequence,
    Type,
)

from azul import (
    config,
    iif,
)
from azul.indexer.document import (
    DocumentType,
    EntityType,
    FieldPath,
    IndexName,
)
from azul.plugins import (
    DocumentSlice,
    ManifestConfig,
    MetadataPlugin,
    Sorting,
    SpecialFields,
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
    DiagnosisTransformer,
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
            bundles=Sorting(field_name=self.special_fields.bundle_uuid),
            datasets=Sorting(field_name='datasets.dataset_id'),
            donors=Sorting(field_name='donors.donor_id'),
            files=Sorting(field_name='files.file_id'),
        )

    @property
    def manifest_formats(self) -> Sequence[ManifestFormat]:
        return [
            ManifestFormat.compact,
            ManifestFormat.terra_pfb,
            *iif(config.enable_replicas, [
                ManifestFormat.verbatim_jsonl,
                ManifestFormat.verbatim_pfb
            ])
        ]

    def transformer_types(self) -> Iterable[Type[BaseTransformer]]:
        return (
            ActivityTransformer,
            BiosampleTransformer,
            BundleTransformer,
            DatasetTransformer,
            DiagnosisTransformer,
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
        if index_name.doc_type in (DocumentType.contribution, DocumentType.aggregate):
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
                'uuid': self.special_fields.bundle_uuid,
                'version': self.special_fields.bundle_version
            },
            'sources': {
                'id': self.special_fields.source_id,
                'spec': self.special_fields.source_spec
            },
            'contents': {
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
    def special_fields(self) -> SpecialFields:
        return SpecialFields(source_id='source_id',
                             source_spec='source_spec',
                             bundle_uuid='bundle_uuid',
                             bundle_version='bundle_version')

    @property
    def implicit_hub_type(self) -> str:
        return 'datasets'

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
    def manifest_config(self) -> ManifestConfig:
        result = defaultdict(dict)

        def recurse(mapping: MetadataPlugin._FieldMapping, path: FieldPath):
            for path_element, name_or_type in mapping.items():
                new_path = (*path, path_element)
                if isinstance(name_or_type, dict):
                    recurse(name_or_type, new_path)
                elif isinstance(name_or_type, str):
                    if new_path == ('entity_id',):
                        pass
                    elif new_path == ('contents', 'files', 'uuid'):
                        # Request the injection of a file URL …
                        result[path]['file_url'] = 'files.file_url'
                        # … but suppress the columns for the fields …
                        result[path][path_element] = None
                    elif new_path == ('contents', 'files', 'version'):
                        # … only used by that injection.
                        result[path][path_element] = None
                    else:
                        result[path][path_element] = name_or_type
                else:
                    assert False, (path, path_element, name_or_type)

        recurse(self._field_mapping, ())
        return result

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
