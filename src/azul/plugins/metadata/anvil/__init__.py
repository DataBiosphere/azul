from collections import (
    defaultdict,
)
from operator import (
    itemgetter,
)
from typing import (
    Iterable,
    Sequence,
)

from azul import (
    JSON,
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
    DonorTransformer,
    FileTransformer,
)
from azul.plugins.metadata.anvil.schema import (
    anvil_schema,
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
from azul.service.avro_pfb import (
    avro_pfb_schema,
)
from azul.service.manifest_service import (
    ManifestFormat,
)
from azul.types import (
    AnyMutableJSON,
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

    def transformer_types(self) -> Iterable[type[BaseTransformer]]:
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
                    # the repository service/controller. Also, these field paths
                    # have a brittle coupling that must be maintained to the
                    # field lookups in `self.manifest_config`.
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

        # Note that there is a brittle coupling that must be maintained between
        # the fields listed here and those used in `self._field_mapping`.
        fields_to_omit_from_manifest = [
            ('contents', 'files', 'uuid'),
            ('contents', 'files', 'version'),
        ]

        def recurse(mapping: MetadataPlugin._FieldMapping, path: FieldPath):
            for path_element, name_or_type in mapping.items():
                new_path = (*path, path_element)
                if isinstance(name_or_type, dict):
                    recurse(name_or_type, new_path)
                elif isinstance(name_or_type, str):
                    if new_path == ('entity_id',):
                        pass
                    elif new_path in fields_to_omit_from_manifest:
                        result[path][path_element] = None
                        fields_to_omit_from_manifest.remove(new_path)
                    else:
                        result[path][path_element] = name_or_type
                else:
                    assert False, (path, path_element, name_or_type)

        recurse(self._field_mapping, ())
        assert len(fields_to_omit_from_manifest) == 0, fields_to_omit_from_manifest
        # The file URL is synthesized from the `uuid` and `version` fields.
        # Above, we already configured these two fields to be omitted from the
        # manifest since they are not informative to the user.
        result[('contents', 'files')]['file_url'] = 'files.file_url'
        return result

    def verbatim_pfb_schema(self,
                            replicas: Iterable[JSON]
                            ) -> tuple[Iterable[JSON], Sequence[str], JSON]:
        entity_schemas = []
        entity_types = []
        for table_schema in sorted(anvil_schema['tables'], key=itemgetter('name')):
            table_name = table_schema['name']
            # FIXME: Improve handling of DUOS replicas
            #        https://github.com/DataBiosphere/azul/issues/6139
            is_duos_type = table_name == 'anvil_dataset'
            entity_types.append(table_name)
            field_schemas = [
                self._pfb_schema_from_anvil_column(table_name=table_name,
                                                   column_name='datarepo_row_id',
                                                   anvil_datatype='string',
                                                   is_optional=False,
                                                   is_polymorphic=is_duos_type)
            ]
            if is_duos_type:
                field_schemas.append(self._pfb_schema_from_anvil_column(table_name=table_name,
                                                                        column_name='description',
                                                                        anvil_datatype='string',
                                                                        is_polymorphic=True))
            elif table_name == 'anvil_file':
                field_schemas.append(self._pfb_schema_from_anvil_column(table_name=table_name,
                                                                        column_name='drs_uri',
                                                                        anvil_datatype='string'))
            for column_schema in table_schema['columns']:
                field_schemas.append(
                    self._pfb_schema_from_anvil_column(table_name=table_name,
                                                       column_name=column_schema['name'],
                                                       anvil_datatype=column_schema['datatype'],
                                                       is_array=column_schema['array_of'],
                                                       is_optional=not column_schema['required'],
                                                       is_polymorphic=is_duos_type)
                )

            field_schemas.sort(key=itemgetter('name'))
            entity_schemas.append({
                'name': table_name,
                'type': 'record',
                'fields': field_schemas
            })
        return replicas, entity_types, avro_pfb_schema(entity_schemas)

    def _pfb_schema_from_anvil_column(self,
                                      *,
                                      table_name: str,
                                      column_name: str,
                                      anvil_datatype: str,
                                      is_array: bool = False,
                                      is_optional: bool = True,
                                      is_polymorphic: bool = False
                                      ) -> AnyMutableJSON:
        _anvil_to_pfb_types = {
            'boolean': 'boolean',
            'float': 'double',
            'integer': 'long',
            'string': 'string',
            'fileref': 'string'
        }
        type_ = _anvil_to_pfb_types[anvil_datatype]
        if is_optional:
            type_ = ['null', type_]
        if is_array:
            type_ = {
                'type': 'array',
                'items': type_
            }
        if is_polymorphic and (is_array or not is_optional):
            type_ = ['null', type_]
        return {
            'name': column_name,
            'namespace': table_name,
            'type': type_,
        }

    def document_slice(self, entity_type: str) -> DocumentSlice | None:
        return None

    @property
    def summary_response_stage(self) -> 'type[AnvilSummaryResponseStage]':
        return AnvilSummaryResponseStage

    @property
    def search_response_stage(self) -> 'type[AnvilSearchResponseStage]':
        return AnvilSearchResponseStage

    @property
    def summary_aggregation_stage(self) -> 'type[AnvilSummaryAggregationStage]':
        return AnvilSummaryAggregationStage

    @property
    def aggregation_stage(self) -> 'type[AnvilAggregationStage]':
        return AnvilAggregationStage

    @property
    def filter_stage(self) -> 'type[AnvilFilterStage]':
        return AnvilFilterStage
