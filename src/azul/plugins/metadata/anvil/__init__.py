from collections import (
    defaultdict,
)
from operator import (
    itemgetter,
)
from typing import (
    Iterable,
    Self,
    Sequence,
    cast,
)

from attrs import (
    frozen,
)
from more_itertools import (
    one,
)
from more_itertools.more import (
    always_iterable,
)

from azul import (
    config,
)
from azul.field_type import (
    pass_thru_str,
)
from azul.indexer.document import (
    DocumentType,
    EntityType,
    FieldPath,
    FieldPathElement,
    IndexName,
)
from azul.lib import (
    strings,
)
from azul.lib.digests import (
    Digest,
)
from azul.lib.functions import (
    iif,
)
from azul.lib.types import (
    AnyMutableJSON,
    JSON,
    MutableJSON,
    MutableJSONArray,
    MutableJSONs,
    json_bool,
    json_element_mappings,
    json_element_strings,
    json_int,
    json_list,
    json_mapping,
    json_str,
    optional,
)
from azul.plugins import (
    DocumentSlice,
    FieldName,
    File,
    InverseFieldMapping,
    ManifestConfig,
    ManifestFormat,
    MetadataPlugin,
    Sorting,
    SpecialField,
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
from azul.source import (
    SourceRef,
)


class Plugin(MetadataPlugin[AnvilBundle]):

    @property
    def exposed_indices(self) -> dict[EntityType, Sorting]:
        return dict(
            activities=Sorting(field_name='activities.activity_id'),
            biosamples=Sorting(field_name='biosamples.biosample_id'),
            bundles=Sorting(field_name=self.special_fields.bundle_uuid.name),
            datasets=Sorting(field_name='datasets.dataset_id'),
            donors=Sorting(field_name='donors.donor_id'),
            files=Sorting(field_name='files.file_id'),
        )

    @property
    def manifest_formats(self) -> Sequence[ManifestFormat]:
        return [
            ManifestFormat.compact,
            ManifestFormat.terra_pfb,
            *iif(config.enable_mirroring, [
                ManifestFormat.curl
            ]),
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

            json_list(mapping['dynamic_templates']).extend([
                range_mapping('biosample_age_range', 'contents.biosamples.donor_age_at_collection'),
                range_mapping('diagnosis_age_range', 'contents.diagnoses.diagnosis_age'),
                range_mapping('diagnosis_onset_age_range', 'contents.diagnoses.diagnosis_onset_age')
            ])
        return mapping

    @property
    def _field_mapping(self) -> InverseFieldMapping:
        common_fields = [
            'document_id',
            'source_datarepo_row_ids'
        ]
        return {
            'entity_id': 'entryId',
            'bundles': {
                # These field paths have a brittle coupling that must be
                # maintained to the field lookups in `self.manifest_config`.
                'uuid': self.special_fields.bundle_uuid.name,
                'version': self.special_fields.bundle_version.name
            },
            'sources': {
                # These field paths have a brittle coupling that must be
                # maintained to the field lookups in `self.manifest_config`.
                'id': self.special_fields.source_id.name,
                'spec': self.special_fields.source_spec.name
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
                        # This field path has a brittle coupling that must be
                        # maintained to the field lookup in
                        # `self.manifest_config`.
                        'duos_id',
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
                        # This field path has a brittle coupling that must be
                        # maintained to the field lookup in
                        # `self.manifest_config`.
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
                    }
                }
            }
        }

    special_fields = SpecialFields(
        source_id=SpecialField.symmetric('source_id', pass_thru_str),
        source_spec=SpecialField.symmetric('source_spec', pass_thru_str),
        source_prefix=SpecialField.symmetric('source_prefix', pass_thru_str),
        bundle_uuid=SpecialField.symmetric('bundle_uuid', pass_thru_str),
        bundle_version=SpecialField.symmetric('bundle_version', pass_thru_str),
        file_uuid=SpecialField(name='files.document_id',
                               name_in_hit='document_id',
                               type=pass_thru_str),
        file_name=SpecialField(name='files.file_name',
                               name_in_hit='file_name',
                               type=pass_thru_str),
    )

    def azul_slug(self, document: JSON) -> str | list[str] | None:
        def slug(title: str) -> str:
            # AnVIL dataset titles are short enough that we don't need to limit
            # the number of words or length of the words used in the slug.
            return strings.azul_slug(title,
                                     words_left=None,
                                     words_right=None,
                                     word_length=None,
                                     hash_length=6)

        contents = json_mapping(document['contents'])
        dataset = one(json_element_mappings(contents['datasets']))
        title = dataset.get('title')
        if title is None:
            return None
        elif isinstance(title, list):
            return [slug(t) for t in json_element_strings(title)]
        else:
            return slug(json_str(title))

    @property
    def root_entity_type(self) -> str:
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
        result: dict[FieldPath, dict[FieldPathElement, FieldName | None]]
        result = defaultdict(dict)

        # Note that there is a brittle coupling that must be maintained between
        # the fields listed here and those used in `self._field_mapping`.
        fields_to_omit_from_manifest: list[FieldPath] = [
            ('contents', 'activities', 'activity_table'),
            # We omit the `duos_id` field from manifests since there is only one
            # DUOS bundle per dataset, and that bundle only contributes to outer
            # entities of the `datasets` type, not to entities of the other
            # types, such as files, which the manifest is generated from.
            ('contents', 'datasets', 'duos_id'),
            ('contents', 'files', 'version'),
        ]

        # Furthermore, renamed values should match the field's path in a
        # response hit from the `/index/files` endpoint.
        fields_to_rename_in_manifest: dict[FieldPath, str] = {
            ('bundles', 'uuid'): 'bundles.bundle_uuid',
            ('bundles', 'version'): 'bundles.bundle_version',
            ('sources', 'id'): 'sources.source_id',
            ('sources', 'spec'): 'sources.source_spec',
        }

        def recurse(mapping: InverseFieldMapping, path: FieldPath):
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
                    elif new_path in fields_to_rename_in_manifest:
                        result[path][path_element] = fields_to_rename_in_manifest.pop(new_path)
                    else:
                        result[path][path_element] = name_or_type
                else:
                    assert False, (path, path_element, name_or_type)

        recurse(self._field_mapping, ())
        assert len(fields_to_omit_from_manifest) == 0, fields_to_omit_from_manifest
        assert len(fields_to_rename_in_manifest) == 0, fields_to_rename_in_manifest
        # The file URL is synthesized from the `uuid` and `version` fields.
        # Above, we already configured these two fields to be omitted from the
        # manifest since they are not informative to the user.
        result[('contents', 'files')]['file_url'] = 'files.azul_url'
        result[('contents', 'files')]['file_mirror_uri'] = 'files.azul_mirror_uri'
        return result

    primary_keys_by_table = {
        table['name']: one(json_element_strings(table['primaryKey']))
        for table in json_element_mappings(anvil_schema['tables'])
    }

    foreign_keys_by_table = {
        json_str(table['name']): [
            (
                json_str(json_mapping(r['to'])['table']),
                json_str(json_mapping(r['from'])['column'])
            )
            for r in json_element_mappings(anvil_schema['relationships'])
            if json_mapping(r['from'])['table'] == table['name']
        ]
        for table in json_element_mappings(anvil_schema['tables'])
    }

    def verbatim_pfb_entity_id(self, replica: JSON) -> str:
        replica_type = replica['replica_type']
        contents = json_mapping(replica['contents'])
        try:
            primary_key = self.primary_keys_by_table[replica_type]
        except KeyError:
            if replica_type == 'duos_dataset_registration':
                return json_str(contents['duos_id'])
            else:
                return super().verbatim_pfb_entity_id(replica)
        else:
            return json_str(contents[primary_key])

    def verbatim_pfb_relations(self, replica: JSON) -> list[tuple[str, str]]:
        table_name = json_str(replica['replica_type'])
        contents = json_mapping(replica['contents'])
        try:
            foreign_keys = self.foreign_keys_by_table[table_name]
        except KeyError:
            if table_name == 'duos_dataset_registration':
                return [('anvil_dataset', json_str(contents['dataset_id']))]
            else:
                return super().verbatim_pfb_relations(replica)
        else:
            return [
                (foreign_table_name, foreign_key)
                for (foreign_table_name, foreign_key_column) in foreign_keys
                # AnVIL foreign keys may be either scalars (e.g. `anvil_diagnosis.donor_id`)
                # or arrays (e.g. `anvil_activity.used_file_id`). Scalar foreign keys may be
                # null; we should never observe null values in array columns thanks to
                # BigQuery's type semantics:
                # https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#array_nulls
                for foreign_key in always_iterable(contents[foreign_key_column])
            ]

    def verbatim_pfb_links(self, replica_type: str) -> MutableJSONs:
        return (
            [
                {
                    'dst': 'anvil_dataset',
                    'name': '',
                    'multiplicity': 'ONE_TO_ONE'
                }
            ]
            if replica_type == 'duos_dataset_registration' else
            [
                {
                    'dst': json_str(json_mapping(r['to'])['table']),
                    'name': json_str(r['name']),
                    # Each link is between a foreign key and a primary key.
                    # Primary keys are unique within their own table, but
                    # multiple rows in other tables can reference them.
                    'multiplicity': 'MANY_TO_ONE',
                }
                for r in json_element_mappings(anvil_schema['relationships'])
                if json_mapping(r['from'])['table'] == replica_type
            ]
        )

    def verbatim_pfb_schema(self, replicas: Iterable[JSON]) -> MutableJSONs:
        table_schemas_by_name = {
            json_str(schema['name']): schema
            for schema in json_element_mappings(anvil_schema['tables'])
        }
        non_schema_replicas = [
            r for r in replicas
            if r['replica_type'] not in table_schemas_by_name
        ]
        # For tables not described by the AnVIL schema, fall back to building
        # their PFB schema dynamically from the shapes of the replicas
        entity_schemas = super().verbatim_pfb_schema(non_schema_replicas)
        # For the rest, use the AnVIL schema as the basis of the PFB schema
        for table_name, table_schema in table_schemas_by_name.items():
            field_schemas: MutableJSONs = [
                self._pfb_schema_from_anvil_column(table_name=table_name,
                                                   column_name='datarepo_row_id',
                                                   anvil_datatype='string',
                                                   is_optional=False)
            ]
            if table_name == 'anvil_file':
                field_schemas.append(self._pfb_schema_from_anvil_column(table_name=table_name,
                                                                        column_name='drs_uri',
                                                                        anvil_datatype='string'))
            for column_schema in json_element_mappings(table_schema['columns']):
                field_schemas.append(
                    self._pfb_schema_from_anvil_column(table_name=table_name,
                                                       column_name=json_str(column_schema['name']),
                                                       anvil_datatype=json_str(column_schema['datatype']),
                                                       is_array=json_bool(column_schema['array_of']),
                                                       is_optional=json_bool(not column_schema['required']))
                )

            field_schemas.sort(key=itemgetter('name'))
            entity_schemas.append({
                'name': table_name,
                'type': 'record',
                # The cast is safe because `field_schemas` is reassigned in the
                # next loop iteration, or goes out of scope when the function
                # returns  after the loop exits. Mypy just doesn't realize that.
                'fields': cast(MutableJSONArray, field_schemas)
            })
        return entity_schemas

    def _pfb_schema_from_anvil_column(self,
                                      *,
                                      table_name: str,
                                      column_name: str,
                                      anvil_datatype: str,
                                      is_array: bool = False,
                                      is_optional: bool = True,
                                      ) -> MutableJSON:
        _anvil_to_pfb_types = {
            'boolean': 'boolean',
            'float': 'double',
            'integer': 'long',
            'string': 'string',
            'fileref': 'string'
        }
        type_: AnyMutableJSON = _anvil_to_pfb_types[anvil_datatype]
        if is_optional:
            type_ = ['null', type_]
        if is_array:
            type_ = {
                'type': 'array',
                'items': type_
            }
        return {
            'name': column_name,
            'namespace': table_name,
            'type': type_,
        }

    def document_slice(self, entity_type: str) -> DocumentSlice | None:
        return None

    @property
    def summary_response_stage(self) -> type[AnvilSummaryResponseStage]:
        return AnvilSummaryResponseStage

    @property
    def search_response_stage(self) -> type[AnvilSearchResponseStage]:
        return AnvilSearchResponseStage

    @property
    def summary_aggregation_stage(self) -> type[AnvilSummaryAggregationStage]:
        return AnvilSummaryAggregationStage

    @property
    def aggregation_stage(self) -> type[AnvilAggregationStage]:
        return AnvilAggregationStage

    @property
    def filter_stage(self) -> type[AnvilFilterStage]:
        return AnvilFilterStage

    @property
    def file_class(self) -> type[File]:
        return AnvilFile


@frozen(kw_only=True)
class AnvilFile(File):
    #: MD5 hash of the file's contents
    md5: str

    @classmethod
    def from_index(cls, hit: JSON, *, source: SourceRef) -> Self:
        return cls(uuid=json_str(hit['document_id']),
                   version=json_str(hit['version']),
                   name=json_str(hit['file_name']),
                   size=json_int(hit['file_size']),
                   drs_uri=optional(json_str, hit['drs_uri']),
                   source=source,
                   md5=json_str(hit['file_md5sum']))

    @property
    def digest(self) -> Digest:
        return Digest(value=self.md5, type='md5')
