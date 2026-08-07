import logging
import re
from typing import (
    Iterable,
    Self,
    Sequence,
)

from attrs import (
    frozen,
)

from azul import (
    CatalogName,
    config,
)
from azul.drs import (
    HostBasedDRSURI,
)
from azul.field_type import (
    pass_thru_str,
)
from azul.indexer.document import (
    Aggregate,
    DocumentType,
    EntityType,
    IndexName,
)
from azul.lib import (
    R,
)
from azul.lib.digests import (
    Digest,
)
from azul.lib.functions import (
    iif,
)
from azul.lib.types import (
    JSON,
    MutableJSON,
    json_dict,
    json_dict_of_dicts,
    json_int,
    json_list,
    json_mapping,
    json_str,
    optional,
)
from azul.plugins import (
    DocumentSlice,
    File,
    InverseFieldMapping,
    ManifestConfig,
    ManifestFormat,
    MetadataPlugin,
    Sorting,
    SpecialField,
    SpecialFields,
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
from azul.source import (
    SourceRef,
)
from humancellatlas.data.metadata import (
    api,
)

log = logging.getLogger(__name__)


class Plugin(MetadataPlugin[HCABundle]):

    def transformer_types(self) -> Iterable[type[BaseTransformer]]:
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
                                manifest=json_dict_of_dicts(bundle.manifest),
                                metadata=json_dict_of_dicts(bundle.metadata),
                                links_json=bundle.links,
                                stitched_entity_ids=bundle.stitched)

        def transformers():
            for transformer_cls in self.transformer_types():
                yield transformer_cls(bundle=bundle, api_bundle=api_bundle, deleted=delete)

        return list(transformers())

    def aggregate_class(self) -> type[Aggregate]:
        return HCAAggregate

    def mapping(self, index_name: IndexName) -> MutableJSON:
        mapping = super().mapping(index_name)
        if index_name.doc_type in (DocumentType.contribution, DocumentType.aggregate):
            json_dict(mapping['properties'])['contents'] = {
                'properties': {
                    'projects': {
                        'properties': {
                            'accessions': {
                                'type': 'nested'
                            },
                            'tissue_atlas': {
                                'type': 'nested'
                            }
                        }
                    }
                }
            }
            json_list(mapping['dynamic_templates'])[0:0] = [
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
            bundles=Sorting(field_name=self.special_fields.bundle_version.name,
                            descending=True,
                            max_page_size=100),
            files=Sorting(field_name='fileName'),
            projects=Sorting(field_name='projectTitle',
                             max_page_size=75),
            samples=Sorting(field_name='sampleId')
        )

    @property
    def manifest_formats(self) -> Sequence[ManifestFormat]:
        return [
            ManifestFormat.compact,
            ManifestFormat.terra_pfb,
            ManifestFormat.curl,
            *iif(config.enable_replicas, [
                ManifestFormat.verbatim_jsonl,
                ManifestFormat.verbatim_pfb
            ])
        ]

    @property
    def _field_mapping(self) -> InverseFieldMapping:
        # FIXME: Detect invalid values in field mapping
        #        https://github.com/DataBiosphere/azul/issues/3071
        return {
            'entity_id': 'entryId',
            'bundles': {
                'uuid': self.special_fields.bundle_uuid.name,
                'version': self.special_fields.bundle_version.name
            },
            'sources': {
                'id': self.special_fields.source_id.name,
                'spec': self.special_fields.source_spec.name
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
                    'uuid': self.special_fields.file_uuid.name,
                    'version': 'fileVersion',
                    'content_description': 'contentDescription',
                    'matrix_cell_count': 'matrixCellCount',
                    'is_intermediate': 'isIntermediate',
                    'sha256': 'sha256',
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
                    'estimated_cell_count': 'projectEstimatedCellCount',
                    'is_tissue_atlas_project': 'isTissueAtlasProject',
                    'tissue_atlas': 'tissueAtlas',
                    'bionetwork_name': 'bionetworkName',
                    'data_use_restriction': 'dataUseRestriction',
                    'duos_id': 'duosId'
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

    special_fields = SpecialFields(
        source_id=SpecialField.symmetric('sourceId', pass_thru_str),
        source_spec=SpecialField.symmetric('sourceSpec', pass_thru_str),
        source_prefix=SpecialField.symmetric('sourcePrefix', pass_thru_str),
        bundle_uuid=SpecialField.symmetric('bundleUuid', pass_thru_str),
        bundle_version=SpecialField.symmetric('bundleVersion', pass_thru_str),
        file_uuid=SpecialField(name='fileId', name_in_hit='uuid', type=pass_thru_str),
        file_name=SpecialField(name='fileName', name_in_hit='name', type=pass_thru_str)
    )

    @property
    def root_entity_type(self) -> str:
        return 'projects'

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
            'publicationTitle',
            'isTissueAtlasProject',
            'tissueAtlas',
            'bionetworkName',
            'dataUseRestriction'
        ]

    @property
    def manifest_config(self) -> ManifestConfig:
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
                'file_url': 'file_azul_url',
                'file_mirror_uri': 'file_mirror_uri',
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

    def document_slice(self, entity_type: str) -> DocumentSlice | None:
        if entity_type in ('files', 'bundles'):
            return None
        else:
            return DocumentSlice(excludes=['bundles'])

    @property
    def summary_response_stage(self) -> type[HCASummaryResponseStage]:
        return HCASummaryResponseStage

    @property
    def search_response_stage(self) -> type[HCASearchResponseStage]:
        return HCASearchResponseStage

    @property
    def summary_aggregation_stage(self) -> type[HCASummaryAggregationStage]:
        return HCASummaryAggregationStage

    @property
    def aggregation_stage(self) -> type[HCAAggregationStage]:
        return HCAAggregationStage

    @property
    def filter_stage(self) -> type[HCAFilterStage]:
        return HCAFilterStage

    @property
    def file_class(self) -> type[File]:
        return HCAFile


@frozen(kw_only=True)
class HCAFile(File):
    #: Various checksums of the file's content
    sha256: str
    # crc32c is required by the HCA API, but we allow it to be None for mirroring
    crc32c: str | None = None
    sha1: str | None = None
    s3_etag: str | None = None

    @classmethod
    def from_index(cls, hit: JSON, *, source: SourceRef) -> Self:
        return cls(uuid=json_str(hit['uuid']),
                   version=json_str(hit['version']),
                   name=json_str(hit['name']),
                   size=json_int(hit['size']),
                   drs_uri=optional(json_str, hit['drs_uri']),
                   content_type=json_str(hit['content-type']),
                   source=source,
                   sha256=json_str(hit['sha256']),
                   crc32c=json_str(hit['crc32c']),
                   sha1=optional(json_str, hit.get('sha1')),
                   s3_etag=optional(json_str, hit.get('s3_etag')))

    @classmethod
    def _parse_drs_uri(cls, file_id: str | None, descriptor: JSON) -> str | None:
        if file_id is None:
            try:
                external_drs_uri = optional(json_str, descriptor['drs_uri'])
            except KeyError:
                assert False, R(
                    '`file_id` is null and `drs_uri` is not set in file descriptor',
                    descriptor)
            else:
                return external_drs_uri
        else:
            parsed = HostBasedDRSURI.parse(file_id)
            assert parsed.server == config.tdr_service_url.netloc, R(
                'DRS URI must point to TDR as the server', file_id)
            return file_id

    @classmethod
    def from_metadata(cls,
                      *,
                      catalog: CatalogName,
                      metadata: JSON,
                      descriptor: JSON,
                      drs_uri: str | None,
                      source: SourceRef,
                      ) -> Self:
        # FIXME: Move validation of descriptor to the metadata API
        #        https://github.com/DataBiosphere/azul/issues/6299
        api.Entity.validate_described_by(descriptor)
        drs_uri = cls._parse_drs_uri(drs_uri, descriptor)
        content_type = json_str(descriptor['content_type'])
        # FIXME: Obsolete MIME parameter in file content types
        #        https://github.com/HumanCellAtlas/dcp2/issues/73
        parameter_suffix = '; dcp-type=data'
        if content_type.endswith(parameter_suffix):
            content_type = content_type.removesuffix(parameter_suffix)

        atlas = config.catalogs[catalog].atlas
        if atlas == 'hca':
            assert ';' not in content_type, R(
                'Unexpected MIME parameter in content type', content_type)
            content_type = cls._content_type_corrections.get(content_type, content_type)
            assert cls._content_type_re.match(content_type), R(
                'Invalid content type', content_type)
        elif atlas == 'lungmap':
            # FIXME: Re-enable content-type validation for lungmap
            #        https://github.com/DataBiosphere/azul/issues/7244
            pass
        else:
            assert False, atlas

        return cls(uuid=json_str(descriptor['file_id']),
                   name=json_str(json_mapping(metadata['file_core'])['file_name']),
                   version=json_str(descriptor['file_version']),
                   size=json_int(descriptor['size']),
                   content_type=content_type,
                   sha256=json_str(descriptor['sha256']),
                   crc32c=json_str(descriptor['crc32c']),
                   sha1=optional(json_str, descriptor.get('sha1')),
                   s3_etag=optional(json_str, descriptor.get('s3_etag')),
                   drs_uri=drs_uri,
                   source=source)

    @property
    def digest(self) -> Digest:
        return Digest(value=self.sha256, type='sha256')

    def to_manifest_entry(self) -> JSON:
        entry = self.to_json()
        entry.pop(self.discriminator())
        entry.pop('source')
        entry['content-type'] = entry.pop('content_type')
        entry['indexed'] = False
        return entry

    _content_type_corrections = {
        # Only correct for files created with Excel 2007 or later. Hopefully we
        # don't have any files older than that.
        'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    }

    # We use the stricter character set and 127-char limit from
    # https://datatracker.ietf.org/doc/html/rfc6838#section-4.2, for the media
    # type and subtype, but since this RFC does not describe MIME parameters or
    # what character to use for the joiner, we draw those specifications from
    # https://datatracker.ietf.org/doc/html/rfc7231#section-3.1.1.5
    _restricted_name_re = r'[a-zA-Z0-9]([a-zA-Z0-9!#$&^_.+-]{,125}[a-zA-Z0-9!#$&^_-])?'
    _token_re = r'[a-zA-Z0-9!#$%&\'*+.^_`|~-]+'
    _qstr_re = r'"[\t !#-[\]-~\x80-\xff]*"'
    _content_type_re = re.compile(
        fr'^{_restricted_name_re}/{_restricted_name_re}'
        fr'([\t ]*;[\t ]*{_token_re}=(({_token_re})|({_qstr_re})))?$'
    )
