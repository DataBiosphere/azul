from abc import (
    ABCMeta,
    abstractmethod,
)
from collections import (
    ChainMap,
    Counter,
    defaultdict,
)
from enum import (
    Enum,
)
import logging
import re
from typing import (
    Dict,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    get_args,
)
from uuid import (
    UUID,
    uuid5,
)

from humancellatlas.data.metadata import (
    api,
)
from more_itertools import (
    only,
)

from azul import (
    reject,
    require,
)
from azul.collections import (
    none_safe_key,
)
from azul.indexer import (
    Bundle,
    BundleFQID,
)
from azul.indexer.aggregate import (
    SimpleAggregator,
)
from azul.indexer.document import (
    Contribution,
    ContributionCoordinates,
    EntityReference,
    FieldType,
    FieldTypes,
    NullableString,
    PassThrough,
    null_bool,
    null_int,
    null_str,
    pass_thru_int,
    pass_thru_json,
)
from azul.indexer.transform import (
    Transformer,
)
from azul.plugins.metadata.hca.aggregate import (
    CellLineAggregator,
    CellSuspensionAggregator,
    DonorOrganismAggregator,
    FileAggregator,
    MatricesAggregator,
    OrganoidAggregator,
    ProjectAggregator,
    ProtocolAggregator,
    SampleAggregator,
    SequencingProcessAggregator,
    SpecimenAggregator,
)
from azul.plugins.metadata.hca.contributor_matrices import (
    parse_strata,
)
from azul.plugins.metadata.hca.full_metadata import (
    FullMetadata,
)
from azul.types import (
    JSON,
    JSONs,
    MutableJSON,
)

log = logging.getLogger(__name__)

Sample = Union[api.CellLine, api.Organoid, api.SpecimenFromOrganism]
sample_types = api.CellLine, api.Organoid, api.SpecimenFromOrganism
assert get_args(Sample) == sample_types  # since we can't use * in generic types

pass_thru_uuid4: PassThrough[api.UUID4] = PassThrough(es_type='string')


class ValueAndUnit(FieldType[JSON, str]):
    # FIXME: change the es_type for JSON to `nested`
    #        https://github.com/DataBiosphere/azul/issues/2621
    es_type = 'string'

    def to_index(self, value_unit: Optional[JSON]) -> str:
        """
        >>> a = ValueAndUnit()
        >>> a.to_index({'value': '20', 'unit': 'year'})
        '20 year'

        >>> a.to_index({'value': '20', 'unit': None})
        '20'

        >>> a.to_index(None)
        '~null'

        >>> a.to_index({})
        Traceback (most recent call last):
        ...
        azul.RequirementError: A dictionary with entries for `value` and `unit` is required

        >>> a.to_index({'value': '1', 'unit': 'day', 'foo': 12})
        Traceback (most recent call last):
        ...
        azul.RequirementError: A dictionary with exactly two entries is required

        >>> a.to_index({'unit': 'day'})
        Traceback (most recent call last):
        ...
        azul.RequirementError: A dictionary with entries for `value` and `unit` is required

        >>> a.to_index({'value': '1'})
        Traceback (most recent call last):
        ...
        azul.RequirementError: A dictionary with entries for `value` and `unit` is required

        >>> a.to_index({'value': '', 'unit': 'year'})
        Traceback (most recent call last):
        ...
        azul.RequirementError: The `value` entry must not be empty

        >>> a.to_index({'value': '20', 'unit': ''})
        Traceback (most recent call last):
        ...
        azul.RequirementError: The `unit` entry must not be empty

        >>> a.to_index({'value': None, 'unit': 'years'})
        Traceback (most recent call last):
        ...
        azul.RequirementError: The `value` entry must not be null

        >>> a.to_index({'value': 20, 'unit': None})
        Traceback (most recent call last):
        ...
        azul.RequirementError: The `value` entry must be a string

        >>> a.to_index({'value': '20', 'unit': True})
        Traceback (most recent call last):
        ...
        azul.RequirementError: The `unit` entry must be a string

        >>> a.to_index({'value': '20 ', 'unit': None})
        Traceback (most recent call last):
        ...
        azul.RequirementError: The `value` entry must not contain space characters

        >>> a.to_index({'value': '20', 'unit': 'years '})
        Traceback (most recent call last):
        ...
        azul.RequirementError: The `unit` entry must not contain space characters
        """
        if value_unit is None:
            return NullableString.null_string
        else:
            try:
                value, unit = value_unit['value'], value_unit['unit']
            except KeyError:
                reject(True, 'A dictionary with entries for `value` and `unit` is required')
            else:
                require(len(value_unit) == 2, 'A dictionary with exactly two entries is required')
                reject(value == '', 'The `value` entry must not be empty')
                reject(unit == '', 'The `unit` entry must not be empty')
                reject(value is None, 'The `value` entry must not be null')
                require(type(value) is str, 'The `value` entry must be a string')
                reject(' ' in value, 'The `value` entry must not contain space characters')
                if unit is None:
                    return value
                else:
                    require(type(unit) is str, 'The `unit` entry must be a string')
                    reject(' ' in unit, 'The `unit` entry must not contain space characters')
                    return f'{value} {unit}'

    def from_index(self, value: str) -> Optional[JSON]:
        """
        >>> a = ValueAndUnit()
        >>> a.from_index('20 year')
        {'value': '20', 'unit': 'year'}

        >>> a.from_index('20')
        {'value': '20', 'unit': None}

        >>> a.from_index('~null') is None
        True

        Although 'year' looks like a unit, we intentionally treat it like a
        value because this class does not enforce any constraints on value or
        unit other than it not contain spaces.

        >>> a.from_index('year')
        {'value': 'year', 'unit': None}

        >>> a.from_index('20  ')
        Traceback (most recent call last):
        ...
        ValueError: too many items in iterable (expected 1)

        >>> a.from_index(' year')
        Traceback (most recent call last):
        ...
        AssertionError

        >>> a.from_index('1 ')
        Traceback (most recent call last):
        ...
        AssertionError

        >>> a.from_index('')
        Traceback (most recent call last):
        ...
        AssertionError
        """
        if value == NullableString.null_string:
            return None
        else:
            i = iter(value.split(' '))
            value = next(i)
            # only() fails with more than one item left in the iterator
            unit = only(i)
            assert value, value
            assert unit is None or unit, unit
            return {'value': value, 'unit': unit}


value_and_unit: ValueAndUnit = ValueAndUnit()


class SubmitterCategory(Enum):
    """
    The types of submitters and the types of metadata entities that describe
    the files they submit.
    """
    internal = api.SupplementaryFile, api.AnalysisFile
    external = api.SupplementaryFile

    def __init__(self, *file_types: Type[api.File]) -> None:
        super().__init__()
        self.file_types = file_types


class SubmitterBase:
    # These class attributes must be defined in a superclass because Enum and
    # EnumMeta would get confused if they were defined in the Enum subclass.
    by_id: Dict[str, 'Submitter'] = {}
    id_namespace = UUID('382415e5-67a6-49be-8f3c-aaaa707d82db')


class Submitter(SubmitterBase, Enum):
    """
    The known submitters of data files, specifically matrix files.
    """
    # A submitter's ID is derived from its slug. We hard-code it for the sake of
    # documenting it. The constructor ensures the hard-coded value is correct.

    arrayexpress = (
        'b7525d8e-8c7a-5fec-911a-323e5c3a79f7',
        'Array Express',
        SubmitterCategory.external
    )
    contributor = (
        'f180f1c3-9073-54a9-9bab-633008c307cc',
        'Contributor',
        SubmitterCategory.external
    )
    geo = (
        '21b9424e-4043-5e80-85d0-1f0449430b57',
        'GEO',
        SubmitterCategory.external
    )
    hca_release = (
        '656db407-02f1-547c-9840-6908c4f09ce8',
        'HCA Release',
        SubmitterCategory.external
    )
    scea = (
        '099feafe-ab42-5fb1-bff5-dbbe5ea61a0d',
        'SCEA',
        SubmitterCategory.external
    )
    scp = (
        '3d76d2d3-51f4-5b17-85c8-f3549a7ab716',
        'SCP',
        SubmitterCategory.external
    )
    dcp2 = (
        'e67aaabe-93ea-564a-aa66-31bc0857b707',
        'DCP/2 Analysis',
        SubmitterCategory.internal
    )
    dcp1_matrix_service = (
        'c9efbb15-c50c-5796-8d15-35e9e1219dc5',
        'DCP/1 Matrix Service',
        SubmitterCategory.internal
    )
    lungmap_external = (
        'fedbcffc-4ebc-54f7-8a21-fc63836ef8bb',
        'LungMAP',
        SubmitterCategory.external
    )

    def __init__(self, id: str, title: str, category: SubmitterCategory):
        super().__init__()
        slug = self.name.replace('_', ' ')
        generated_uuid = str(uuid5(self.id_namespace, slug))
        assert id == generated_uuid, (id, generated_uuid)
        self.id = id
        self.slug = slug
        self.title = title
        self.category = category
        self.by_id[self.id] = self

    @classmethod
    def for_id(cls, submitter_id: str) -> Optional['Submitter']:
        try:
            return cls.by_id[submitter_id]
        except KeyError:
            return None

    @classmethod
    def for_file(cls, file: api.File) -> Optional['Submitter']:
        return cls.for_id(file.submitter_id)

    @classmethod
    def title_for_id(cls, submitter_id: str) -> Optional[str]:
        """
        Return the human-readable version of the name that was used to generate
        the submitter UUID.
        """
        self = cls.for_id(submitter_id)
        if self is None:
            return None
        else:
            return self.title

    @classmethod
    def category_for_file(cls, file: api.File) -> Optional[SubmitterCategory]:
        self = cls.for_file(file)
        if self is None:
            return None
        else:
            require(isinstance(file, self.category.file_types), file, self)
            return self.category


class BaseTransformer(Transformer, metaclass=ABCMeta):

    @classmethod
    def create(cls, bundle: Bundle, deleted: bool) -> Transformer:
        return cls(bundle, deleted)

    def __init__(self, bundle: Bundle, deleted: bool) -> None:
        super().__init__()
        self.deleted = deleted
        self.bundle = bundle
        self.api_bundle = api.Bundle(uuid=bundle.uuid,
                                     version=bundle.version,
                                     manifest=bundle.manifest,
                                     metadata_files=bundle.metadata_files)

    @classmethod
    def get_aggregator(cls, entity_type):
        if entity_type == 'files':
            return FileAggregator()
        elif entity_type == 'samples':
            return SampleAggregator()
        elif entity_type == 'specimens':
            return SpecimenAggregator()
        elif entity_type == 'cell_suspensions':
            return CellSuspensionAggregator()
        elif entity_type == 'cell_lines':
            return CellLineAggregator()
        elif entity_type == 'donors':
            return DonorOrganismAggregator()
        elif entity_type == 'organoids':
            return OrganoidAggregator()
        elif entity_type == 'projects':
            return ProjectAggregator()
        elif entity_type in (
            'analysis_protocols',
            'imaging_protocols',
            'library_preparation_protocols',
            'sequencing_protocols'
        ):
            return ProtocolAggregator()
        elif entity_type == 'sequencing_processes':
            return SequencingProcessAggregator()
        elif entity_type in ('matrices', 'contributor_matrices'):
            return MatricesAggregator()
        else:
            return SimpleAggregator()

    def _find_ancestor_samples(self, entity: api.LinkedEntity, samples: MutableMapping[str, Sample]):
        """
        Populate the `samples` argument with the sample ancestors of the given entity. A sample is any biomaterial
        that is neither a cell suspension nor an ancestor of another sample.
        """
        if isinstance(entity, sample_types):
            samples[str(entity.document_id)] = entity
        else:
            for parent in entity.parents.values():
                self._find_ancestor_samples(parent, samples)

    def _visit_file(self, file):
        visitor = TransformerVisitor()
        file.accept(visitor)
        file.ancestors(visitor)
        samples: MutableMapping[str, Sample] = dict()
        self._find_ancestor_samples(file, samples)
        return visitor, samples

    @classmethod
    def _contact_types(cls) -> FieldTypes:
        return {
            'contact_name': null_str,
            'corresponding_contributor': null_bool,
            'email': null_str,
            'institution': null_str,
            'laboratory': null_str,
            'project_role': null_str
        }

    def _contact(self, p: api.ProjectContact):
        # noinspection PyDeprecation
        return {
            "contact_name": p.contact_name,
            "corresponding_contributor": p.corresponding_contributor,
            "email": p.email,
            "institution": p.institution,
            "laboratory": p.laboratory,
            "project_role": p.project_role
        }

    @classmethod
    def _publication_types(cls) -> FieldTypes:
        return {
            'publication_title': null_str,
            'publication_url': null_str
        }

    def _publication(self, p: api.ProjectPublication):
        # noinspection PyDeprecation
        return {
            "publication_title": p.publication_title,
            "publication_url": p.publication_url
        }

    @classmethod
    def _project_types(cls) -> FieldTypes:
        return {
            'project_title': null_str,
            'project_description': null_str,
            'project_short_name': null_str,
            'laboratory': null_str,
            'institutions': null_str,
            'contact_names': null_str,
            'contributors': cls._contact_types(),
            'document_id': null_str,
            'publication_titles': null_str,
            'publications': cls._publication_types(),
            'insdc_project_accessions': null_str,
            'geo_series_accessions': null_str,
            'array_express_accessions': null_str,
            'insdc_study_accessions': null_str,
            'supplementary_links': null_str,
            '_type': null_str
        }

    def _project(self, project: api.Project) -> MutableJSON:
        # Store lists of all values of each of these facets to allow facet filtering
        # and term counting on the webservice
        laboratories: Set[str] = set()
        institutions: Set[str] = set()
        contact_names: Set[str] = set()
        publication_titles: Set[str] = set()

        for contributor in project.contributors:
            if contributor.laboratory:
                laboratories.add(contributor.laboratory)
            # noinspection PyDeprecation
            if contributor.contact_name:
                # noinspection PyDeprecation
                contact_names.add(contributor.contact_name)
            if contributor.institution:
                institutions.add(contributor.institution)

        for publication in project.publications:
            # noinspection PyDeprecation
            if publication.publication_title:
                # noinspection PyDeprecation
                publication_titles.add(publication.publication_title)

        return {
            'project_title': project.project_title,
            'project_description': project.project_description,
            'project_short_name': project.project_short_name,
            'laboratory': sorted(laboratories),
            'institutions': sorted(institutions),
            'contact_names': sorted(contact_names),
            'contributors': list(map(self._contact, project.contributors)),
            'document_id': str(project.document_id),
            'publication_titles': sorted(publication_titles),
            'publications': list(map(self._publication, project.publications)),
            'insdc_project_accessions': sorted(project.insdc_project_accessions),
            'geo_series_accessions': sorted(project.geo_series_accessions),
            'array_express_accessions': sorted(project.array_express_accessions),
            'insdc_study_accessions': sorted(project.insdc_study_accessions),
            'supplementary_links': sorted(project.supplementary_links),
            '_type': 'project'
        }

    @classmethod
    def _specimen_types(cls) -> FieldTypes:
        return {
            'has_input_biomaterial': null_str,
            '_source': null_str,
            'document_id': null_str,
            'biomaterial_id': null_str,
            'disease': null_str,
            'organ': null_str,
            'organ_part': null_str,
            'storage_method': null_str,
            'preservation_method': null_str,
            '_type': null_str
        }

    def _specimen(self, specimen: api.SpecimenFromOrganism) -> MutableJSON:
        return {
            'has_input_biomaterial': specimen.has_input_biomaterial,
            '_source': api.schema_names[type(specimen)],
            'document_id': str(specimen.document_id),
            'biomaterial_id': specimen.biomaterial_id,
            'disease': sorted(specimen.diseases),
            'organ': specimen.organ,
            'organ_part': sorted(specimen.organ_parts),
            'storage_method': specimen.storage_method,
            'preservation_method': specimen.preservation_method,
            '_type': 'specimen'
        }

    @classmethod
    def _cell_suspension_types(cls) -> FieldTypes:
        return {
            'document_id': null_str,
            'biomaterial_id': null_str,
            'total_estimated_cells': null_int,
            'selected_cell_type': null_str,
            'organ': null_str,
            'organ_part': null_str
        }

    def _cell_suspension(self, cell_suspension: api.CellSuspension) -> MutableJSON:
        organs = set()
        organ_parts = set()
        samples: MutableMapping[str, Sample] = dict()
        self._find_ancestor_samples(cell_suspension, samples)
        for sample in samples.values():
            if isinstance(sample, api.SpecimenFromOrganism):
                organs.add(sample.organ)
                organ_parts.update(sample.organ_parts)
            elif isinstance(sample, api.CellLine):
                organs.add(sample.model_organ)
                organ_parts.add(None)
            elif isinstance(sample, api.Organoid):
                organs.add(sample.model_organ)
                organ_parts.add(sample.model_organ_part)
            else:
                assert False
        return {
            'document_id': str(cell_suspension.document_id),
            'biomaterial_id': str(cell_suspension.biomaterial_id),
            'total_estimated_cells': cell_suspension.estimated_cell_count,
            'selected_cell_type': sorted(cell_suspension.selected_cell_types),
            'organ': sorted(organs),
            # With multiple samples it is possible to have str and None values
            'organ_part': sorted(organ_parts, key=none_safe_key(none_last=True))
        }

    @classmethod
    def _cell_line_types(cls) -> FieldTypes:
        return {
            'document_id': null_str,
            'biomaterial_id': null_str,
            'cell_line_type': null_str,
            'model_organ': null_str
        }

    def _cell_line(self, cell_line: api.CellLine) -> MutableJSON:
        # noinspection PyDeprecation
        return {
            'document_id': str(cell_line.document_id),
            'biomaterial_id': cell_line.biomaterial_id,
            'cell_line_type': cell_line.cell_line_type,
            'model_organ': cell_line.model_organ
        }

    @classmethod
    def _donor_types(cls) -> FieldTypes:
        return {
            'document_id': null_str,
            'biomaterial_id': null_str,
            'biological_sex': null_str,
            'genus_species': null_str,
            'development_stage': null_str,
            'diseases': null_str,
            'organism_age': value_and_unit,
            'organism_age_unit': null_str,
            'organism_age_value': null_str,
            # Prevent problem due to shadow copies on numeric ranges
            'organism_age_range': pass_thru_json,
            'donor_count': null_int
        }

    def _donor(self, donor: api.DonorOrganism) -> MutableJSON:
        if donor.organism_age is None:
            require(donor.organism_age_unit is None)
            organism_age = None
        else:
            organism_age = {
                'value': donor.organism_age,
                'unit': donor.organism_age_unit
            }
        return {
            'document_id': str(donor.document_id),
            'biomaterial_id': donor.biomaterial_id,
            'biological_sex': donor.sex,
            'genus_species': sorted(donor.genus_species),
            'development_stage': donor.development_stage,
            'diseases': sorted(donor.diseases),
            'organism_age': organism_age,
            'organism_age_value': donor.organism_age,
            'organism_age_unit': donor.organism_age_unit,
            **(
                {
                    'organism_age_range': {
                        'gte': donor.organism_age_in_seconds.min,
                        'lte': donor.organism_age_in_seconds.max
                    }
                } if donor.organism_age_in_seconds else {
                }
            )
        }

    @classmethod
    def _organoid_types(cls) -> FieldTypes:
        return {
            'document_id': null_str,
            'biomaterial_id': null_str,
            'model_organ': null_str,
            'model_organ_part': null_str
        }

    def _organoid(self, organoid: api.Organoid) -> MutableJSON:
        return {
            'document_id': str(organoid.document_id),
            'biomaterial_id': organoid.biomaterial_id,
            'model_organ': organoid.model_organ,
            'model_organ_part': organoid.model_organ_part
        }

    @classmethod
    def _file_types(cls) -> FieldTypes:
        return {
            'content-type': null_str,
            'indexed': null_bool,
            'name': null_str,
            'crc32c': null_str,
            'sha256': null_str,
            'size': null_int,
            # Pass through field added by FileAggregator, will never be None
            'count': pass_thru_int,
            'uuid': pass_thru_uuid4,
            'drs_path': null_str,
            'version': null_str,
            'document_id': null_str,
            'file_type': null_str,
            'file_format': null_str,
            'content_description': null_str,
            'source': null_str,
            '_type': null_str,
            'related_files': cls._related_file_types(),
            'read_index': null_str,
            'lane_index': null_int
        }

    def _file(self, file: api.File, related_files: Iterable[api.File] = ()) -> MutableJSON:
        # noinspection PyDeprecation
        return {
            'content-type': file.manifest_entry.content_type,
            'indexed': file.manifest_entry.indexed,
            'name': file.manifest_entry.name,
            'crc32c': file.manifest_entry.crc32c,
            'sha256': file.manifest_entry.sha256,
            'size': file.manifest_entry.size,
            'uuid': file.manifest_entry.uuid,
            'drs_path': self.bundle.drs_path(file.manifest_entry.json),
            'version': file.manifest_entry.version,
            'document_id': str(file.document_id),
            'file_type': file.schema_name,
            'file_format': file.file_format,
            'content_description': sorted(file.content_description),
            'source': Submitter.title_for_id(file.submitter_id),
            '_type': 'file',
            'related_files': list(map(self._related_file, related_files)),
            **(
                {
                    'read_index': file.read_index,
                    'lane_index': file.lane_index
                } if isinstance(file, api.SequenceFile) else {
                }
            ),
        }

    @classmethod
    def _related_file_types(cls) -> FieldTypes:
        return {
            'name': null_str,
            'crc32c': null_str,
            'sha256': null_str,
            'size': null_int,
            'uuid': pass_thru_uuid4,
            'version': null_str,
        }

    def _related_file(self, file: api.File) -> MutableJSON:
        return {
            'name': file.manifest_entry.name,
            'crc32c': file.manifest_entry.crc32c,
            'sha256': file.manifest_entry.sha256,
            'size': file.manifest_entry.size,
            'uuid': file.manifest_entry.uuid,
            'version': file.manifest_entry.version,
        }

    @classmethod
    def _analysis_protocol_types(cls) -> FieldTypes:
        return {
            'document_id': null_str,
            'workflow': null_str
        }

    def _analysis_protocol(self, protocol: api.AnalysisProtocol) -> MutableJSON:
        return {
            'document_id': protocol.document_id,
            'workflow': protocol.protocol_id
        }

    @classmethod
    def _imaging_protocol_types(cls) -> FieldTypes:
        return {
            'document_id': null_str,
            # Pass through counter used to produce a FrequencySetAccumulator
            'assay_type': pass_thru_json
        }

    def _imaging_protocol(self, protocol: api.ImagingProtocol) -> MutableJSON:
        return {
            'document_id': protocol.document_id,
            'assay_type': dict(Counter(target.assay_type for target in protocol.target))
        }

    @classmethod
    def _library_preparation_protocol_types(cls) -> FieldTypes:
        return {
            'document_id': null_str,
            'library_construction_approach': null_str,
            'nucleic_acid_source': null_str
        }

    def _library_preparation_protocol(self, protocol: api.LibraryPreparationProtocol) -> MutableJSON:
        return {
            'document_id': protocol.document_id,
            'library_construction_approach': protocol.library_construction_method,
            'nucleic_acid_source': protocol.nucleic_acid_source
        }

    @classmethod
    def _sequencing_protocol_types(cls) -> FieldTypes:
        return {
            'document_id': null_str,
            'instrument_manufacturer_model': null_str,
            'paired_end': null_bool
        }

    def _sequencing_protocol(self, protocol: api.SequencingProtocol) -> MutableJSON:
        return {
            'document_id': protocol.document_id,
            'instrument_manufacturer_model': protocol.instrument_manufacturer_model,
            'paired_end': protocol.paired_end
        }

    @classmethod
    def _sequencing_process_types(cls) -> FieldTypes:
        return {
            'document_id': null_str,
        }

    def _sequencing_process(self, process: api.Process) -> MutableJSON:
        return {
            'document_id': str(process.document_id),
        }

    @classmethod
    def _sequencing_input_types(cls) -> FieldTypes:
        return {
            'document_id': null_str,
            'biomaterial_id': null_str,
            'sequencing_input_type': null_str,
        }

    def _sequencing_input(self, sequencing_input: api.Biomaterial) -> MutableJSON:
        return {
            'document_id': str(sequencing_input.document_id),
            'biomaterial_id': sequencing_input.biomaterial_id,
            'sequencing_input_type': api.schema_names[type(sequencing_input)]
        }

    @classmethod
    def _sample_types(cls) -> FieldTypes:
        return {
            'entity_type': null_str,
            'effective_organ': null_str,
            **cls._cell_line_types(),
            **cls._organoid_types(),
            **cls._specimen_types()
        }

    def _sample(self, sample: api.Biomaterial) -> MutableJSON:
        # Start construction of a `sample` inner entity by including all fields
        # possible from any entities that can be a sample. This is done to
        # have consistency of fields between various sample inner entities
        # to allow Elasticsearch to search and sort against these entities.
        sample_ = dict.fromkeys(ChainMap(
            self._cell_line_types(),
            self._organoid_types(),
            self._specimen_types()
        ).keys())
        entity_type, entity_dict = (
            'cell_lines', self._cell_line(sample)
        ) if isinstance(sample, api.CellLine) else (
            'organoids', self._organoid(sample)
        ) if isinstance(sample, api.Organoid) else (
            'specimens', self._specimen(sample)
        ) if isinstance(sample, api.SpecimenFromOrganism) else (
            require(False, sample), None
        )
        sample_.update(entity_dict)
        sample_['entity_type'] = entity_type
        assert hasattr(sample, 'organ') != hasattr(sample, 'model_organ')
        sample_['effective_organ'] = sample.organ if hasattr(sample, 'organ') else sample.model_organ
        assert sample_['document_id'] == str(sample.document_id)
        assert sample_['biomaterial_id'] == sample.biomaterial_id
        return sample_

    @classmethod
    def _matrices_types(cls) -> FieldTypes:
        return {
            'document_id': null_str,
            # Pass through dict with file properties, will never be None
            'file': pass_thru_json,
        }

    def _matrices(self, file: api.File) -> MutableJSON:
        if isinstance(file, api.SupplementaryFile):
            # Stratification values for supplementary files are
            # provided in the 'file_description' field of the file JSON.
            strata_string = file.json['file_description']
        else:
            # Stratification values for analysis files are gathered by
            # visiting the file and using values from the graph.
            strata_string = self._build_strata_string(file)
        return {
            'document_id': str(file.document_id),
            # These values are grouped together in a dict so when the dicts are
            # aggregated together we will have preserved the grouping of values.
            'file': {
                'uuid': str(file.manifest_entry.uuid),
                'version': file.manifest_entry.version,
                'name': file.manifest_entry.name,
                'strata': strata_string
            }
        }

    dimension_value_re = re.compile(r'[^,=;\n]+')

    def _build_strata_string(self, file):
        visitor, samples = self._visit_file(file)
        points = {
            'genusSpecies': {
                genus_species
                for donor in visitor.donors.values()
                for genus_species in donor.genus_species
            },
            'developmentStage': {
                donor.development_stage
                for donor in visitor.donors.values()
                if donor.development_stage is not None
            },
            'organ': {
                sample.organ if hasattr(sample, 'organ') else sample.model_organ
                for sample in samples.values()
            },
            'libraryConstructionApproach': {
                protocol.library_construction_method
                for protocol in visitor.library_preparation_protocols.values()
            }
        }
        point_strings = []
        for dimension, values in points.items():
            if values:
                for value in values:
                    assert self.dimension_value_re.fullmatch(value), value
                point_strings.append(dimension + '=' + ','.join(values))
        return ';'.join(point_strings)

    def _get_project(self, bundle) -> api.Project:
        project, *additional_projects = bundle.projects.values()
        reject(additional_projects, "Azul can currently only handle a single project per bundle")
        assert isinstance(project, api.Project)
        return project

    def _contribution(self, contents: MutableJSON, entity_id: api.UUID4) -> Contribution:
        entity = EntityReference(entity_type=self.entity_type(),
                                 entity_id=str(entity_id))
        bundle_fqid = BundleFQID(uuid=str(self.api_bundle.uuid),
                                 version=self.api_bundle.version)
        coordinates = ContributionCoordinates(entity=entity,
                                              bundle=bundle_fqid,
                                              deleted=self.deleted)
        return Contribution(coordinates=coordinates,
                            version=None,
                            contents=contents)

    @classmethod
    def field_types(cls) -> FieldTypes:
        return {
            'samples': cls._sample_types(),
            'sequencing_inputs': cls._sequencing_input_types(),
            'specimens': cls._specimen_types(),
            'cell_suspensions': cls._cell_suspension_types(),
            'cell_lines': cls._cell_line_types(),
            'donors': cls._donor_types(),
            'organoids': cls._organoid_types(),
            'files': cls._file_types(),
            'analysis_protocols': cls._analysis_protocol_types(),
            'imaging_protocols': cls._imaging_protocol_types(),
            'library_preparation_protocols': cls._library_preparation_protocol_types(),
            'sequencing_protocols': cls._sequencing_protocol_types(),
            'sequencing_processes': cls._sequencing_process_types(),
            'total_estimated_cells': pass_thru_int,
            'matrices': cls._matrices_types(),
            'contributor_matrices': cls._matrices_types(),
            'projects': cls._project_types()
        }

    def _protocols(self, visitor) -> Mapping[str, JSONs]:
        return {
            p + 's': list(map(getattr(self, '_' + p), getattr(visitor, p + 's').values()))
            for p in (
                'analysis_protocol',
                'imaging_protocol',
                'library_preparation_protocol',
                'sequencing_protocol'
            )
        }


def _parse_zarr_file_name(file_name: str) -> Tuple[bool, Optional[str], Optional[str]]:
    file_name = file_name.split('.zarr/')
    if len(file_name) == 1:
        return False, None, None
    elif len(file_name) == 2:
        zarr_name, sub_name = file_name
        return True, zarr_name, sub_name
    else:
        assert False


class TransformerVisitor(api.EntityVisitor):
    # Entities are tracked by ID to ensure uniqueness if an entity is visited twice while descending the entity DAG
    specimens: MutableMapping[api.UUID4, api.SpecimenFromOrganism]
    cell_suspensions: MutableMapping[api.UUID4, api.CellSuspension]
    cell_lines: MutableMapping[api.UUID4, api.CellLine]
    donors: MutableMapping[api.UUID4, api.DonorOrganism]
    organoids: MutableMapping[api.UUID4, api.Organoid]
    analysis_protocols: MutableMapping[api.UUID4, api.AnalysisProtocol]
    imaging_protocols: MutableMapping[api.UUID4, api.ImagingProtocol]
    library_preparation_protocols: MutableMapping[api.UUID4, api.LibraryPreparationProtocol]
    sequencing_protocols: MutableMapping[api.UUID4, api.SequencingProtocol]
    sequencing_processes: MutableMapping[api.UUID4, api.Process]
    files: MutableMapping[api.UUID4, api.File]

    def __init__(self) -> None:
        self.specimens = {}
        self.cell_suspensions = {}
        self.cell_lines = {}
        self.donors = {}
        self.organoids = {}
        self.analysis_protocols = {}
        self.imaging_protocols = {}
        self.library_preparation_protocols = {}
        self.sequencing_protocols = {}
        self.sequencing_processes = {}
        self.files = {}

    def visit(self, entity: api.Entity) -> None:
        if isinstance(entity, api.SpecimenFromOrganism):
            self.specimens[entity.document_id] = entity
        elif isinstance(entity, api.CellSuspension):
            self.cell_suspensions[entity.document_id] = entity
        elif isinstance(entity, api.CellLine):
            self.cell_lines[entity.document_id] = entity
        elif isinstance(entity, api.DonorOrganism):
            self.donors[entity.document_id] = entity
        elif isinstance(entity, api.Organoid):
            self.organoids[entity.document_id] = entity
        elif isinstance(entity, api.Process):
            if entity.is_sequencing_process():
                self.sequencing_processes[entity.document_id] = entity
            for protocol in entity.protocols.values():
                if isinstance(protocol, api.AnalysisProtocol):
                    self.analysis_protocols[protocol.document_id] = protocol
                elif isinstance(protocol, api.ImagingProtocol):
                    self.imaging_protocols[protocol.document_id] = protocol
                elif isinstance(protocol, api.LibraryPreparationProtocol):
                    self.library_preparation_protocols[protocol.document_id] = protocol
                elif isinstance(protocol, api.SequencingProtocol):
                    self.sequencing_protocols[protocol.document_id] = protocol
        elif isinstance(entity, api.File):
            # noinspection PyDeprecation
            file_name = entity.manifest_entry.name
            is_zarr, zarr_name, sub_name = _parse_zarr_file_name(file_name)
            # FIXME: Remove condition once https://github.com/HumanCellAtlas/metadata-schema/issues/623 is resolved
            if not is_zarr or sub_name.endswith('.zattrs'):
                self.files[entity.document_id] = entity


class FileTransformer(BaseTransformer):

    @classmethod
    def entity_type(cls) -> str:
        return 'files'

    def transform(self) -> Iterable[Contribution]:
        project = self._get_project(self.api_bundle)
        zarr_stores: Mapping[str, List[api.File]] = self.group_zarrs(self.api_bundle.files.values())
        for file in self.api_bundle.files.values():
            file_name = file.manifest_entry.name
            is_zarr, zarr_name, sub_name = _parse_zarr_file_name(file_name)
            # FIXME: Remove condition once https://github.com/HumanCellAtlas/metadata-schema/issues/579 is resolved
            if not is_zarr or sub_name.endswith('.zattrs'):
                if is_zarr:
                    # This is the representative file, so add the related files
                    related_files = zarr_stores[zarr_name]
                else:
                    related_files = ()
                visitor, samples = self._visit_file(file)
                contents = dict(samples=list(map(self._sample, samples.values())),
                                sequencing_inputs=list(map(self._sequencing_input, self.api_bundle.sequencing_input)),
                                specimens=list(map(self._specimen, visitor.specimens.values())),
                                cell_suspensions=list(map(self._cell_suspension, visitor.cell_suspensions.values())),
                                cell_lines=list(map(self._cell_line, visitor.cell_lines.values())),
                                donors=list(map(self._donor, visitor.donors.values())),
                                organoids=list(map(self._organoid, visitor.organoids.values())),
                                files=[self._file(file, related_files=related_files)],
                                **self._protocols(visitor),
                                sequencing_processes=list(
                                    map(self._sequencing_process, visitor.sequencing_processes.values())
                                ),
                                projects=[self._project(project)])
                # Supplementary file matrices provide stratification values that
                # need to be reflected by inner entities in the contribution.
                if isinstance(file, api.SupplementaryFile):
                    if Submitter.for_file(file) is not None:
                        additional_contents = self.matrix_stratification_values(file)
                        for entity_type, values in additional_contents.items():
                            contents[entity_type].extend(values)
                yield self._contribution(contents, file.document_id)

    def matrix_stratification_values(self, file: api.File) -> JSON:
        """
        Returns inner entity values (contents) read from the stratification
        values provided by a supplementary file project-level matrix.
        """
        contents = defaultdict(list)
        file_description = file.json.get('file_description')
        if file_description:
            file_name = file.manifest_entry.name
            strata = parse_strata(file_description)
            for stratum in strata:
                donors = {}
                genus_species = stratum.get('genusSpecies')
                if genus_species is not None:
                    donors['genus_species'] = sorted(genus_species)
                development_stage = stratum.get('developmentStage')
                if development_stage is not None:
                    donors['development_stage'] = sorted(development_stage)
                if donors:
                    donors['biomaterial_id'] = f'donor_organism_{file_name}'
                    contents['donors'].append(donors)
                organ = stratum.get('organ')
                if organ is not None:
                    organ = sorted(organ)
                    contents['samples'].append(
                        {
                            'biomaterial_id': f'specimen_from_organism_{file_name}',
                            'entity_type': 'specimens',
                            'organ': organ,
                            'effective_organ': organ,
                        }
                    )
                library = stratum.get('libraryConstructionApproach')
                if library is not None:
                    contents['library_preparation_protocols'].append(
                        {
                            'library_construction_approach': sorted(library)
                        }
                    )
        return contents

    def group_zarrs(self, files: Iterable[api.File]) -> Mapping[str, List[api.File]]:
        zarr_stores = defaultdict(list)
        for file in files:
            file_name = file.manifest_entry.name
            is_zarr, zarr_name, sub_name = _parse_zarr_file_name(file_name)
            if is_zarr:
                # Leave the representative file out of the list since it's already in the manifest
                if not sub_name.startswith('.zattrs'):
                    zarr_stores[zarr_name].append(file)
        return zarr_stores


class CellSuspensionTransformer(BaseTransformer):

    @classmethod
    def entity_type(cls) -> str:
        return 'cell_suspensions'

    def transform(self) -> Iterable[Contribution]:
        project = self._get_project(self.api_bundle)
        for cell_suspension in self.api_bundle.biomaterials.values():
            if isinstance(cell_suspension, api.CellSuspension):
                samples: MutableMapping[str, Sample] = dict()
                self._find_ancestor_samples(cell_suspension, samples)
                visitor = TransformerVisitor()
                cell_suspension.accept(visitor)
                cell_suspension.ancestors(visitor)
                contents = dict(samples=list(map(self._sample, samples.values())),
                                sequencing_inputs=list(map(self._sequencing_input, self.api_bundle.sequencing_input)),
                                specimens=list(map(self._specimen, visitor.specimens.values())),
                                cell_suspensions=[self._cell_suspension(cell_suspension)],
                                cell_lines=list(map(self._cell_line, visitor.cell_lines.values())),
                                donors=list(map(self._donor, visitor.donors.values())),
                                organoids=list(map(self._organoid, visitor.organoids.values())),
                                files=list(map(self._file, visitor.files.values())),
                                **self._protocols(visitor),
                                sequencing_processes=list(
                                    map(self._sequencing_process, visitor.sequencing_processes.values())
                                ),
                                projects=[self._project(project)])
                yield self._contribution(contents, cell_suspension.document_id)


class SampleTransformer(BaseTransformer):

    @classmethod
    def entity_type(cls) -> str:
        return 'samples'

    def transform(self) -> Iterable[Contribution]:
        project = self._get_project(self.api_bundle)
        samples: MutableMapping[str, Sample] = dict()
        for file in self.api_bundle.files.values():
            self._find_ancestor_samples(file, samples)
        for sample in samples.values():
            visitor = TransformerVisitor()
            sample.accept(visitor)
            sample.ancestors(visitor)
            contents = dict(samples=[self._sample(sample)],
                            sequencing_inputs=list(map(self._sequencing_input, self.api_bundle.sequencing_input)),
                            specimens=list(map(self._specimen, visitor.specimens.values())),
                            cell_suspensions=list(map(self._cell_suspension, visitor.cell_suspensions.values())),
                            cell_lines=list(map(self._cell_line, visitor.cell_lines.values())),
                            donors=list(map(self._donor, visitor.donors.values())),
                            organoids=list(map(self._organoid, visitor.organoids.values())),
                            files=list(map(self._file, visitor.files.values())),
                            **self._protocols(visitor),
                            sequencing_processes=list(
                                map(self._sequencing_process, visitor.sequencing_processes.values())
                            ),
                            projects=[self._project(project)])
            yield self._contribution(contents, sample.document_id)


class BundleProjectTransformer(BaseTransformer, metaclass=ABCMeta):

    @abstractmethod
    def _get_entity_id(self, project: api.Project) -> api.UUID4:
        raise NotImplementedError

    def transform(self) -> Iterable[Contribution]:
        # Project entities are not explicitly linked in the graph. The mere presence of project metadata in a bundle
        # indicates that all other entities in that bundle belong to that project. Because of that we can't rely on a
        # visitor to collect the related entities but have to enumerate the explicitly:
        #
        visitor = TransformerVisitor()
        for specimen in self.api_bundle.specimens:
            specimen.accept(visitor)
            specimen.ancestors(visitor)
        samples: MutableMapping[str, Sample] = dict()
        for file in self.api_bundle.files.values():
            file.accept(visitor)
            file.ancestors(visitor)
            self._find_ancestor_samples(file, samples)
        project = self._get_project(self.api_bundle)

        contents = dict(samples=list(map(self._sample, samples.values())),
                        sequencing_inputs=list(map(self._sequencing_input, self.api_bundle.sequencing_input)),
                        specimens=list(map(self._specimen, visitor.specimens.values())),
                        cell_suspensions=list(map(self._cell_suspension, visitor.cell_suspensions.values())),
                        cell_lines=list(map(self._cell_line, visitor.cell_lines.values())),
                        donors=list(map(self._donor, visitor.donors.values())),
                        organoids=list(map(self._organoid, visitor.organoids.values())),
                        files=list(map(self._file, visitor.files.values())),
                        **self._protocols(visitor),
                        sequencing_processes=list(
                            map(self._sequencing_process, visitor.sequencing_processes.values())
                        ),
                        matrices=[
                            self._matrices(file)
                            for file in visitor.files.values()
                            if Submitter.category_for_file(file) == SubmitterCategory.internal
                        ],
                        contributor_matrices=[
                            self._matrices(file)
                            for file in visitor.files.values()
                            if Submitter.category_for_file(file) == SubmitterCategory.external
                        ],
                        projects=[self._project(project)])

        yield self._contribution(contents, self._get_entity_id(project))


class ProjectTransformer(BundleProjectTransformer):

    def _get_entity_id(self, project: api.Project) -> api.UUID4:
        return project.document_id

    @classmethod
    def entity_type(cls) -> str:
        return 'projects'


class BundleTransformer(BundleProjectTransformer):

    def __init__(self, bundle: Bundle, deleted: bool) -> None:
        super().__init__(bundle, deleted)
        if 'project.json' in bundle.metadata_files:
            # we can't handle v5 bundles
            self.metadata = []
        else:
            full_metadata = FullMetadata()
            full_metadata.add_bundle(bundle)
            self.metadata = full_metadata.dump()

    def _get_entity_id(self, project: api.Project) -> api.UUID4:
        return self.api_bundle.uuid

    @classmethod
    def get_aggregator(cls, entity_type):
        if entity_type in ('files', 'metadata'):
            return None
        else:
            return super().get_aggregator(entity_type)

    @classmethod
    def entity_type(cls) -> str:
        return 'bundles'

    def _contribution(self, contents: MutableJSON, entity_id: api.UUID4) -> Contribution:
        contents['metadata'] = self.metadata
        return super()._contribution(contents, entity_id)

    @classmethod
    def field_types(cls) -> FieldTypes:
        return {
            **super().field_types(),
            'metadata': pass_thru_json  # Exclude full metadata from translation
        }
