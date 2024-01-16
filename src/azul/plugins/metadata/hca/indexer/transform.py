from abc import (
    ABCMeta,
    abstractmethod,
)
from collections import (
    Counter,
    defaultdict,
)
from collections.abc import (
    Iterable,
    Iterator,
    Mapping,
)
from datetime import (
    datetime,
)
from enum import (
    Enum,
)
import logging
import re
from typing import (
    Callable,
    Generic,
    Optional,
    Protocol,
    Type,
    TypeVar,
    Union,
    get_args,
)
from uuid import (
    UUID,
    uuid5,
)

import attr
from more_itertools import (
    ilen,
    one,
    only,
)

from azul import (
    cached_property,
    config,
    reject,
    require,
)
from azul.collections import (
    OrderedSet,
    none_safe_key,
)
from azul.enums import (
    auto,
)
from azul.indexer import (
    BundleFQID,
    BundlePartition,
)
from azul.indexer.aggregate import (
    EntityAggregator,
    SimpleAggregator,
)
from azul.indexer.document import (
    ClosedRange,
    Contribution,
    EntityID,
    EntityReference,
    EntityType,
    FieldType,
    FieldTypes,
    Nested,
    NullableString,
    PassThrough,
    null_bool,
    null_datetime,
    null_int,
    null_str,
    pass_thru_float,
    pass_thru_int,
    pass_thru_json,
)
from azul.indexer.transform import (
    Transform,
    Transformer,
)
from azul.iterators import (
    generable,
)
from azul.openapi import (
    schema,
)
from azul.plugins.metadata.hca.bundle import (
    HCABundle,
)
from azul.plugins.metadata.hca.indexer.aggregate import (
    CellLineAggregator,
    CellSuspensionAggregator,
    DateAggregator,
    DonorOrganismAggregator,
    FileAggregator,
    MatricesAggregator,
    OrganoidAggregator,
    ProjectAggregator,
    ProtocolAggregator,
    SampleAggregator,
    SequencingInputAggregator,
    SequencingProcessAggregator,
    SpecimenAggregator,
)
from azul.plugins.metadata.hca.service.contributor_matrices import (
    parse_strata,
)
from azul.time import (
    format_dcp2_datetime,
    parse_dcp2_version,
)
from azul.types import (
    JSON,
    JSONs,
    MutableJSON,
)
from humancellatlas.data.metadata import (
    api,
)

log = logging.getLogger(__name__)

Sample = Union[api.CellLine, api.Organoid, api.SpecimenFromOrganism]
sample_types = api.CellLine, api.Organoid, api.SpecimenFromOrganism
assert get_args(Sample) == sample_types  # since we can't use * in generic types

pass_thru_uuid4: PassThrough[api.UUID4] = PassThrough(str, es_type='keyword')


def _format_dcp2_datetime(d: Optional[datetime]) -> Optional[str]:
    return None if d is None else format_dcp2_datetime(d)


class ValueAndUnit(FieldType[JSON, str]):
    # FIXME: change the es_type for JSON to `nested`
    #        https://github.com/DataBiosphere/azul/issues/2621
    es_type = 'keyword'

    def to_index(self, value_unit: Optional[JSON]) -> str:
        """
        >>> a = ValueAndUnit(JSON, str)
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
        >>> a = ValueAndUnit(JSON, str)
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
        ValueError: Expected exactly one item in iterable, but got '', '', and perhaps more.

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

    def to_tsv(self, value: Optional[JSON]) -> str:
        return '' if value is None else self.to_index(value)

    @property
    def api_schema(self) -> JSON:
        return schema.object(value=str, unit=str)


value_and_unit: ValueAndUnit = ValueAndUnit(JSON, str)

accession: Nested = Nested(namespace=null_str, accession=null_str)

age_range = ClosedRange(pass_thru_float)


class SubmitterCategory(Enum):
    """
    The types of submitters, such as internal (submitter of DCP generated
    matrices) and external (submitter of contributor generated matrices).
    """
    internal = auto()
    external = auto()


class SubmitterBase:
    # These class attributes must be defined in a superclass because Enum and
    # EnumMeta would get confused if they were defined in the Enum subclass.
    by_id: dict[str, 'Submitter'] = {}
    by_title: dict[str, 'Submitter'] = {}
    id_namespace = UUID('382415e5-67a6-49be-8f3c-aaaa707d82db')


class Submitter(SubmitterBase, Enum):
    """
    The known submitters of data files, specifically matrix files.
    """
    # A submitter's ID is derived from its slug. We hard-code it for the sake of
    # documenting it. The constructor ensures the hard-coded value is correct.

    arrayexpress = (
        'b7525d8e-8c7a-5fec-911a-323e5c3a79f7',
        'ArrayExpress',
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
    dcp2_ingest = (
        '8d59f7a5-6245-5e42-9bc0-a53dd8a10f28',
        'DCP/2 Ingest',
        SubmitterCategory.internal
    )
    dcp1_matrix_service = (
        'c9efbb15-c50c-5796-8d15-35e9e1219dc5',
        'DCP/1 Matrix Service',
        SubmitterCategory.internal
    )
    lungmap = (
        '31ad7d2c-7262-54aa-92df-7f16418f3b84',
        'LungMAP',
        SubmitterCategory.external
    )
    zenodo = (
        'bd24572b-a535-5ff8-b167-0e43d7f0d4b0',
        'Zenodo',
        SubmitterCategory.external
    )
    publication = (
        '210ca4c7-f6f6-5a0d-8b1c-88ab5349a8f3',
        'Publication',
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
        assert title not in self.by_title, title
        self.by_title[title] = self
        self.by_id[id] = self

    @classmethod
    def for_id(cls, submitter_id: str) -> Optional['Submitter']:
        try:
            return cls.by_id[submitter_id]
        except KeyError:
            return None

    @classmethod
    def for_file(cls, file: api.File) -> Optional['Submitter']:
        if file.file_source is None:
            if (
                # The DCP/2 system design specification mistakenly required that
                # intermediate matrices generated by the DCP/2 Analysis do not
                # carry any submitter_id:
                #
                # > Any intermediate matrices created during the processing are
                # > described as analysis_file, but the
                # > analysis_file.provenance.submitter_id property is omitted.
                #
                # https://github.com/HumanCellAtlas/dcp2/blob/main/docs/dcp2_system_design.rst#52dcp2-generated-matrices
                #
                # This heuristic attempts to retroactively assign the `dcp2`
                # submitter ID to all analysis files produced by DCP/2 Analysis,
                # not just intermediate matrices but also BAMs and other
                # intermediate files.
                file.submitter_id is None
                and isinstance(file, api.AnalysisFile)
                and any(isinstance(p, api.AnalysisProcess)
                        for p in file.from_processes.values())
            ):
                self = cls.dcp2
            else:
                self = cls.for_id(file.submitter_id)
        else:
            self = cls.by_title[file.file_source]
        return self

    @classmethod
    def title_for_file(cls, file: api.File) -> Optional[str]:
        self = cls.for_file(file)
        return None if self is None else self.title

    @classmethod
    def category_for_file(cls, file: api.File) -> Optional[SubmitterCategory]:
        self = cls.for_file(file)
        if self is None:
            return None
        else:
            return self.category


class Entity(Protocol):
    document_id: api.UUID4


class DatedEntity(Entity, Protocol):
    submission_date: datetime
    update_date: datetime


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class BaseTransformer(Transformer, metaclass=ABCMeta):
    bundle: HCABundle
    api_bundle: api.Bundle

    def replica_type(self, entity: EntityReference) -> str:
        assert entity.entity_type == self.entity_type(), entity
        return entity.entity_type.removesuffix('s')

    @classmethod
    def aggregator(cls, entity_type: EntityType) -> Optional[EntityAggregator]:
        if entity_type == 'files':
            return FileAggregator()
        elif entity_type in SampleTransformer.inner_entity_types():
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
        elif entity_type == 'sequencing_inputs':
            return SequencingInputAggregator()
        elif entity_type == 'sequencing_processes':
            return SequencingProcessAggregator()
        elif entity_type in ('matrices', 'contributed_analyses'):
            return MatricesAggregator()
        elif entity_type == 'dates':
            return DateAggregator()
        else:
            return SimpleAggregator()

    def _add_replica(self,
                     contribution: MutableJSON,
                     entity: Union[api.Entity, DatedEntity]
                     ) -> Transform:
        entity_ref = EntityReference(entity_id=str(entity.document_id),
                                     entity_type=self.entity_type())
        if not config.enable_replicas or self.entity_type() == 'bundles':
            replica = None
        else:
            assert isinstance(entity, api.Entity), entity
            replica = self._replica(entity.json, entity_ref)
        return (
            self._contribution(contribution, entity_ref),
            replica
        )

    def _find_ancestor_samples(self,
                               entity: api.LinkedEntity,
                               samples: dict[str, Sample]
                               ):
        """
        Populate the `samples` argument with the sample ancestors of the given
        entity. A sample is any biomaterial that is neither a cell suspension
        nor an ancestor of another sample.

        :param entity: the entity whose ancestor samples should be found

        :param samples: the dictionary into which to place found ancestor
                        samples, by their document ID
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
        samples: dict[str, Sample] = dict()
        self._find_ancestor_samples(file, samples)
        return visitor, samples

    def __dates(self, entity: DatedEntity) -> MutableJSON:
        dates = (entity.submission_date, entity.update_date)
        last_modified_date = max(filter(None, dates))
        return {
            'submission_date': format_dcp2_datetime(entity.submission_date),
            'update_date': _format_dcp2_datetime(entity.update_date),
            'last_modified_date': format_dcp2_datetime(last_modified_date)
        }

    def __aggregate_dates(self, entities: Iterable[DatedEntity]) -> MutableJSON:
        submission_dates = {entity.submission_date for entity in entities}
        update_dates = {entity.update_date for entity in entities}
        dates = submission_dates | update_dates
        agg_last_modified_date = max(filter(None, dates), default=None)
        agg_submission_date = min(submission_dates, default=None)
        agg_update_date = max(filter(None, update_dates), default=None)
        return {
            'aggregate_last_modified_date': _format_dcp2_datetime(agg_last_modified_date),
            'aggregate_submission_date': _format_dcp2_datetime(agg_submission_date),
            'aggregate_update_date': _format_dcp2_datetime(agg_update_date),
        }

    @classmethod
    def _date_types(cls) -> FieldTypes:
        return {
            **cls._entity_types(),
            'aggregate_last_modified_date': null_datetime,
            'aggregate_submission_date': null_datetime,
            'aggregate_update_date': null_datetime,
            'submission_date': null_datetime,
            'update_date': null_datetime,
            'last_modified_date': null_datetime,
        }

    def _date(self, entity: DatedEntity) -> MutableJSON:
        return {
            **self._entity(entity),
            **self.__dates(entity),
            **self.__aggregate_dates(self._dated_entities())
        }

    def _dated_entities(self) -> Iterable[DatedEntity]:
        # Only containers have dated entities
        return []

    @classmethod
    def _entity_types(cls) -> FieldTypes:
        return {
            'document_id': null_str,
        }

    def _entity(self, entity: Entity):
        return {
            'document_id': str(entity.document_id),
        }

    @classmethod
    def _biomaterial_types(cls) -> FieldTypes:
        return {
            **cls._entity_types(),
            'biomaterial_id': null_str,
        }

    def _biomaterial(self, biomaterial: api.Biomaterial):
        return {
            **self._entity(biomaterial),
            'biomaterial_id': str(biomaterial.biomaterial_id),
        }

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
            'contact_name': p.contact_name,
            'corresponding_contributor': p.corresponding_contributor,
            'email': p.email,
            'institution': p.institution,
            'laboratory': p.laboratory,
            'project_role': p.project_role
        }

    @classmethod
    def _publication_types(cls) -> FieldTypes:
        return {
            'publication_title': null_str,
            'publication_url': null_str,
            'official_hca_publication': null_bool,
            'doi': null_str
        }

    def _publication(self, p: api.ProjectPublication):
        # noinspection PyDeprecation
        return {
            'publication_title': p.publication_title,
            'publication_url': p.publication_url,
            'official_hca_publication': p.official_hca,
            'doi': p.doi
        }

    def _accession(self, p: api.Accession):
        return {
            'namespace': p.namespace,
            'accession': p.accession
        }

    @classmethod
    def _project_types(cls) -> FieldTypes:
        return {
            **cls._entity_types(),
            'project_title': null_str,
            'project_description': null_str,
            'project_short_name': null_str,
            'laboratory': [null_str],
            'institutions': [null_str],
            'contact_names': [null_str],
            'contributors': cls._contact_types(),
            'publication_titles': [null_str],
            'publications': cls._publication_types(),
            'supplementary_links': [null_str],
            '_type': null_str,
            'accessions': [accession],
            'estimated_cell_count': null_int
        }

    def _project(self, project: api.Project) -> MutableJSON:
        # Store lists of all values of each of these facets to allow facet filtering
        # and term counting on the webservice
        laboratories: OrderedSet[str] = OrderedSet()
        institutions: OrderedSet[str] = OrderedSet()
        contact_names: OrderedSet[str] = OrderedSet()
        publication_titles: OrderedSet[str] = OrderedSet()

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
            **self._entity(project),
            'project_title': project.project_title,
            # FIXME: Omit large project fields from non-project contributions
            #        https://github.com/DataBiosphere/azul/issues/5346
            'project_description': project.project_description,
            'project_short_name': project.project_short_name,
            'laboratory': list(laboratories),
            'institutions': list(institutions),
            'contact_names': list(contact_names),
            'contributors': list(map(self._contact, project.contributors)),
            'publication_titles': list(publication_titles),
            'publications': list(map(self._publication, project.publications)),
            'supplementary_links': sorted(project.supplementary_links),
            '_type': 'project',
            'accessions': list(map(self._accession, project.accessions)),
            'estimated_cell_count': project.estimated_cell_count
        }

    @classmethod
    def _specimen_types(cls) -> FieldTypes:
        return {
            **cls._biomaterial_types(),
            'has_input_biomaterial': null_str,
            '_source': null_str,
            'disease': [null_str],
            'organ': null_str,
            'organ_part': [null_str],
            'storage_method': null_str,
            'preservation_method': null_str,
            '_type': null_str
        }

    def _specimen(self, specimen: api.SpecimenFromOrganism) -> MutableJSON:
        return {
            **self._biomaterial(specimen),
            'has_input_biomaterial': specimen.has_input_biomaterial,
            '_source': api.schema_names[type(specimen)],
            'disease': sorted(specimen.diseases),
            'organ': specimen.organ,
            'organ_part': sorted(specimen.organ_parts),
            'storage_method': specimen.storage_method,
            'preservation_method': specimen.preservation_method,
            '_type': 'specimen'
        }

    cell_count_fields = [
        ('total_estimated_cells', True),
        ('total_estimated_cells_redundant', False)
    ]

    @classmethod
    def _cell_suspension_types(cls) -> FieldTypes:
        return {
            **cls._biomaterial_types(),
            **{field: null_int for field, _ in cls.cell_count_fields},
            'selected_cell_type': [null_str],
            'organ': [null_str],
            'organ_part': [null_str]
        }

    def _cell_suspension(self, cell_suspension: api.CellSuspension) -> MutableJSON:
        organs = set()
        organ_parts = set()
        samples: dict[str, Sample] = dict()
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
        is_leaf = cell_suspension.document_id in self.api_bundle.leaf_cell_suspensions
        return {
            **self._biomaterial(cell_suspension),
            **{
                field: cell_suspension.estimated_cell_count if is_leaf_field == is_leaf else 0
                for field, is_leaf_field in self.cell_count_fields
            },
            'selected_cell_type': sorted(cell_suspension.selected_cell_types),
            'organ': sorted(organs),
            # With multiple samples it is possible to have str and None values
            'organ_part': sorted(organ_parts, key=none_safe_key(none_last=True))
        }

    @classmethod
    def _cell_line_types(cls) -> FieldTypes:
        return {
            **cls._biomaterial_types(),
            'cell_line_type': null_str,
            'model_organ': null_str
        }

    def _cell_line(self, cell_line: api.CellLine) -> MutableJSON:
        # noinspection PyDeprecation
        return {
            **self._biomaterial(cell_line),
            'cell_line_type': cell_line.cell_line_type,
            'model_organ': cell_line.model_organ
        }

    @classmethod
    def _donor_types(cls) -> FieldTypes:
        return {
            **cls._biomaterial_types(),
            'biological_sex': null_str,
            'genus_species': [null_str],
            'development_stage': null_str,
            'diseases': [null_str],
            'organism_age': value_and_unit,
            # Prevent problem due to shadow copies on numeric ranges
            'organism_age_range': age_range,
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
            **self._biomaterial(donor),
            'biological_sex': donor.sex,
            'genus_species': sorted(donor.genus_species),
            'development_stage': donor.development_stage,
            'diseases': sorted(donor.diseases),
            'organism_age': organism_age,
            **(
                {
                    'organism_age_range': (
                        donor.organism_age_in_seconds.min,
                        donor.organism_age_in_seconds.max
                    )
                } if donor.organism_age_in_seconds else {
                }
            )
        }

    @classmethod
    def _organoid_types(cls) -> FieldTypes:
        return {
            **cls._biomaterial_types(),
            'model_organ': null_str,
            'model_organ_part': null_str
        }

    def _organoid(self, organoid: api.Organoid) -> MutableJSON:
        return {
            **self._biomaterial(organoid),
            'model_organ': organoid.model_organ,
            'model_organ_part': organoid.model_organ_part
        }

    def _is_intermediate_matrix(self, file: api.File) -> Optional[bool]:
        if file.is_matrix:
            if isinstance(file, api.SupplementaryFile):
                # Non-organic CGM
                is_intermediate = False
            elif isinstance(file, api.AnalysisFile):
                if (
                    any(isinstance(p, api.AnalysisProcess) for p in file.to_processes.values())
                    # As per DCP/2 System Design, intermediate matrices generated by
                    # DCP/2 analysis do not carry a submitter ID. Also see Submitter.for_file
                    or (file.submitter_id is None and Submitter.for_file(file) == Submitter.dcp2)
                ):
                    # Intermediate DCP/2-generated matrix
                    is_intermediate = True
                else:
                    # Organic CGM or final DCP/2-generated matrix
                    is_intermediate = False
            else:
                assert False, file
        else:
            # Not a matrix
            is_intermediate = None
        return is_intermediate

    @classmethod
    def _file_base_types(cls) -> FieldTypes:
        return {
            **cls._entity_types(),
            'content-type': null_str,
            'indexed': null_bool,
            'name': null_str,
            'crc32c': null_str,
            'sha256': null_str,
            'size': null_int,
            'uuid': pass_thru_uuid4,
            'drs_uri': null_str,
            'version': null_str,
            'file_type': null_str,
            'file_format': null_str,
            'content_description': [null_str],
            'is_intermediate': null_bool,
            'file_source': null_str,
            '_type': null_str,
            'read_index': null_str,
            'lane_index': null_int,
            'matrix_cell_count': null_int
        }

    def _file_base(self, file: api.File) -> MutableJSON:
        # noinspection PyDeprecation
        return {
            **self._entity(file),
            'content-type': file.manifest_entry.content_type,
            'indexed': file.manifest_entry.indexed,
            'name': file.manifest_entry.name,
            'crc32c': file.manifest_entry.crc32c,
            'sha256': file.manifest_entry.sha256,
            'size': file.manifest_entry.size,
            'uuid': file.manifest_entry.uuid,
            'drs_uri': self.bundle.drs_uri(file.manifest_entry.json),
            'version': file.manifest_entry.version,
            'file_type': file.schema_name,
            'file_format': file.file_format,
            'content_description': sorted(file.content_description),
            'is_intermediate': self._is_intermediate_matrix(file),
            'file_source': Submitter.title_for_file(file),
            '_type': 'file',
            **(
                {
                    'read_index': file.read_index,
                    'lane_index': file.lane_index
                } if isinstance(file, api.SequenceFile) else {
                }
            ),
            **(
                {
                    'matrix_cell_count': file.matrix_cell_count
                } if isinstance(file, api.AnalysisFile) else {
                }
            ),
        }

    @classmethod
    def _file_types(cls) -> FieldTypes:
        return {
            **cls._file_base_types(),
            # Pass through field added by FileAggregator, will never be None
            'count': pass_thru_int,
            'related_files': cls._related_file_types(),
        }

    def _file(self,
              file: api.File,
              related_files: Iterable[api.File] = ()
              ) -> MutableJSON:
        # noinspection PyDeprecation
        return {
            **self._file_base(file),
            'related_files': list(map(self._related_file, related_files)),
        }

    @classmethod
    def _related_file_types(cls) -> FieldTypes:
        return cls._file_base_types()

    def _related_file(self, file: api.File) -> MutableJSON:
        return self._file_base(file)

    @classmethod
    def _analysis_protocol_types(cls) -> FieldTypes:
        return {
            **cls._entity_types(),
            'workflow': null_str
        }

    def _analysis_protocol(self, protocol: api.AnalysisProtocol) -> MutableJSON:
        return {
            **self._entity(protocol),
            'workflow': protocol.protocol_id
        }

    @classmethod
    def _imaging_protocol_types(cls) -> FieldTypes:
        return {
            **cls._entity_types(),
            # Pass through counter used to produce a FrequencySetAccumulator
            'assay_type': pass_thru_json
        }

    def _imaging_protocol(self, protocol: api.ImagingProtocol) -> MutableJSON:
        return {
            **self._entity(protocol),
            'assay_type': dict(Counter(probe.assay_type for probe in protocol.probe))
        }

    @classmethod
    def _library_preparation_protocol_types(cls) -> FieldTypes:
        return {
            **cls._entity_types(),
            'library_construction_approach': null_str,
            'nucleic_acid_source': null_str
        }

    def _library_preparation_protocol(self,
                                      protocol: api.LibraryPreparationProtocol
                                      ) -> MutableJSON:
        return {
            **self._entity(protocol),
            'library_construction_approach': protocol.library_construction_method,
            'nucleic_acid_source': protocol.nucleic_acid_source
        }

    @classmethod
    def _sequencing_protocol_types(cls) -> FieldTypes:
        return {
            **cls._entity_types(),
            'instrument_manufacturer_model': null_str,
            'paired_end': null_bool
        }

    def _sequencing_protocol(self, protocol: api.SequencingProtocol) -> MutableJSON:
        return {
            **self._entity(protocol),
            'instrument_manufacturer_model': protocol.instrument_manufacturer_model,
            'paired_end': protocol.paired_end
        }

    @classmethod
    def _sequencing_process_types(cls) -> FieldTypes:
        return {
            **cls._entity_types(),
        }

    def _sequencing_process(self, process: api.Process) -> MutableJSON:
        return {
            **self._entity(process),
        }

    @classmethod
    def _sequencing_input_types(cls) -> FieldTypes:
        return {
            **cls._biomaterial_types(),
            'sequencing_input_type': null_str,
        }

    def _sequencing_input(self, sequencing_input: api.Biomaterial) -> MutableJSON:
        return {
            **self._biomaterial(sequencing_input),
            'sequencing_input_type': api.schema_names[type(sequencing_input)]
        }

    @classmethod
    def _sample_types(cls) -> FieldTypes:
        return {
            **cls._biomaterial_types(),
            'entity_type': null_str,
            'organ': null_str,
            'organ_part': [null_str],
            'model_organ': null_str,
            'model_organ_part': null_str,
            'effective_organ': null_str,
        }

    class Sample:
        entity_type: str
        api_class: Type[api.Biomaterial]

        @classmethod
        def to_dict(cls, sample: api.Biomaterial) -> MutableJSON:
            assert isinstance(sample, cls.api_class)
            return {
                'document_id': sample.document_id,
                'biomaterial_id': sample.biomaterial_id,
                'entity_type': cls.entity_type,
            }

    class SampleCellLine(Sample):
        entity_type = 'cell_lines'
        api_class = api.CellLine

        @classmethod
        def to_dict(cls, cellline: api_class) -> MutableJSON:
            return {
                **super().to_dict(cellline),
                'organ': None,
                'organ_part': [],
                'model_organ': cellline.model_organ,
                'model_organ_part': None,
                'effective_organ': cellline.model_organ,
            }

    class SampleOrganoid(Sample):
        entity_type = 'organoids'
        api_class = api.Organoid

        @classmethod
        def to_dict(cls, organoid: api_class) -> MutableJSON:
            return {
                **super().to_dict(organoid),
                'organ': None,
                'organ_part': [],
                'model_organ': organoid.model_organ,
                'model_organ_part': organoid.model_organ_part,
                'effective_organ': organoid.model_organ,
            }

    class SampleSpecimen(Sample):
        entity_type = 'specimens'
        api_class = api.SpecimenFromOrganism

        @classmethod
        def to_dict(cls, specimen: api_class) -> MutableJSON:
            return {
                **super().to_dict(specimen),
                'organ': specimen.organ,
                'organ_part': sorted(specimen.organ_parts),
                'model_organ': None,
                'model_organ_part': None,
                'effective_organ': specimen.organ,
            }

    sample_types: Mapping[Callable, Type[Sample]] = {
        _cell_line: SampleCellLine,
        _organoid: SampleOrganoid,
        _specimen: SampleSpecimen
    }

    def _samples(self, samples: Iterable[api.Biomaterial]) -> MutableJSON:
        """
        Returns inner entities representing the given samples as both, generic
        'samples' inner entities and specific 'sample_{entity_type}' entities.
        A 'samples' inner entity is a polymorphic structure containing
        the properties common to all samples. This allows filtering on these
        common properties regardless of the sample entity type.
        """
        result = defaultdict(list)
        for sample in samples:
            for to_dict, sample_type in self.sample_types.items():
                if isinstance(sample, sample_type.api_class):
                    entity_type = f'sample_{sample_type.entity_type}'
                    result[entity_type].append(to_dict(self, sample))
                    result['samples'].append(sample_type.to_dict(sample))
                    break
            else:
                assert False, sample
        return result

    @classmethod
    def _matrix_types(cls) -> FieldTypes:
        return {
            'document_id': null_str,
            'file': {
                **cls._file_types(),
                'strata': null_str
            }
        }

    def _matrix(self, file: api.File) -> MutableJSON:
        if isinstance(file, api.SupplementaryFile):
            # Stratification values for supplementary files are
            # provided in the 'file_description' field of the file JSON.
            strata_string = file.json['file_description']
        elif isinstance(file, api.File):
            # Stratification values for other file types are gathered by
            # visiting the file and using values from the graph.
            strata_string = self._build_strata_string(file)
        else:
            assert False, type(file)
        return {
            'document_id': str(file.document_id),
            # These values are grouped together in a dict so when the dicts are
            # aggregated together we will have preserved the grouping of values.
            'file': {
                **self._file(file),
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
                point_strings.append(dimension + '=' + ','.join(sorted(values)))
        return ';'.join(point_strings)

    @classmethod
    def field_types(cls) -> FieldTypes:
        """
        Field types outline the general shape of our documents.
        """
        # FIXME: Not all information is captured. Lists of primitive types are
        #        represented, but lists of container types are not. Eventually,
        #        we want field_types to more accurately describe the shape of
        #        the documents, in particular the contributions.
        #        https://github.com/DataBiosphere/azul/issues/2689
        return {
            'samples': cls._sample_types(),
            'sample_cell_lines': cls._cell_line_types(),
            'sample_organoids': cls._organoid_types(),
            'sample_specimens': cls._specimen_types(),
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
            'matrices': cls._matrix_types(),
            'contributed_analyses': cls._matrix_types(),
            'projects': cls._project_types(),
            'dates': cls._date_types(),
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

    @classmethod
    def validate_class(cls):
        # Manifest generation depends on this:
        assert cls._related_file_types().keys() <= cls._file_types().keys()

    @cached_property
    def _api_project(self) -> api.Project:
        return one(self.api_bundle.projects.values())

    @classmethod
    def inner_entity_id(cls, entity_type: EntityType, entity: JSON) -> EntityID:
        return entity['document_id']

    @classmethod
    def reconcile_inner_entities(cls,
                                 entity_type: EntityType,
                                 *,
                                 this: tuple[JSON, BundleFQID],
                                 that: tuple[JSON, BundleFQID]
                                 ) -> tuple[JSON, BundleFQID]:
        this_entity, this_bundle = this
        that_entity, that_bundle = that
        if that_entity.keys() != this_entity.keys():
            mismatch = set(that_entity.keys()).symmetric_difference(this_entity)
            log.warning('Document shape of `%s` this_entity `%s` '
                        'does not match between bundles %r and %r, '
                        'the mismatched properties being: %s',
                        entity_type, cls.inner_entity_id(entity_type, this_entity),
                        this_bundle, that_bundle,
                        mismatch)
        return that if that_bundle.version > this_bundle.version else this


BaseTransformer.validate_class()


def _parse_zarr_file_name(file_name: str
                          ) -> tuple[bool, Optional[str], Optional[str]]:
    file_name = file_name.split('.zarr/')
    if len(file_name) == 1:
        return False, None, None
    elif len(file_name) == 2:
        zarr_name, sub_name = file_name
        return True, zarr_name, sub_name
    else:
        assert False


class TransformerVisitor(api.EntityVisitor):
    # Entities are tracked by ID to ensure uniqueness if an entity is visited
    # twice while descending the entity DAG
    specimens: dict[api.UUID4, api.SpecimenFromOrganism]
    cell_suspensions: dict[api.UUID4, api.CellSuspension]
    cell_lines: dict[api.UUID4, api.CellLine]
    donors: dict[api.UUID4, api.DonorOrganism]
    organoids: dict[api.UUID4, api.Organoid]
    analysis_protocols: dict[api.UUID4, api.AnalysisProtocol]
    imaging_protocols: dict[api.UUID4, api.ImagingProtocol]
    library_preparation_protocols: dict[api.UUID4, api.LibraryPreparationProtocol]
    sequencing_inputs: dict[api.UUID4, api.Biomaterial]
    sequencing_protocols: dict[api.UUID4, api.SequencingProtocol]
    sequencing_processes: dict[api.UUID4, api.Process]
    files: dict[api.UUID4, api.File]

    def __init__(self) -> None:
        self.specimens = {}
        self.cell_suspensions = {}
        self.cell_lines = {}
        self.donors = {}
        self.organoids = {}
        self.analysis_protocols = {}
        self.imaging_protocols = {}
        self.library_preparation_protocols = {}
        self.sequencing_inputs = {}
        self.sequencing_protocols = {}
        self.sequencing_processes = {}
        self.files = {}

    def visit(self, entity: api.Entity) -> None:
        if (
            isinstance(entity, api.Biomaterial)
            and any(isinstance(protocol, api.SequencingProtocol)
                    for process in entity.to_processes.values()
                    for protocol in process.protocols.values())
        ):
            self.sequencing_inputs[entity.document_id] = entity
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


ENTITY = TypeVar('ENTITY', bound=api.Entity)


class PartitionedTransformer(BaseTransformer, Generic[ENTITY]):

    @abstractmethod
    def _transform(self, entities: Iterable[ENTITY]) -> Iterable[Transform]:
        """
        Transform the given outer entities into contributions.
        """
        raise NotImplementedError

    @abstractmethod
    def _entities(self) -> Iterable[ENTITY]:
        """
        Return all outer entities of interest in the bundle.
        """
        raise NotImplementedError

    def _entities_in(self, partition: BundlePartition) -> Iterator[ENTITY]:
        return (e for e in self._entities() if partition.contains(e.document_id))

    def estimate(self, partition: BundlePartition) -> int:
        return ilen(self._entities_in(partition))

    def transform(self, partition: BundlePartition) -> Iterable[Transform]:
        return self._transform(generable(self._entities_in, partition))


class FileTransformer(PartitionedTransformer[api.File]):

    @classmethod
    def entity_type(cls) -> str:
        return 'files'

    def _entities(self) -> Iterable[api.File]:
        return api.not_stitched(self.api_bundle.files.values())

    def _transform(self, files: Iterable[api.File]) -> Iterable[Contribution]:
        zarr_stores: Mapping[str, list[api.File]] = self.group_zarrs(files)
        for file in files:
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
                contents = dict(self._samples(samples.values()),
                                sequencing_inputs=list(
                                    map(self._sequencing_input, visitor.sequencing_inputs.values())
                                ),
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
                                dates=[self._date(file)],
                                projects=[self._project(self._api_project)])
                # Supplementary file matrices provide stratification values that
                # need to be reflected by inner entities in the contribution.
                if isinstance(file, api.SupplementaryFile) and file.is_matrix:
                    if Submitter.category_for_file(file) in (
                        SubmitterCategory.internal,
                        SubmitterCategory.external
                    ):
                        additional_contents = self.matrix_stratification_values(file)
                        for entity_type, values in additional_contents.items():
                            contents[entity_type].extend(values)
                yield self._add_replica(contents, file)

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
                donor = {}
                genus_species = stratum.get('genusSpecies')
                if genus_species is not None:
                    donor['genus_species'] = sorted(genus_species)
                development_stage = stratum.get('developmentStage')
                if development_stage is not None:
                    donor['development_stage'] = sorted(development_stage)
                if donor:
                    donor.update(
                        {
                            'biomaterial_id': f'donor_organism_{file_name}',
                        }
                    )
                    contents['donors'].append(donor)
                organ = stratum.get('organ')
                if organ is not None:
                    for i, one_organ in enumerate(sorted(organ)):
                        contents['specimens'].append(
                            {
                                'biomaterial_id': f'specimen_from_organism_{i}_{file_name}',
                                'organ': one_organ,
                            },
                        )
                library = stratum.get('libraryConstructionApproach')
                if library is not None:
                    contents['library_preparation_protocols'].append(
                        {
                            'library_construction_approach': sorted(library),
                        }
                    )
        return contents

    def group_zarrs(self,
                    files: Iterable[api.File]
                    ) -> Mapping[str, list[api.File]]:
        zarr_stores = defaultdict(list)
        for file in files:
            file_name = file.manifest_entry.name
            is_zarr, zarr_name, sub_name = _parse_zarr_file_name(file_name)
            if is_zarr:
                # Leave the representative file out of the list since it's already in the manifest
                if not sub_name.startswith('.zattrs'):
                    zarr_stores[zarr_name].append(file)
        return zarr_stores


class CellSuspensionTransformer(PartitionedTransformer):

    @classmethod
    def entity_type(cls) -> str:
        return 'cell_suspensions'

    def _entities(self) -> Iterable[api.CellSuspension]:
        for biomaterial in self.api_bundle.biomaterials.values():
            if isinstance(biomaterial, api.CellSuspension):
                yield biomaterial

    def _transform(self,
                   cell_suspensions: Iterable[api.CellSuspension]
                   ) -> Iterable[Contribution]:
        for cell_suspension in cell_suspensions:
            samples: dict[str, Sample] = dict()
            self._find_ancestor_samples(cell_suspension, samples)
            visitor = TransformerVisitor()
            cell_suspension.accept(visitor)
            cell_suspension.ancestors(visitor)
            contents = dict(self._samples(samples.values()),
                            sequencing_inputs=list(
                                map(self._sequencing_input, visitor.sequencing_inputs.values())
                            ),
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
                            dates=[self._date(cell_suspension)],
                            projects=[self._project(self._api_project)])
            yield self._add_replica(contents, cell_suspension)


class SampleTransformer(PartitionedTransformer):

    @classmethod
    def entity_type(cls) -> str:
        return 'samples'

    @classmethod
    def inner_entity_types(cls) -> frozenset[str]:
        return frozenset([
            cls.entity_type(),
            'sample_cell_lines',
            'sample_organoids',
            'sample_specimens'
        ])

    def _entities(self) -> Iterable[Sample]:
        samples: dict[str, Sample] = dict()
        for file in api.not_stitched(self.api_bundle.files.values()):
            self._find_ancestor_samples(file, samples)
        return samples.values()

    def _transform(self, samples: Iterable[Sample]) -> Iterable[Contribution]:
        for sample in samples:
            visitor = TransformerVisitor()
            sample.accept(visitor)
            sample.ancestors(visitor)
            contents = dict(self._samples([sample]),
                            sequencing_inputs=list(
                                map(self._sequencing_input, visitor.sequencing_inputs.values())
                            ),
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
                            dates=[self._date(sample)],
                            projects=[self._project(self._api_project)])
            yield self._add_replica(contents, sample)


class BundleAsEntity(DatedEntity):

    def __init__(self, bundle: api.Bundle) -> None:
        super().__init__()
        self.document_id = bundle.uuid
        # A bundle's version should be a sortable string, however we happen to
        # know that all bundles in current deployments use a DCP/2 version
        # string, so we use this to set the entity's date fields.
        date = parse_dcp2_version(bundle.version)
        self.update_date = date
        self.submission_date = date


class SingletonTransformer(BaseTransformer, metaclass=ABCMeta):
    """
    A transformer for entity types of which there is exactly one instance in
    every bundle.
    """

    @property
    def _singleton_id(self) -> api.UUID4:
        return self._singleton_entity().document_id

    @abstractmethod
    def _singleton_entity(self) -> DatedEntity:
        raise NotImplementedError

    def _dated_entities(self) -> Iterable[DatedEntity]:
        return api.not_stitched(self.api_bundle.entities.values())

    def estimate(self, partition: BundlePartition) -> int:
        return int(partition.contains(self._singleton_id))

    def transform(self, partition: BundlePartition) -> Iterable[Transform]:
        if partition.contains(self._singleton_id):
            yield self._transform()
        else:
            return ()

    def _transform(self) -> Transform:
        # Project entities are not explicitly linked in the graph. The mere
        # presence of project metadata in a bundle indicates that all other
        # entities in that bundle belong to that project. Because of that we
        # can't rely on a visitor to collect the related entities but have to
        # enumerate them explicitly.
        # FIXME: https://github.com/DataBiosphere/azul/issues/3270
        #        Comment doesn't match code behavior
        # The enumeration should not include any
        # stitched entities because those will be discovered when the stitched
        # bundle is transformed.
        #
        visitor = TransformerVisitor()
        for specimen in self.api_bundle.specimens:
            specimen.accept(visitor)
            specimen.ancestors(visitor)
        samples: dict[str, Sample] = dict()
        for file in self.api_bundle.files.values():
            file.accept(visitor)
            file.ancestors(visitor)
            self._find_ancestor_samples(file, samples)
        matrices = [
            self._matrix(file)
            for file in visitor.files.values()
            if (
                file.is_matrix
                and not self._is_intermediate_matrix(file)
                and Submitter.category_for_file(file) == SubmitterCategory.internal
            )
        ]
        contributed_analyses = [
            self._matrix(file)
            for file in visitor.files.values()
            if (
                (file.is_matrix or isinstance(file, api.AnalysisFile))
                and not self._is_intermediate_matrix(file)
                and Submitter.category_for_file(file) == SubmitterCategory.external
            )
        ]

        contents = dict(self._samples(samples.values()),
                        sequencing_inputs=list(
                            map(self._sequencing_input, visitor.sequencing_inputs.values())
                        ),
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
                        matrices=matrices,
                        contributed_analyses=contributed_analyses,
                        dates=[self._date(self._singleton_entity())],
                        projects=[self._project(self._api_project)])
        return self._add_replica(contents, self._singleton_entity())


class ProjectTransformer(SingletonTransformer):

    def _singleton_entity(self) -> DatedEntity:
        return self._api_project

    @classmethod
    def entity_type(cls) -> str:
        return 'projects'


class BundleTransformer(SingletonTransformer):

    def _singleton_entity(self) -> DatedEntity:
        return BundleAsEntity(self.api_bundle)

    @classmethod
    def aggregator(cls, entity_type: EntityType) -> Optional[EntityAggregator]:
        if entity_type == 'files':
            return None
        else:
            return super().aggregator(entity_type)

    @classmethod
    def entity_type(cls) -> str:
        return 'bundles'
