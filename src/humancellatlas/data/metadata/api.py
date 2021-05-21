from abc import (
    ABC,
    abstractmethod,
)
from collections import defaultdict
from dataclasses import (
    dataclass,
    field,
    fields,
)
from itertools import chain
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)
from uuid import UUID
import warnings

from humancellatlas.data.metadata.age_range import AgeRange
from humancellatlas.data.metadata.lookup import (
    LookupDefault,
    lookup,
)

# A few helpful type aliases
#
UUID4 = UUID
AnyJSON2 = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]
AnyJSON1 = Union[str, int, float, bool, None, Dict[str, AnyJSON2], List[AnyJSON2]]
AnyJSON = Union[str, int, float, bool, None, Dict[str, AnyJSON1], List[AnyJSON1]]
JSON = Dict[str, AnyJSON]


@dataclass(init=False)
class ManifestEntry:
    json: JSON = field(init=False, repr=False)
    content_type: str = field(init=False)
    crc32c: str
    indexed: bool
    name: str
    s3_etag: Optional[str]
    sha1: Optional[str]
    sha256: str
    size: int
    # only populated if bundle was requested with `directurls` or `directurls` set
    url: Optional[str]
    uuid: UUID4 = field(init=False)
    version: str
    is_stitched: bool = field(init=False)

    def __init__(self, json: JSON):
        # '/' was once forbidden in file paths and was encoded with '!'. Now
        # '/' is allowed and we force it in the metadata so that backwards
        # compatibility is simplified downstream.
        json['name'] = json['name'].replace('!', '/')
        self.json = json
        self.content_type = json['content-type']
        self.uuid = UUID4(json['uuid'])
        self.is_stitched = json.get('is_stitched', False)
        for f in fields(self):
            if f.init:
                value = json.get(f.name)
                if value is None and not is_optional(f.type):
                    raise TypeError('Property cannot be absent or None', f.name)
                else:
                    setattr(self, f.name, value)


@dataclass(init=False)
class Entity:
    json: JSON = field(repr=False)
    document_id: UUID4
    submitter_id: Optional[str]
    metadata_manifest_entry: Optional[ManifestEntry]
    submission_date: str
    update_date: Optional[str]

    @property
    def is_stitched(self):
        if self.metadata_manifest_entry is None:
            return False
        else:
            return self.metadata_manifest_entry.is_stitched

    @classmethod
    def from_json(cls,
                  json: JSON,
                  metadata_manifest_entry: Optional[ManifestEntry],
                  **kwargs):
        content = json.get('content', json)
        described_by = content['describedBy']
        schema_name = described_by.rpartition('/')[2]
        try:
            sub_cls = entity_types[schema_name]
        except KeyError:
            raise TypeLookupError(described_by)
        return sub_cls(json, metadata_manifest_entry, **kwargs)

    def __init__(self,
                 json: JSON,
                 metadata_manifest_entry: Optional[ManifestEntry]
                 ) -> None:
        super().__init__()
        self.json = json
        self.metadata_manifest_entry = metadata_manifest_entry
        provenance = json.get('hca_ingest') or json['provenance']
        self.document_id = UUID4(provenance['document_id'])
        # Some older DCP/1 bundles use different UUIDs in the manifest and
        # metadata.
        # noinspection PyUnreachableCode
        if False and self.metadata_manifest_entry is not None:
            assert self.document_id == self.metadata_manifest_entry.uuid
        self.submitter_id = provenance.get('submitter_id')
        self.submission_date = lookup(provenance, 'submission_date', 'submissionDate')
        self.update_date = lookup(provenance, 'update_date', 'updateDate', default=None)

    @property
    def address(self):
        return self.schema_name + '@' + str(self.document_id)

    @property
    def schema_name(self):
        return schema_names[type(self)]

    def accept(self, visitor: 'EntityVisitor') -> None:
        visitor.visit(self)


# A type variable for subtypes of Entity
#
E = TypeVar('E', bound=Entity)


# noinspection PyPep8Naming
@dataclass(frozen=True)
class not_stitched(Iterable[E]):
    """
    An iterable of the entities in the argument iterable that are not stitched.
    This is an iterable, so it can be consumed repeatedly.
    """

    entities: Iterable[E]

    def __iter__(self) -> Iterator[E]:
        return (e for e in self.entities if not e.is_stitched)


class TypeLookupError(Exception):

    def __init__(self, described_by: str) -> None:
        super().__init__(f"No entity type for schema URL '{described_by}'")


class EntityVisitor(ABC):

    @abstractmethod
    def visit(self, entity: 'Entity') -> None:
        raise NotImplementedError()


@dataclass(init=False)
class LinkedEntity(Entity, ABC):
    children: MutableMapping[UUID4, Entity] = field(repr=False)
    parents: MutableMapping[UUID4, 'LinkedEntity'] = field(repr=False)

    @abstractmethod
    def _connect_to(self, other: Entity, forward: bool) -> None:
        raise NotImplementedError()

    def __init__(self,
                 json: JSON,
                 metadata_manifest_entry: Optional[ManifestEntry]
                 ) -> None:
        super().__init__(json, metadata_manifest_entry)
        self.children = {}
        self.parents = {}

    def connect_to(self, other: Entity, forward: bool) -> None:
        mapping = self.children if forward else self.parents
        mapping[other.document_id] = other
        self._connect_to(other, forward)

    def ancestors(self, visitor: EntityVisitor):
        for parent in self.parents.values():
            parent.ancestors(visitor)
            visitor.visit(parent)

    def accept(self, visitor: EntityVisitor):
        super().accept(visitor)
        for child in self.children.values():
            child.accept(visitor)


class LinkError(RuntimeError):

    def __init__(self, entity: LinkedEntity, other_entity: Entity, forward: bool) -> None:
        super().__init__(entity.address +
                         ' cannot ' + ('reference ' if forward else 'be referenced by ') +
                         other_entity.address)


@dataclass(frozen=True)
class ProjectPublication:
    title: str
    url: Optional[str]

    @classmethod
    def from_json(cls, json: JSON) -> 'ProjectPublication':
        title = lookup(json, 'title', 'publication_title')
        url = lookup(json, 'url', 'publication_url', default=None)
        return cls(title=title, url=url)

    @property
    def publication_title(self):
        warnings.warn(f"ProjectPublication.publication_title is deprecated. "
                      f"Use ProjectPublication.title instead.", DeprecationWarning)
        return self.title

    @property
    def publication_url(self):
        warnings.warn(f"ProjectPublication.publication_url is deprecated. "
                      f"Use ProjectPublication.url instead.", DeprecationWarning)
        return self.url


@dataclass(frozen=True)
class ProjectContact:
    name: str
    email: Optional[str]
    institution: Optional[str]  # optional up to project/5.3.0/contact
    laboratory: Optional[str]
    corresponding_contributor: Optional[bool]
    project_role: Optional[str]

    @classmethod
    def from_json(cls, json: JSON) -> 'ProjectContact':
        project_role = json.get('project_role')
        project_role = ontology_label(project_role) if isinstance(project_role, dict) else project_role
        return cls(name=lookup(json, 'name', 'contact_name'),
                   email=json.get('email'),
                   institution=json.get('institution'),
                   laboratory=json.get('laboratory'),
                   corresponding_contributor=json.get('corresponding_contributor'),
                   project_role=project_role)

    @property
    def contact_name(self) -> str:
        warnings.warn(f"ProjectContact.contact_name is deprecated. "
                      f"Use ProjectContact.name instead.", DeprecationWarning)
        return self.name


@dataclass(init=False)
class Project(Entity):
    project_short_name: str
    project_title: str
    project_description: Optional[str]  # optional up to core/project/5.2.2/project_core
    publications: Set[ProjectPublication]
    contributors: Set[ProjectContact]
    insdc_project_accessions: Set[str]
    geo_series_accessions: Set[str]
    array_express_accessions: Set[str]
    insdc_study_accessions: Set[str]
    supplementary_links: Set[str]

    def __init__(self,
                 json: JSON,
                 metadata_manifest_entry: Optional[ManifestEntry]
                 ) -> None:
        super().__init__(json, metadata_manifest_entry)
        content = json.get('content', json)
        core = content['project_core']
        self.project_short_name = lookup(core, 'project_short_name', 'project_shortname')
        self.project_title = core['project_title']
        self.project_description = core.get('project_description')
        self.publications = set(ProjectPublication.from_json(publication)
                                for publication in content.get('publications', []))
        self.contributors = {ProjectContact.from_json(contributor) for contributor in content.get('contributors', [])}
        self.insdc_project_accessions = set(content.get('insdc_project_accessions', []))
        self.geo_series_accessions = set(content.get('geo_series_accessions', []))
        self.array_express_accessions = set(content.get('array_express_accessions', []))
        self.insdc_study_accessions = set(content.get('insdc_study_accessions', []))
        self.supplementary_links = set(content.get('supplementary_links', []))

    @property
    def laboratory_names(self) -> set:
        warnings.warn("Project.laboratory_names is deprecated. "
                      "Use contributors.laboratory instead.", DeprecationWarning)
        return {contributor.laboratory for contributor in self.contributors if contributor.laboratory}

    @property
    def project_shortname(self) -> str:
        warnings.warn("Project.project_shortname is deprecated. "
                      "Use project_short_name instead.", DeprecationWarning)
        return self.project_short_name


@dataclass(init=False)
class Biomaterial(LinkedEntity):
    biomaterial_id: str
    ncbi_taxon_id: List[int]
    has_input_biomaterial: Optional[str]
    from_processes: MutableMapping[UUID4, 'Process'] = field(repr=False)
    to_processes: MutableMapping[UUID4, 'Process']

    def __init__(self,
                 json: JSON,
                 metadata_manifest_entry: Optional[ManifestEntry]
                 ) -> None:
        super().__init__(json, metadata_manifest_entry)
        content = json.get('content', json)
        self.biomaterial_id = content['biomaterial_core']['biomaterial_id']
        self.ncbi_taxon_id = content['biomaterial_core']['ncbi_taxon_id']
        self.has_input_biomaterial = content['biomaterial_core'].get('has_input_biomaterial')
        self.from_processes = {}
        self.to_processes = {}

    def _connect_to(self, other: Entity, forward: bool) -> None:
        if isinstance(other, Process):
            if forward:
                self.to_processes[other.document_id] = other
            else:
                self.from_processes[other.document_id] = other
        else:
            raise LinkError(self, other, forward)


@dataclass(init=False)
class DonorOrganism(Biomaterial):
    genus_species: Set[str]
    diseases: Set[str]
    organism_age: str
    organism_age_unit: str
    sex: str
    development_stage: Optional[str]

    def __init__(self,
                 json: JSON,
                 metadata_manifest_entry: Optional[ManifestEntry]
                 ) -> None:
        super().__init__(json, metadata_manifest_entry)
        content = json.get('content', json)
        self.genus_species = {ontology_label(gs) for gs in content['genus_species']}
        self.diseases = {ontology_label(d) for d in lookup(content, 'diseases', 'disease', default=[]) if d}
        self.organism_age = content.get('organism_age')
        self.organism_age_unit = ontology_label(content.get('organism_age_unit'), default=None)
        self.sex = lookup(content, 'sex', 'biological_sex')
        self.development_stage = ontology_label(content.get('development_stage'), default=None)

    @property
    def organism_age_in_seconds(self) -> Optional[AgeRange]:
        if self.organism_age and self.organism_age_unit:
            return AgeRange.parse(self.organism_age, self.organism_age_unit)
        else:
            return None

    @property
    def biological_sex(self):
        warnings.warn(f"DonorOrganism.biological_sex is deprecated. "
                      f"Use DonorOrganism.sex instead.", DeprecationWarning)
        return self.sex

    @property
    def disease(self):
        warnings.warn(f"DonorOrganism.disease is deprecated. "
                      f"Use DonorOrganism.diseases instead.", DeprecationWarning)
        return self.diseases


@dataclass(init=False)
class SpecimenFromOrganism(Biomaterial):
    storage_method: Optional[str]
    preservation_method: Optional[str]
    diseases: Set[str]
    organ: Optional[str]
    organ_parts: Set[str]

    def __init__(self,
                 json: JSON,
                 metadata_manifest_entry: Optional[ManifestEntry]
                 ) -> None:
        super().__init__(json, metadata_manifest_entry)
        content = json.get('content', json)
        preservation_storage = content.get('preservation_storage')
        self.storage_method = preservation_storage.get('storage_method') if preservation_storage else None
        self.preservation_method = preservation_storage.get('preservation_method') if preservation_storage else None
        self.diseases = {ontology_label(d) for d in lookup(content, 'diseases', 'disease', default=[]) if d}
        self.organ = ontology_label(content.get('organ'), default=None)

        organ_parts = lookup(content, 'organ_parts', 'organ_part', default=[])
        if not isinstance(organ_parts, list):
            organ_parts = [organ_parts]
        self.organ_parts = {ontology_label(d) for d in organ_parts if d}

    @property
    def disease(self):
        warnings.warn(f"SpecimenFromOrganism.disease is deprecated. "
                      f"Use SpecimenFromOrganism.diseases instead.", DeprecationWarning)
        return self.diseases

    @property
    def organ_part(self):
        msg = ("SpecimenFromOrganism.organ_part has been removed. "
               "Use SpecimenFromOrganism.organ_parts instead.")
        warnings.warn(msg, DeprecationWarning)
        raise AttributeError(msg)


@dataclass(init=False)
class ImagedSpecimen(Biomaterial):
    slice_thickness: Union[float, int]

    def __init__(self,
                 json: JSON,
                 metadata_manifest_entry: Optional[ManifestEntry]
                 ) -> None:
        super().__init__(json, metadata_manifest_entry)
        self.slice_thickness = json['slice_thickness']


@dataclass(init=False)
class CellSuspension(Biomaterial):
    estimated_cell_count: Optional[int]
    selected_cell_types: Set[str]

    def __init__(self,
                 json: JSON,
                 metadata_manifest_entry: Optional[ManifestEntry]
                 ) -> None:
        super().__init__(json, metadata_manifest_entry)
        content = json.get('content', json)
        self.estimated_cell_count = lookup(content, 'estimated_cell_count', 'total_estimated_cells', default=None)
        self.selected_cell_types = {ontology_label(sct) for sct in
                                    lookup(content, 'selected_cell_types', 'selected_cell_type', default=[])}

    @property
    def total_estimated_cells(self) -> int:
        warnings.warn(f"CellSuspension.total_estimated_cells is deprecated. "
                      f"Use CellSuspension.estimated_cell_count instead.", DeprecationWarning)
        return self.estimated_cell_count

    @property
    def selected_cell_type(self) -> Set[str]:
        warnings.warn(f"CellSuspension.selected_cell_type is deprecated. "
                      f"Use CellSuspension.selected_cell_types instead.", DeprecationWarning)
        return self.selected_cell_types


@dataclass(init=False)
class CellLine(Biomaterial):
    type: str
    model_organ: Optional[str]

    def __init__(self,
                 json: JSON,
                 metadata_manifest_entry: Optional[ManifestEntry]
                 ) -> None:
        super().__init__(json, metadata_manifest_entry)
        content = json.get('content', json)
        self.type = lookup(content, 'type', 'cell_line_type')
        self.model_organ = ontology_label(content.get('model_organ'), default=None)

    @property
    def cell_line_type(self) -> str:
        warnings.warn(f"CellLine.cell_line_type is deprecated. "
                      f"Use CellLine.type instead.", DeprecationWarning)
        return self.type


@dataclass(init=False)
class Organoid(Biomaterial):
    model_organ: str
    model_organ_part: Optional[str]

    def __init__(self,
                 json: JSON,
                 metadata_manifest_entry: Optional[ManifestEntry]
                 ) -> None:
        super().__init__(json, metadata_manifest_entry)
        content = json.get('content', json)
        self.model_organ = ontology_label(lookup(content, 'model_organ', 'model_for_organ'), default=None)
        self.model_organ_part = ontology_label(content.get('model_organ_part'), default=None)


@dataclass(init=False)
class Process(LinkedEntity):
    process_id: str
    process_name: Optional[str]
    input_biomaterials: MutableMapping[UUID4, Biomaterial] = field(repr=False)
    input_files: MutableMapping[UUID4, 'File'] = field(repr=False)
    output_biomaterials: MutableMapping[UUID4, Biomaterial]
    output_files: MutableMapping[UUID4, 'File']
    protocols: MutableMapping[UUID4, 'Protocol']

    def __init__(self,
                 json: JSON,
                 metadata_manifest_entry: Optional[ManifestEntry]
                 ) -> None:
        super().__init__(json, metadata_manifest_entry)
        content = json.get('content', json)
        process_core = content['process_core']
        self.process_id = process_core['process_id']
        self.process_name = process_core.get('process_name')
        self.input_biomaterials = {}
        self.input_files = {}
        self.output_biomaterials = {}
        self.output_files = {}
        self.protocols = {}

    def _connect_to(self, other: Entity, forward: bool) -> None:
        if isinstance(other, Biomaterial):
            biomaterials = self.output_biomaterials if forward else self.input_biomaterials
            biomaterials[other.document_id] = other
        elif isinstance(other, File):
            files = self.output_files if forward else self.input_files
            files[other.document_id] = other
        elif isinstance(other, Protocol):
            if forward:
                self.protocols[other.document_id] = other
            else:
                raise LinkError(self, other, forward)
        else:
            raise LinkError(self, other, forward)

    def is_sequencing_process(self):
        return any(isinstance(pl, SequencingProtocol) for pl in self.protocols.values())


@dataclass(init=False)
class AnalysisProcess(Process):
    pass


@dataclass(init=False)
class DissociationProcess(Process):

    def __init__(self,
                 json: JSON,
                 metadata_manifest_entry: Optional[ManifestEntry]
                 ) -> None:
        warnings.warn(f"{type(self)} is deprecated", DeprecationWarning)
        super().__init__(json, metadata_manifest_entry)


@dataclass(init=False)
class EnrichmentProcess(Process):

    def __init__(self,
                 json: JSON,
                 metadata_manifest_entry: Optional[ManifestEntry]
                 ) -> None:
        warnings.warn(f"{type(self)} is deprecated", DeprecationWarning)
        super().__init__(json, metadata_manifest_entry)


@dataclass(init=False)
class LibraryPreparationProcess(Process):
    library_construction_approach: str

    def __init__(self,
                 json: JSON,
                 metadata_manifest_entry: Optional[ManifestEntry]
                 ) -> None:
        warnings.warn(f"{type(self)} is deprecated", DeprecationWarning)
        super().__init__(json, metadata_manifest_entry)
        content = json.get('content', json)
        self.library_construction_approach = content['library_construction_approach']


@dataclass(init=False)
class SequencingProcess(Process):
    instrument_manufacturer_model: str

    def __init__(self,
                 json: JSON,
                 metadata_manifest_entry: Optional[ManifestEntry]
                 ) -> None:
        warnings.warn(f"{type(self)} is deprecated", DeprecationWarning)
        super().__init__(json, metadata_manifest_entry)
        content = json.get('content', json)
        self.instrument_manufacturer_model = ontology_label(content['instrument_manufacturer_model'])

    def is_sequencing_process(self):
        return True


@dataclass(frozen=True)
class ImagingTarget:
    assay_type: str

    @classmethod
    def from_json(cls, json: JSON) -> 'ImagingTarget':
        assay_type = ontology_label(json['assay_type'])
        return cls(assay_type=assay_type)


@dataclass(init=False)
class Protocol(LinkedEntity):
    protocol_id: str
    protocol_name: Optional[str]

    def __init__(self,
                 json: JSON,
                 metadata_manifest_entry: Optional[ManifestEntry]
                 ) -> None:
        super().__init__(json, metadata_manifest_entry)
        content = json.get('content', json)
        protocol_core = content['protocol_core']
        self.protocol_id = protocol_core['protocol_id']
        self.protocol_name = protocol_core.get('protocol_name')

    def _connect_to(self, other: Entity, forward: bool) -> None:
        if isinstance(other, Process) and not forward:
            pass  # no explicit, typed back reference
        else:
            raise LinkError(self, other, forward)


@dataclass(init=False)
class LibraryPreparationProtocol(Protocol):
    library_construction_method: str
    nucleic_acid_source: Optional[str]

    def __init__(self,
                 json: JSON,
                 metadata_manifest_entry: Optional[ManifestEntry]
                 ) -> None:
        super().__init__(json, metadata_manifest_entry)
        content = json.get('content', json)
        temp = lookup(content, 'library_construction_method', 'library_construction_approach')
        self.library_construction_method = ontology_label(temp) if isinstance(temp, dict) else temp
        self.nucleic_acid_source = content.get('nucleic_acid_source')

    @property
    def library_construction_approach(self) -> str:
        warnings.warn(f"LibraryPreparationProtocol.library_construction_approach is deprecated. "
                      f"Use LibraryPreparationProtocol.library_construction_method instead.", DeprecationWarning)
        return self.library_construction_method


@dataclass(init=False)
class SequencingProtocol(Protocol):
    instrument_manufacturer_model: str
    paired_end: Optional[bool]

    def __init__(self,
                 json: JSON,
                 metadata_manifest_entry: Optional[ManifestEntry]
                 ) -> None:
        super().__init__(json, metadata_manifest_entry)
        content = json.get('content', json)
        self.instrument_manufacturer_model = ontology_label(content.get('instrument_manufacturer_model'), default=None)
        self.paired_end = lookup(content, 'paired_end', 'paired_ends', default=None)


@dataclass(init=False)
class AnalysisProtocol(Protocol):
    pass


@dataclass(init=False)
class AggregateGenerationProtocol(Protocol):
    pass


@dataclass(init=False)
class CollectionProtocol(Protocol):
    pass


@dataclass(init=False)
class DifferentiationProtocol(Protocol):
    pass


@dataclass(init=False)
class DissociationProtocol(Protocol):
    pass


@dataclass(init=False)
class EnrichmentProtocol(Protocol):
    pass


@dataclass(init=False)
class IpscInductionProtocol(Protocol):
    pass


@dataclass(init=False)
class ImagingProtocol(Protocol):
    target: List[ImagingTarget]  # A list so all the ImagingTarget objects can be tallied when indexed

    def __init__(self,
                 json: JSON,
                 metadata_manifest_entry: Optional[ManifestEntry]
                 ) -> None:
        super().__init__(json, metadata_manifest_entry)
        content = json.get('content', json)
        self.target = [ImagingTarget.from_json(target) for target in content['target']]


@dataclass(init=False)
class ImagingPreparationProtocol(Protocol):
    pass


def is_optional(t):
    """
    https://stackoverflow.com/a/62641842/4171119

    >>> is_optional(str)
    False
    >>> is_optional(Optional[str])
    True
    >>> is_optional(Union[str, None])
    True
    >>> is_optional(Union[None, str])
    True
    >>> is_optional(Union[str, None, int])
    True
    >>> is_optional(Union[str, int])
    False
    """
    return t == Optional[t]


@dataclass(init=False)
class File(LinkedEntity):
    format: str
    from_processes: MutableMapping[UUID4, Process] = field(repr=False)
    to_processes: MutableMapping[UUID4, Process]
    manifest_entry: ManifestEntry
    content_description: Set[str]
    file_source: str

    def __init__(self,
                 json: JSON,
                 metadata_manifest_entry: Optional[ManifestEntry],
                 manifest: Mapping[str, ManifestEntry]):
        super().__init__(json, metadata_manifest_entry)
        content = json.get('content', json)
        # '/' was once forbidden in file paths and was encoded with '!'. Now
        # '/' is allowed and we force it in the metadata so that backwards
        # compatibility is simplified downstream.
        core = content['file_core']
        core['file_name'] = core['file_name'].replace('!', '/')
        self.format = lookup(core, 'format', 'file_format')
        self.manifest_entry = manifest[core['file_name']]
        self.content_description = {ontology_label(cd) for cd in core.get('content_description', [])}
        self.file_source = core.get('file_source')
        self.from_processes = {}
        self.to_processes = {}

    def _connect_to(self, other: Entity, forward: bool) -> None:
        if isinstance(other, Process):
            if forward:
                self.to_processes[other.document_id] = other
            else:
                self.from_processes[other.document_id] = other
        else:
            raise LinkError(self, other, forward)

    @property
    def file_format(self) -> str:
        warnings.warn(f"File.file_format is deprecated. "
                      f"Use File.format instead.", DeprecationWarning)
        return self.format


@dataclass(init=False)
class SequenceFile(File):
    read_index: str
    lane_index: Optional[str]

    def __init__(self,
                 json: JSON,
                 metadata_manifest_entry,
                 manifest: Mapping[str, ManifestEntry]):
        super().__init__(json, metadata_manifest_entry, manifest)
        content = json.get('content', json)
        self.read_index = content['read_index']
        self.lane_index = content.get('lane_index')


@dataclass(init=False)
class SupplementaryFile(File):
    pass


@dataclass(init=False)
class AnalysisFile(File):
    matrix_cell_count: int

    def __init__(self,
                 json: JSON,
                 metadata_manifest_entry,
                 manifest: Mapping[str, ManifestEntry]):
        super().__init__(json, metadata_manifest_entry, manifest)
        content = json.get('content', json)
        self.matrix_cell_count = content.get('matrix_cell_count')


@dataclass(init=False)
class ReferenceFile(File):
    pass


@dataclass(init=False)
class ImageFile(File):
    pass


@dataclass
class Link:
    source_id: UUID4
    source_type: str
    destination_id: UUID4
    destination_type: str
    link_type: str = 'process_link'

    @classmethod
    def from_json(cls, json: JSON, schema_version: Tuple[int]) -> Iterable['Link']:
        if 'source_id' in json:
            # DCP/1 v5 (obsolete)
            yield cls(source_id=UUID4(json['source_id']),
                      source_type=json['source_type'],
                      destination_id=UUID4(json['destination_id']),
                      destination_type=json['destination_type'])
        elif schema_version[0] == 1:
            # DCP/1 vx (current)
            process_id = UUID4(json['process'])
            for source_id in json['inputs']:
                yield cls(source_id=UUID4(source_id),
                          source_type=json['input_type'],
                          destination_id=process_id,
                          destination_type='process')
            for destination_id in json['outputs']:
                yield cls(source_id=process_id,
                          source_type='process',
                          destination_id=UUID4(destination_id),
                          destination_type=json['output_type'])
            for protocol in json['protocols']:
                yield cls(source_id=process_id,
                          source_type='process',
                          destination_id=UUID4(protocol['protocol_id']),
                          destination_type=lookup(protocol, 'type', 'protocol_type'))
        elif schema_version[0] in (2, 3):
            # DCP/2 (current)
            link_type = json['link_type']
            if link_type == 'process_link':
                process_id = UUID4(json['process_id'])
                process_type = json['process_type']
                for input_ in json['inputs']:
                    yield cls(link_type=link_type,
                              source_id=UUID4(input_['input_id']),
                              source_type=input_['input_type'],
                              destination_id=process_id,
                              destination_type=process_type)
                for output in json['outputs']:
                    yield cls(link_type=link_type,
                              source_id=process_id,
                              source_type=process_type,
                              destination_id=UUID4(output['output_id']),
                              destination_type=output['output_type'])
                for protocol in json['protocols']:
                    yield cls(link_type=link_type,
                              source_id=process_id,
                              source_type=process_type,
                              destination_id=UUID4(protocol['protocol_id']),
                              destination_type=protocol['protocol_type'])
            elif link_type == 'supplementary_file_link':
                entity = json['entity']
                for supp_file in json['files']:
                    yield cls(link_type=link_type,
                              source_id=UUID4(entity['entity_id']),
                              source_type=entity['entity_type'],
                              destination_id=UUID4(supp_file['file_id']),
                              destination_type=supp_file['file_type'])
            else:
                assert False, f'Unknown link_type {link_type}'
        else:
            assert False, f'Unknown schema_version {schema_version}'


@dataclass(init=False)
class Bundle:
    uuid: UUID4
    version: str
    projects: MutableMapping[UUID4, Project]
    biomaterials: MutableMapping[UUID4, Biomaterial]
    processes: MutableMapping[UUID4, Process]
    protocols: MutableMapping[UUID4, Protocol]
    files: MutableMapping[UUID4, File]

    manifest: MutableMapping[str, ManifestEntry]
    entities: MutableMapping[UUID4, Entity] = field(repr=False)
    links: List[Link]

    def __init__(self, uuid: str, version: str, manifest: List[JSON], metadata_files: Mapping[str, JSON]):
        self.uuid = UUID4(uuid)
        self.version = version
        self.manifest = {m.name: m for m in map(ManifestEntry, manifest)}

        def from_json(core_cls: Type[E],
                      json_entities: List[Tuple[JSON, ManifestEntry]],
                      **kwargs
                      ) -> MutableMapping[UUID4, E]:
            entities = (
                core_cls.from_json(entity, manifest_entry, **kwargs)
                for entity, manifest_entry in json_entities
            )
            return {entity.document_id: entity for entity in entities}

        if 'project.json' in metadata_files:

            def from_json_v5(core_cls: Type[E], file_name, key=None, **kwargs) -> MutableMapping[UUID4, E]:
                file_content = metadata_files.get(file_name)
                if file_content:
                    manifest_entry = self.manifest.get(file_name)
                    json_entities = file_content[key] if key else [file_content]
                    json_entities = [(json_entity, manifest_entry) for json_entity in json_entities]
                    return from_json(core_cls, json_entities, **kwargs)
                else:
                    return {}

            self.projects = from_json_v5(Project, 'project.json')
            self.biomaterials = from_json_v5(Biomaterial, 'biomaterial.json', 'biomaterials')
            self.processes = from_json_v5(Process, 'process.json', 'processes')
            self.protocols = from_json_v5(Protocol, 'protocol.json', 'protocols')
            self.files = from_json_v5(File, 'file.json', 'files', manifest=self.manifest)

        elif 'project_0.json' in metadata_files:

            json_by_core_cls: MutableMapping[Type[E], List[Tuple[JSON, ManifestEntry]]] = defaultdict(list)
            for file_name, json in metadata_files.items():
                assert file_name.endswith('.json')
                schema_name, _, suffix = file_name[:-5].rpartition('_')
                if schema_name and suffix.isdigit():
                    entity_cls = entity_types[schema_name]
                    core_cls = core_types[entity_cls]
                    json_by_core_cls[core_cls].append((json, self.manifest.get(file_name)))

            def from_json_vx(core_cls: Type[E], **kwargs) -> MutableMapping[UUID4, E]:
                json_entities = json_by_core_cls[core_cls]
                return from_json(core_cls, json_entities, **kwargs)

            self.projects = from_json_vx(Project)
            self.biomaterials = from_json_vx(Biomaterial)
            self.processes = from_json_vx(Process)
            self.protocols = from_json_vx(Protocol)
            self.files = from_json_vx(File, manifest=self.manifest)

        else:

            raise RuntimeError('Unable to detect bundle structure')

        self.entities = {**self.projects, **self.biomaterials, **self.processes, **self.protocols, **self.files}

        links_json = metadata_files['links.json']
        schema_version = tuple(map(int, links_json['schema_version'].split('.')))
        self.links = list(chain.from_iterable(
            Link.from_json(link, schema_version)
            for link in links_json['links']
        ))

        for link in self.links:
            if link.link_type == 'process_link':
                source_entity = self.entities[link.source_id]
                destination_entity = self.entities[link.destination_id]
                assert isinstance(source_entity, LinkedEntity)
                assert isinstance(destination_entity, LinkedEntity)
                source_entity.connect_to(destination_entity, forward=True)
                destination_entity.connect_to(source_entity, forward=False)

    def root_entities(self) -> Mapping[UUID4, LinkedEntity]:
        roots = {}

        class RootFinder(EntityVisitor):

            def visit(self, entity: Entity) -> None:
                if isinstance(entity, LinkedEntity) and not entity.parents:
                    roots[entity.document_id] = entity

        visitor = RootFinder()
        for entity in self.entities.values():
            entity.accept(visitor)

        return roots

    @property
    def specimens(self) -> List[SpecimenFromOrganism]:
        return [s for s in self.biomaterials.values() if isinstance(s, SpecimenFromOrganism)]

    @property
    def sequencing_input(self) -> List[Biomaterial]:
        return [bm for bm in self.biomaterials.values()
                if any(ps.is_sequencing_process() for ps in bm.to_processes.values())]

    @property
    def sequencing_output(self) -> List[SequenceFile]:
        return [f for f in self.files.values()
                if isinstance(f, SequenceFile)
                and any(ps.is_sequencing_process() for ps in f.from_processes.values())]


entity_types = {
    # Biomaterials
    'donor_organism': DonorOrganism,
    'specimen_from_organism': SpecimenFromOrganism,
    'cell_suspension': CellSuspension,
    'cell_line': CellLine,
    'organoid': Organoid,
    'imaged_specimen': ImagedSpecimen,

    # Files
    'analysis_file': AnalysisFile,
    'reference_file': ReferenceFile,
    'sequence_file': SequenceFile,
    'supplementary_file': SupplementaryFile,
    'image_file': ImageFile,

    # Protocols
    'protocol': Protocol,
    'analysis_protocol': AnalysisProtocol,
    'aggregate_generation_protocol': AggregateGenerationProtocol,
    'collection_protocol': CollectionProtocol,
    'differentiation_protocol': DifferentiationProtocol,
    'dissociation_protocol': DissociationProtocol,
    'enrichment_protocol': EnrichmentProtocol,
    'ipsc_induction_protocol': IpscInductionProtocol,
    'imaging_protocol': ImagingProtocol,
    'library_preparation_protocol': LibraryPreparationProtocol,
    'sequencing_protocol': SequencingProtocol,
    'imaging_preparation_protocol': ImagingPreparationProtocol,

    'project': Project,

    # Processes
    'process': Process,
    'analysis_process': AnalysisProcess,
    'dissociation_process': DissociationProcess,
    'enrichment_process': EnrichmentProcess,
    'library_preparation_process': LibraryPreparationProcess,
    'sequencing_process': SequencingProcess
}

schema_names = {
    v: k for k, v in entity_types.items()
}

core_types = {
    entity_type: core_type
    for core_type in (Project, Biomaterial, Process, Protocol, File)
    for entity_type in entity_types.values()
    if issubclass(entity_type, core_type)
}

assert len(entity_types) == len(schema_names), "The mapping from schema name to entity type is not bijective"


def ontology_label(ontology: Optional[Mapping[str, str]],
                   default: Union[str, None, LookupDefault] = LookupDefault.RAISE) -> str:
    """
    Return the best-suited value from the given ontology dictionary.

    >>> ontology_label({'ontology_label': '1', 'text': '2', 'ontology': '3'})
    '1'

    >>> ontology_label({'text': '2', 'ontology': '3'})
    '2'

    >>> ontology_label({'ontology': '3'})
    '3'

    >>> ontology_label({}, default=None)
    >>> ontology_label({}, default='default')
    'default'

    >>> ontology_label(None, default=None)
    >>> ontology_label(None, default='default')
    'default'

    >>> ontology_label({})
    Traceback (most recent call last):
    ...
    KeyError: 'ontology_label'

    >>> ontology_label(None)
    Traceback (most recent call last):
    ...
    TypeError: 'NoneType' object is not subscriptable
    """
    if ontology is None and default is not LookupDefault.RAISE:
        return default
    else:
        return lookup(ontology, 'ontology_label', 'text', 'ontology', default=default)
