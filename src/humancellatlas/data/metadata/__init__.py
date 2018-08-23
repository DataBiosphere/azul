from abc import ABC, abstractmethod
from collections import defaultdict
from itertools import chain
from typing import Any, Iterable, List, MutableMapping, Optional, Set, Union, Mapping, TypeVar, Type
from uuid import UUID
import warnings

from dataclasses import dataclass, field

from humancellatlas.data.metadata.age_range import AgeRange

# A few helpful type aliases
#
UUID4 = UUID
AnyJSON2 = Union[str, int, float, bool, None, Mapping[str, Any], List[Any]]
AnyJSON1 = Union[str, int, float, bool, None, Mapping[str, AnyJSON2], List[AnyJSON2]]
AnyJSON = Union[str, int, float, bool, None, Mapping[str, AnyJSON1], List[AnyJSON1]]
JSON = Mapping[str, AnyJSON]


@dataclass(init=False)
class Entity:
    json: JSON = field(repr=False)
    document_id: UUID4

    @classmethod
    def from_json(cls, json: JSON, **kwargs):
        content = json.get('content', json)
        described_by = content['describedBy']
        schema_name = described_by.rpartition('/')[2]
        try:
            sub_cls = entity_types[schema_name]
        except KeyError:
            raise TypeLookupError(described_by)
        return sub_cls(json, **kwargs)

    def __init__(self, json: JSON) -> None:
        super().__init__()
        self.json = json
        provenance = json.get('hca_ingest') or json['provenance']
        self.document_id = UUID4(provenance['document_id'])

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

    def __init__(self, json: JSON) -> None:
        super().__init__(json)
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


@dataclass(init=False)
class Project(Entity):
    project_short_name: Optional[str]
    laboratory_names: Set[str]

    def __init__(self, json: JSON) -> None:
        super().__init__(json)
        content = json.get('content', json)
        core = content['project_core']
        self.project_short_name = core.get('project_shortname') or core['project_short_name']
        self.laboratory_names = {c.get('laboratory') for c in content['contributors']} - {None}

    @property
    def project_shortname(self):
        warnings.warn(f"Project.project_shortname is deprecated. "
                      f"Use project_short_name instead.", DeprecationWarning)
        return self.project_short_name


@dataclass(init=False)
class Biomaterial(LinkedEntity):
    biomaterial_id: str
    has_input_biomaterial: Optional[str]
    from_processes: MutableMapping[UUID4, 'Process'] = field(repr=False)
    to_processes: MutableMapping[UUID4, 'Process']

    def __init__(self, json: JSON) -> None:
        super().__init__(json)
        content = json.get('content', json)
        self.biomaterial_id = content['biomaterial_core']['biomaterial_id']
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
    disease: Set[str]
    organism_age: str
    organism_age_unit: str
    sex: str

    def __init__(self, json: JSON):
        super().__init__(json)
        content = json.get('content', json)
        self.genus_species = {gs['text'] for gs in content['genus_species']}
        self.disease = {d['text'] for d in content.get('disease', []) if d}
        self.organism_age = content.get('organism_age')
        self.organism_age_unit = content.get('organism_age_unit', {}).get('text')
        self.sex = content.get('biological_sex') or content['sex']

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


@dataclass(init=False)
class SpecimenFromOrganism(Biomaterial):
    storage_method: str
    disease: Set[str]
    organ: Optional[str]
    organ_part: Optional[str]

    def __init__(self, json: JSON):
        super().__init__(json)
        content = json.get('content', json)
        self.storage_method = content.get('preservation_storage', {}).get('storage_method')
        self.disease = {d['text'] for d in content.get('disease', []) if d}
        self.organ = content.get('organ', {}).get('text')
        self.organ_part = content.get('organ_part', {}).get('text')


@dataclass(init=False)
class CellSuspension(Biomaterial):
    total_estimated_cells: int

    def __init__(self, json: JSON) -> None:
        super().__init__(json)
        content = json.get('content', json)
        self.total_estimated_cells = content.get('total_estimated_cells')


@dataclass(init=False)
class CellLine(Biomaterial):
    pass


@dataclass(init=False)
class Organoid(Biomaterial):
    pass


@dataclass(init=False)
class Process(LinkedEntity):
    process_id: str
    process_name: Optional[str]
    input_biomaterials: MutableMapping[UUID4, Biomaterial] = field(repr=False)
    input_files: MutableMapping[UUID4, 'File'] = field(repr=False)
    output_biomaterials: MutableMapping[UUID4, Biomaterial]
    output_files: MutableMapping[UUID4, 'File']
    protocols: MutableMapping[UUID4, 'Protocol']

    def __init__(self, json: JSON) -> None:
        super().__init__(json)
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
    def __init__(self, json: JSON) -> None:
        warnings.warn(f"{type(self)} is deprecated", DeprecationWarning)
        super().__init__(json)


@dataclass(init=False)
class EnrichmentProcess(Process):
    def __init__(self, json: JSON) -> None:
        warnings.warn(f"{type(self)} is deprecated", DeprecationWarning)
        super().__init__(json)


@dataclass(init=False)
class LibraryPreparationProcess(Process):
    library_construction_approach: str

    def __init__(self, json: JSON):
        warnings.warn(f"{type(self)} is deprecated", DeprecationWarning)
        super().__init__(json)
        content = json.get('content', json)
        self.library_construction_approach = content['library_construction_approach']


@dataclass(init=False)
class SequencingProcess(Process):
    instrument_manufacturer_model: str

    def __init__(self, json: JSON):
        warnings.warn(f"{type(self)} is deprecated", DeprecationWarning)
        super().__init__(json)
        content = json.get('content', json)
        self.instrument_manufacturer_model = content['instrument_manufacturer_model']['text']

    def is_sequencing_process(self):
        return True


@dataclass(init=False)
class Protocol(LinkedEntity):
    protocol_id: str
    protocol_name: Optional[str]

    def __init__(self, json: JSON) -> None:
        super().__init__(json)
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
    library_construction_approach: str

    def __init__(self, json: JSON) -> None:
        super().__init__(json)
        content = json.get('content', json)
        lca = content.get('library_construction_approach')
        self.library_construction_approach = lca['text'] if isinstance(lca, dict) else lca


@dataclass(init=False)
class SequencingProtocol(Protocol):
    instrument_manufacturer_model: str

    def __init__(self, json: JSON):
        super().__init__(json)
        content = json.get('content', json)
        self.instrument_manufacturer_model = content.get('instrument_manufacturer_model', {}).get('text')


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
    pass


@dataclass
class ManifestEntry:
    content_type: str
    crc32c: str
    indexed: bool
    name: str
    s3_etag: str
    sha1: str
    sha256: str
    size: int
    url: str
    uuid: UUID4
    version: str

    @classmethod
    def from_json(cls, json: JSON):
        kwargs = dict(json)
        kwargs['content_type'] = kwargs.pop('content-type')
        kwargs['uuid'] = UUID4(json['uuid'])
        kwargs.setdefault('url')
        return cls(**kwargs)


@dataclass(init=False)
class File(LinkedEntity):
    file_format: str
    from_processes: MutableMapping[UUID4, Process] = field(repr=False)
    to_processes: MutableMapping[UUID4, Process]
    manifest_entry: ManifestEntry

    def __init__(self, json: JSON, manifest: Mapping[str, ManifestEntry]):
        super().__init__(json)
        content = json.get('content', json)
        core = content['file_core']
        self.file_format = core['file_format']
        self.manifest_entry = manifest[core['file_name']]
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
class SequenceFile(File):
    read_index: str
    lane_index: Optional[str]

    def __init__(self, json: JSON, manifest: Mapping[str, ManifestEntry]):
        super().__init__(json, manifest)
        content = json.get('content', json)
        self.read_index = content['read_index']
        self.lane_index = content.get('lane_index')


@dataclass(init=False)
class SupplementaryFile(File):
    pass


@dataclass(init=False)
class AnalysisFile(File):
    pass


@dataclass(init=False)
class ReferenceFile(File):
    pass


@dataclass
class Link:
    source_id: UUID4
    source_type: str
    destination_id: UUID4
    destination_type: str

    @classmethod
    def from_json(cls, json: JSON) -> Iterable['Link']:
        if 'source_id' in json:
            # v5
            yield cls(source_id=UUID4(json['source_id']),
                      source_type=json['source_type'],
                      destination_id=UUID4(json['destination_id']),
                      destination_type=json['destination_type'])
        else:
            # vx
            process_id = UUID4(json['process'])
            for source_id in json['inputs']:
                yield cls(source_id=UUID4(source_id), source_type=json['input_type'],
                          destination_id=process_id, destination_type='process')
            for destination_id in json['outputs']:
                yield cls(source_id=process_id, source_type='process',
                          destination_id=UUID4(destination_id), destination_type=json['output_type'])
            for protocol in json['protocols']:
                yield cls(source_id=process_id, source_type='process',
                          destination_id=UUID4(protocol['protocol_id']), destination_type=protocol['protocol_type'])


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
        self.manifest = {m.name: m for m in map(ManifestEntry.from_json, manifest)}

        def from_json(core_cls: Type[E], json_entities: List[JSON], **kwargs) -> MutableMapping[UUID4, E]:
            entities = (core_cls.from_json(entity, **kwargs) for entity in json_entities)
            return {entity.document_id: entity for entity in entities}

        if 'project.json' in metadata_files:

            def from_json_v5(core_cls: Type[E], file_name, key=None, **kwargs) -> MutableMapping[UUID4, E]:
                file_content = metadata_files.get(file_name)
                if file_content:
                    json_entities = file_content[key] if key else [file_content]
                    return from_json(core_cls, json_entities, **kwargs)
                else:
                    return {}

            self.projects = from_json_v5(Project, 'project.json')
            self.biomaterials = from_json_v5(Biomaterial, 'biomaterial.json', 'biomaterials')
            self.processes = from_json_v5(Process, 'process.json', 'processes')
            self.protocols = from_json_v5(Protocol, 'protocol.json', 'protocols')
            self.files = from_json_v5(File, 'file.json', 'files', manifest=self.manifest)

        elif 'project_0.json' in metadata_files:

            json_by_core_cls: MutableMapping[Type[E], List[JSON]] = defaultdict(list)
            for file_name, json in metadata_files.items():
                assert file_name.endswith('.json')
                schema_name, _, suffix = file_name[:-5].rpartition('_')
                if schema_name and suffix.isdigit():
                    entity_cls = entity_types.get(schema_name)
                    core_cls = core_types[entity_cls]
                    json_by_core_cls[core_cls].append(json)

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

        links = metadata_files['links.json']['links']
        self.links = list(chain.from_iterable(map(Link.from_json, links)))

        for link in self.links:
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
    def sequencing_input(self) -> List[CellSuspension]:
        return [bm for bm in self.biomaterials.values()
                if isinstance(bm, CellSuspension)
                and any(ps.is_sequencing_process() for ps in bm.to_processes.values())]

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

    # Files
    'analysis_file': AnalysisFile,
    'reference_file': ReferenceFile,
    'sequence_file': SequenceFile,
    'supplementary_file': SupplementaryFile,

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
