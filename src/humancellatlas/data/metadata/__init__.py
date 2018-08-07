from abc import ABC, abstractmethod
from typing import Any, Iterable, List, MutableMapping, Optional, Set, Union, Mapping
from uuid import UUID

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
        described_by = json['content']['describedBy']
        schema_name = described_by.rpartition('/')[2]
        try:
            sub_cls = entity_types[schema_name]
        except KeyError:
            raise TypeLookupError(described_by)
        return sub_cls(json, **kwargs)

    def __init__(self, json: JSON) -> None:
        super().__init__()
        self.json = json
        self.document_id = UUID4(json['hca_ingest']['document_id'])

    @property
    def address(self):
        return schema_names[type(self)] + '@' + self.document_id

    def accept(self, visitor: 'EntityVisitor') -> None:
        visitor.visit(self)


class TypeLookupError(Exception):
    def __init__(self, described_by: str) -> None:
        super().__init__(f"No entity type for schema URL '{described_by}'")


class EntityVisitor(ABC):
    @abstractmethod
    def visit(self, entity: 'Entity') -> None:
        raise NotImplementedError()


@dataclass(init=False)
class LinkedEntity(Entity, ABC):
    children_: MutableMapping[UUID4, Entity] = field(repr=False)
    parents_: MutableMapping[UUID4, 'LinkedEntity'] = field(repr=False)

    @abstractmethod
    def _connect_to(self, other: Entity, forward: bool) -> None:
        raise NotImplementedError()

    def __init__(self, json: JSON) -> None:
        super().__init__(json)
        self.children_ = {}
        self.parents_ = {}

    def connect_to(self, other: Entity, forward: bool) -> None:
        mapping = self.children_ if forward else self.parents_
        mapping[other.document_id] = other
        self._connect_to(other, forward)

    def ancestors(self, visitor: EntityVisitor):
        for parent in self.parents_.values():
            parent.ancestors(visitor)
            visitor.visit(parent)

    def accept(self, visitor: EntityVisitor):
        super().accept(visitor)
        for child in self.children_.values():
            child.accept(visitor)


class LinkError(RuntimeError):
    def __init__(self, entity: LinkedEntity, other_entity: Entity, forward: bool) -> None:
        super().__init__(entity.address +
                         " cannot " + ('reference' if forward else 'be referenced by') +
                         other_entity.address)


@dataclass(init=False)
class Project(Entity):
    project_shortname: Optional[str]
    laboratory_names: Set[str]

    def __init__(self, json: JSON) -> None:
        super().__init__(json)
        content = json['content']
        self.project_shortname = content['project_core']['project_shortname']
        self.laboratory_names = {c.get('laboratory') for c in content['contributors']} - {None}


@dataclass(init=False)
class Biomaterial(LinkedEntity):
    biomaterial_id: str
    has_input_biomaterial: Optional[str]
    from_processes: MutableMapping[UUID4, 'Process'] = field(repr=False)
    to_processes: MutableMapping[UUID4, 'Process']

    def __init__(self, json: JSON) -> None:
        super().__init__(json)
        content = json['content']
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
    biological_sex: str

    def __init__(self, json: JSON):
        super().__init__(json)
        content = json['content']
        self.genus_species = {gs['text'] for gs in content['genus_species']}
        self.disease = {d['text'] for d in content.get('disease', []) if d}
        self.organism_age = content.get('organism_age')
        self.organism_age_unit = content.get('organism_age_unit', {}).get('text')
        self.biological_sex = content['biological_sex']

    @property
    def organism_age_in_seconds(self) -> Optional[AgeRange]:
        if self.organism_age and self.organism_age_unit:
            return AgeRange.parse(self.organism_age, self.organism_age_unit)
        else:
            return None


@dataclass(init=False)
class SpecimenFromOrganism(Biomaterial):
    storage_method: str
    disease: Set[str]
    organ: Optional[str]
    organ_part: Optional[str]

    def __init__(self, json: JSON):
        super().__init__(json)
        content = json['content']
        self.storage_method = content.get('preservation_storage', {}).get('storage_method')
        self.disease = {d['text'] for d in content.get('disease', []) if d}
        self.organ = content.get('organ', {}).get('text')
        self.organ_part = content.get('organ_part', {}).get('text')


@dataclass(init=False)
class CellSuspension(Biomaterial):
    total_estimated_cells: int

    def __init__(self, json: JSON) -> None:
        super().__init__(json)
        self.total_estimated_cells = json['content'].get('total_estimated_cells')


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
        process_core = json['content']['process_core']
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


@dataclass(init=False)
class DissociationProcess(Process):
    pass


@dataclass(init=False)
class EnrichmentProcess(Process):
    pass


@dataclass(init=False)
class LibraryPreparationProcess(Process):
    library_construction_approach: str

    def __init__(self, json: JSON):
        super().__init__(json)
        self.library_construction_approach = json['content']['library_construction_approach']


@dataclass(init=False)
class SequencingProcess(Process):
    instrument_manufacturer_model: str

    def __init__(self, json: JSON):
        super().__init__(json)
        self.instrument_manufacturer_model = json['content']['instrument_manufacturer_model']['text']


@dataclass(init=False)
class Protocol(LinkedEntity):
    protocol_id: str
    protocol_name: Optional[str]

    def __init__(self, json: JSON) -> None:
        super().__init__(json)
        protocol_core = json['content']['protocol_core']
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
        self.library_construction_approach = json['content'].get('library_construction_approach')


@dataclass(init=False)
class SequencingProtocol(Protocol):
    instrument_manufacturer_model: str

    def __init__(self, json: JSON):
        super().__init__(json)
        self.instrument_manufacturer_model = json['content'].get('instrument_manufacturer_model', {}).get('text')


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
    uuid: UUID4
    version: str

    @classmethod
    def from_json(cls, json: JSON):
        kwargs = dict(json)
        kwargs['content_type'] = kwargs.pop('content-type')
        kwargs['uuid'] = UUID4(json['uuid'])
        return cls(**kwargs)


@dataclass(init=False)
class File(LinkedEntity):
    file_format: str
    from_processes: MutableMapping[UUID4, Process] = field(repr=False)
    to_processes: MutableMapping[UUID4, Process]
    manifest_entry: ManifestEntry

    def __init__(self, json: JSON, manifest: Mapping[str, ManifestEntry]):
        super().__init__(json)
        core = json['content']['file_core']
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
        content = json['content']
        self.read_index = content['read_index']
        self.lane_index = content.get('lane_index')


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
    def from_json(cls, json: JSON):
        return cls(source_id=UUID4(json['source_id']),
                   source_type=json['source_type'],
                   destination_id=UUID4(json['destination_id']),
                   destination_type=json['destination_type'])


@dataclass(init=False)
class Bundle:
    uuid: UUID4
    version: str
    project: Project
    biomaterials: MutableMapping[UUID4, Biomaterial]
    processes: MutableMapping[UUID4, Process]
    protocols: MutableMapping[UUID4, Protocol]
    files: MutableMapping[UUID4, File]

    entities_: MutableMapping[UUID4, Entity] = field(repr=False)
    links_: List[Link]
    manifest_: MutableMapping[str, ManifestEntry]

    def __init__(self,
                 uuid: str,
                 version: str,
                 manifest: List[JSON],
                 metadata_files: Mapping[str, JSON]
                 ) -> None:
        self.uuid = UUID4(uuid)
        self.version = version
        manifest = (ManifestEntry.from_json(m) for m in manifest)
        manifest = {m.name: m for m in manifest}

        project = metadata_files['project.json']
        project = Project.from_json(project)

        biomaterials = metadata_files['biomaterial.json']['biomaterials']
        biomaterials = (Biomaterial.from_json(b) for b in biomaterials)
        biomaterials = {b.document_id: b for b in biomaterials}

        processes = metadata_files['process.json']['processes']
        processes = (Process.from_json(p) for p in processes)
        processes = {p.document_id: p for p in processes}

        protocols = metadata_files['protocol.json']['protocols']
        protocols = (Protocol.from_json(p) for p in protocols)
        protocols = {p.document_id: p for p in protocols}

        files = metadata_files['file.json']['files']
        files = (File.from_json(f, manifest=manifest) for f in files)
        files = {f.document_id: f for f in files}

        links = metadata_files['links.json']['links']
        links = [Link.from_json(l) for l in links]

        entities = {**biomaterials,
                    **processes,
                    **protocols,
                    **files,
                    project.document_id: project}

        for link in links:
            source_entity = entities[link.source_id]
            destination_entity = entities[link.destination_id]
            assert isinstance(source_entity, LinkedEntity)
            assert isinstance(destination_entity, LinkedEntity)
            source_entity.connect_to(destination_entity, forward=True)
            destination_entity.connect_to(source_entity, forward=False)

        self.project = project
        self.biomaterials = biomaterials
        self.processes = processes
        self.protocols = protocols
        self.files = files
        self.entities_ = entities
        self.links_ = links
        self.manifest_ = manifest

    def root_entities(self) -> Mapping[UUID4, LinkedEntity]:

        roots = {}

        class RootFinder(EntityVisitor):
            def visit(self, entity: Entity) -> None:
                if isinstance(entity, LinkedEntity) and not entity.parents_:
                    roots[entity.document_id] = entity

        visitor = RootFinder()
        for entity in self.entities_.values():
            entity.accept(visitor)

        return roots

    @property
    def specimens(self) -> Iterable[SpecimenFromOrganism]:
        return [s for s in self.biomaterials.values() if isinstance(s, SpecimenFromOrganism)]

    @property
    def sequencing_input(self) -> Iterable[CellSuspension]:
        return [cs for cs in self.biomaterials.values() if isinstance(cs, CellSuspension)
                and any(isinstance(pr, SequencingProcess) for pr in cs.to_processes.values())]


entity_types = {
    'donor_organism': DonorOrganism,
    'specimen_from_organism': SpecimenFromOrganism,
    'cell_suspension': CellSuspension,
    'cell_line': CellLine,
    'organoid': Organoid,
    'analysis_file': AnalysisFile,
    'reference_file': ReferenceFile,
    'sequence_file': SequenceFile,
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
    'protocol': Protocol,
    'process': Process,
    'dissociation_process': DissociationProcess,
    'enrichment_process': EnrichmentProcess,
    'library_preparation_process': LibraryPreparationProcess,
    'sequencing_process': SequencingProcess
}

schema_names = {
    v: k for k, v in entity_types.items()
}

assert len(entity_types) == len(schema_names), "The mapping from schema name to entity type is not bijective"
