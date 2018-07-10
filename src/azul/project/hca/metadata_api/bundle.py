from abc import ABC, abstractmethod
from types import SimpleNamespace
from typing import List, MutableMapping, Any, Set, Optional, Union, Iterable
from uuid import UUID

from dataclasses import dataclass

from azul.dss_bundle import DSSBundle
from azul.project.hca.metadata_api.expando import Expando
from azul.types import AnyJSON, JSON

UUID4 = UUID
_ObjectifiedJSON = Union[str, int, float, bool, None, MutableMapping[str, Any], List[Any]]
ObjectifiedJSON = Union[MutableMapping[str, _ObjectifiedJSON], SimpleNamespace]

mydataclass = dataclass(init=False)


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
    def from_metadata(cls, json: ObjectifiedJSON):
        json.content_type = json.pop("content-type")
        # noinspection PyTypeChecker
        json.uuid = UUID4(json.uuid)
        # noinspection PyArgumentList
        return cls(**json)


class EntityVisitor(ABC):

    @abstractmethod
    def visit(self, entity: 'Entity') -> None:
        raise NotImplementedError()


@mydataclass
class Entity:
    document_id: UUID4
    _json: ObjectifiedJSON
    _source: str

    @classmethod
    def from_metadata(cls, json: ObjectifiedJSON):
        described_by = json.content.describedBy.rpartition('/')[2]
        sub_cls = object_types[described_by]
        return sub_cls(json)

    def __init__(self, json: ObjectifiedJSON) -> None:
        super().__init__()
        self._source = json.content.describedBy.rpartition('/')[2]
        self._json = json
        self.document_id = UUID4(json.hca_ingest.document_id)

    def accept(self, visitor: EntityVisitor) -> None:
        visitor.visit(self)


@mydataclass
class LinkedEntity(Entity, ABC):
    _children: MutableMapping[UUID4, Entity]
    _parents: MutableMapping[UUID4, Entity]

    @abstractmethod
    def _connect_to(self, entity: Entity, forward: bool) -> None:
        """
        TODO:
        :param entity:
        :param forward:
        :return:
        """
        raise NotImplementedError()

    def __init__(self, json: ObjectifiedJSON) -> None:
        super().__init__(json)
        self._children = {}
        self._parents = {}

    def connect_to(self, entity: Entity, forward: bool) -> None:
        mapping = self._children if forward else self._parents
        mapping[entity.document_id] = entity
        self._connect_to(entity, forward)

    def ancestors(self, visitor: EntityVisitor):
        for parent in self._parents.values():
            parent.ancestors(visitor)
            visitor.visit(parent)

    def accept(self, visitor: EntityVisitor):
        super().accept(visitor)
        for child in self._children.values():
            child.accept(visitor)


@mydataclass
class Project(Entity):
    project_shortname: Optional[str]
    laboratory_names: Set[str]

    def __init__(self, json: ObjectifiedJSON) -> None:
        # TODO: Add if statements if version causes changes on certain fields
        super().__init__(json)
        self.project_shortname = json.content.project_core.project_shortname
        self.laboratory_names = {c.get("laboratory") for c in json.content.contributors} - {None}


@mydataclass
class Biomaterial(LinkedEntity):
    biomaterial_id: str
    has_input_biomaterial: Optional[str]
    from_processes: MutableMapping[UUID4, 'Process']
    to_processes: MutableMapping[UUID4, 'Process']

    def __init__(self, json: ObjectifiedJSON) -> None:
        super().__init__(json)
        self.biomaterial_id = json.content.biomaterial_core.biomaterial_id
        self.has_input_biomaterial = json.content.biomaterial_core.get("has_input_biomaterial")
        self.from_processes = {}
        self.to_processes = {}

    def _connect_to(self, entity: Entity, forward: bool) -> None:
        if isinstance(entity, Process):
            if forward:
                self.to_processes[entity.document_id] = entity
            else:
                self.from_processes[entity.document_id] = entity
        else:
            raise ValueError(f"Biomaterial only connects to process, not {entity}")


@mydataclass
class DonorOrganism(Biomaterial):
    genus_species: Set[str]
    disease: Set[str]
    organism_age: str
    organism_age_unit: str
    biological_sex: str

    def __init__(self, json: ObjectifiedJSON):
        super().__init__(json)
        self.genus_species = {gs.text for gs in json.content.genus_species}
        self.disease = {d.text for d in json.content.get("disease", []) if d}
        self.organism_age = json.content.get("organism_age")
        self.organism_age_unit = json.content.get("organism_age_unit", {}).get("text")
        self.biological_sex = json.content.biological_sex

    @property
    def organism_age_in_seconds(self):
        return AgeRange(self.organism_age, self.organism_age_unit)


@mydataclass
class SpecimenFromOrganism(Biomaterial):
    storage_method: str
    disease: Set[str]
    organ: Optional[str]
    organ_part: Optional[str]

    def __init__(self, json: ObjectifiedJSON):
        super().__init__(json)
        self.storage_method = json.content.get("preservation_storage", {}).get("storage_method")
        self.disease = {d.text for d in json.content.get("disease", []) if d}
        self.organ = json.content.get("organ", {}).get("text")
        self.organ_part = json.content.get("organ_part", {}).get("text")


@mydataclass
class CellSuspension(Biomaterial):
    total_estimated_cells: int

    def __init__(self, json: ObjectifiedJSON) -> None:
        super().__init__(json)
        self.total_estimated_cells = json.content.get("total_estimated_cells")


@mydataclass
class CellLine(Biomaterial):

    def __init__(self, json: ObjectifiedJSON) -> None:
        super().__init__(json)


@mydataclass
class Organoid(Biomaterial):

    def __init__(self, json: ObjectifiedJSON) -> None:
        super().__init__(json)


@mydataclass
class Process(LinkedEntity):
    process_id: str
    process_name: Optional[str]
    input_biomaterials: MutableMapping[UUID4, Biomaterial]
    input_files: MutableMapping[UUID4, 'File']
    output_biomaterials: MutableMapping[UUID4, Biomaterial]
    output_files: MutableMapping[UUID4, 'File']
    protocols: MutableMapping[UUID4, 'Protocol']

    def __init__(self, json: ObjectifiedJSON) -> None:
        # TODO: Add if statements if version causes changes on certain fields
        super().__init__(json)
        self.process_id = json.content.process_core.process_id
        self.process_name = json.content.process_core.get('process_name')
        self.input_biomaterials = {}
        self.input_files = {}
        self.output_biomaterials = {}
        self.output_files = {}
        self.protocols = {}

    def _connect_to(self, entity: Entity, forward: bool) -> None:
        if isinstance(entity, Biomaterial):
            if forward:
                self.output_biomaterials[entity.document_id] = entity
            else:
                self.input_biomaterials[entity.document_id] = entity
        elif isinstance(entity, File):
            if forward:
                self.output_files[entity.document_id] = entity
            else:
                self.input_files[entity.document_id] = entity
        elif isinstance(entity, Protocol):
            if forward:
                self.protocols[entity.document_id] = entity
            else:
                raise ValueError(f"Protocol cannot reference process")
        else:
            raise ValueError(f"Process cannot connect to {entity}")


# WILL BE DEPRECATED
@mydataclass
class DissociationProcess(Process):

    def __init__(self, json: ObjectifiedJSON) -> None:
        super().__init__(json)


# WILL BE DEPRECATED
@mydataclass
class EnrichmentProcess(Process):

    def __init__(self, json: ObjectifiedJSON) -> None:
        super().__init__(json)


# WILL BE DEPRECATED
@mydataclass
class LibraryPreparationProcess(Process):
    library_construction_approach: str

    def __init__(self, json: ObjectifiedJSON):
        super().__init__(json)
        self.library_construction_approach = json.content.library_construction_approach


@mydataclass
class SequencingProcess(Process):
    instrument_manufacturer_model: str

    def __init__(self, json: ObjectifiedJSON):
        super().__init__(json)
        self.instrument_manufacturer_model = json.content.instrument_manufacturer_model.text


@mydataclass
class Protocol(LinkedEntity):
    protocol_id: str
    protocol_name: Optional[str]

    def __init__(self, json: ObjectifiedJSON) -> None:
        super().__init__(json)
        self.protocol_id = json.content.protocol_core.protocol_id
        self.protocol_name = json.content.protocol_core.get("protocol_name")

    def _connect_to(self, entity: Entity, forward: bool) -> None:
        if isinstance(entity, Process) and not forward:
            pass
        else:
            verb = 'reference' if forward else 'be referenced by'
            raise ValueError(f"Protocol cannot {verb} {entity}")


@mydataclass
class LibraryPreparationProtocol(Protocol):
    library_construction_approach: str

    def __init__(self, json: ObjectifiedJSON) -> None:
        super().__init__(json)
        self.library_construction_approach = json.content.get("library_construction_approach")


@mydataclass
class SequencingProtocol(Protocol):
    instrument_manufacturer_model: str

    def __init__(self, json: ObjectifiedJSON):
        super().__init__(json)
        self.instrument_manufacturer_model = json.content.get("instrument_manufacturer_model",
                                                              {}).get("text")


@mydataclass
class AnalysisProtocol(Protocol):

    def __init__(self, json: ObjectifiedJSON) -> None:
        super().__init__(json)


@mydataclass
class AggregateGenerationProtocol(Protocol):

    def __init__(self, json: ObjectifiedJSON) -> None:
        super().__init__(json)


@mydataclass
class CollectionProtocol(Protocol):

    def __init__(self, json: ObjectifiedJSON) -> None:
        super().__init__(json)


@mydataclass
class DifferentiationProtocol(Protocol):

    def __init__(self, json: ObjectifiedJSON) -> None:
        super().__init__(json)


@mydataclass
class DissociationProtocol(Protocol):

    def __init__(self, json: ObjectifiedJSON) -> None:
        super().__init__(json)


@mydataclass
class EnrichmentProtocol(Protocol):

    def __init__(self, json: ObjectifiedJSON) -> None:
        super().__init__(json)


@mydataclass
class IpscInductionProtocol(Protocol):

    def __init__(self, json: ObjectifiedJSON) -> None:
        super().__init__(json)


@mydataclass
class ImagingProtocol(Protocol):

    def __init__(self, json: ObjectifiedJSON) -> None:
        super().__init__(json)


@mydataclass
class File(LinkedEntity):
    file_format: str
    from_processes: MutableMapping[UUID4, Process]
    to_processes: MutableMapping[UUID4, Process]
    manifest_entry: ManifestEntry

    def __init__(self, json: ObjectifiedJSON):
        super().__init__(json)
        self.file_format = json.content.file_core.file_format
        self.from_processes = {}
        self.to_processes = {}

    def _connect_to(self, entity: Entity, forward: bool) -> None:
        if isinstance(entity, Process):
            if forward:
                self.to_processes[entity.document_id] = entity
            else:
                self.from_processes[entity.document_id] = entity
        else:
            raise ValueError(f"File only connects to process, not {entity}")


@mydataclass
class SequenceFile(File):
    read_index: str
    lane_index: Optional[str]

    def __init__(self, json: ObjectifiedJSON):
        super().__init__(json)
        self.read_index = json.content.read_index
        self.lane_index = json.content.get("lane_index")


@mydataclass
class AnalysisFile(File):

    def __init__(self, json: ObjectifiedJSON):
        super().__init__(json)


@mydataclass
class ReferenceFile(File):
    pass


@dataclass
class Link:
    source_id: UUID4
    source_type: str
    destination_id: UUID4
    destination_type: str

    @classmethod
    def from_metadata(cls, json: ObjectifiedJSON):
        # noinspection PyTypeChecker
        json.source_id = UUID4(json.source_id)
        # noinspection PyTypeChecker
        json.destination_id = UUID4(json.destination_id)
        # noinspection PyArgumentList
        return cls(**json)


@mydataclass
class Bundle:
    uuid: UUID4
    version: str
    project: Project
    files: MutableMapping[UUID4, File]
    biomaterials: MutableMapping[UUID4, Biomaterial]
    processes: MutableMapping[UUID4, Process]
    protocols: MutableMapping[UUID4, Protocol]
    entities: MutableMapping[UUID4, Entity]

    _links: List[Link]
    _manifest: MutableMapping[UUID4, ManifestEntry]

    def __init__(self, dss_bundle: DSSBundle) -> None:
        self.uuid = UUID4(dss_bundle.uuid)
        self.version = dss_bundle.version

        project = objectify(dss_bundle.metadata_files['project.json'])
        project = Project.from_metadata(project)

        biomaterials = objectify(dss_bundle.metadata_files["biomaterial.json"])
        biomaterials = (Biomaterial.from_metadata(b) for b in biomaterials.biomaterials)
        biomaterials = {b.document_id: b for b in biomaterials}

        processes = objectify(dss_bundle.metadata_files["process.json"])
        processes = (Process.from_metadata(p) for p in processes.processes)
        processes = {p.document_id: p for p in processes}

        protocols = objectify(dss_bundle.metadata_files["protocol.json"])
        protocols = (Protocol.from_metadata(p) for p in protocols.protocols)
        protocols = {p.document_id: p for p in protocols}

        files = objectify(dss_bundle.metadata_files["file.json"])
        files = (File.from_metadata(f) for f in files.files)
        files = {f.document_id: f for f in files}

        links = [Link.from_metadata(objectify(l))
                 for l in dss_bundle.metadata_files["links.json"]["links"]]

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
        self._links = links
        self._manifest = {UUID4(m["uuid"]): ManifestEntry.from_metadata(objectify(m))
                          for m in dss_bundle.manifest}
        for k, v in self._manifest.items():
            if k in self.files:
                self.files[k].manifest_entry = v
        self.entities = entities

    @property
    def specimens(self) -> Iterable[SpecimenFromOrganism]:
        return [s for s in self.biomaterials.values() if isinstance(s, SpecimenFromOrganism)]

    # v5
    @property
    def sequencing_input(self) -> Iterable[CellSuspension]:
        return [cs for cs in self.biomaterials.values() if isinstance(cs, CellSuspension)
                and any(isinstance(pr, SequencingProcess)
                        for pr in cs.to_processes.values())]


object_types = {
    "donor_organism": DonorOrganism,
    "specimen_from_organism": SpecimenFromOrganism,
    "cell_suspension": CellSuspension,
    "cell_line": CellLine,
    "organoid": Organoid,
    "analysis_file": AnalysisFile,
    "reference_file": ReferenceFile,
    "sequence_file": SequenceFile,
    "analysis_protocol": AnalysisProtocol,
    "aggregate_generation_protocol": AggregateGenerationProtocol,
    "collection_protocol": CollectionProtocol,
    "differentiation_protocol": DifferentiationProtocol,
    "dissociation_protocol": DissociationProtocol,
    "enrichment_protocol": EnrichmentProtocol,
    "ipsc_induction_protocol": IpscInductionProtocol,
    "imaging_protocol": ImagingProtocol,
    "library_preparation_protocol": LibraryPreparationProtocol,
    "sequencing_protocol": SequencingProtocol,
    "project": Project,
    "protocol": Protocol,
    "process": Process,

    "dissociation_process": DissociationProcess,
    "enrichment_process": EnrichmentProcess,
    "library_preparation_process": LibraryPreparationProcess,
    "sequencing_process": SequencingProcess
}


def objectify(j: JSON) -> ObjectifiedJSON:
    assert isinstance(j, dict)
    return _objectify(j)


def _objectify(j: AnyJSON) -> _ObjectifiedJSON:
    if isinstance(j, dict):
        return Expando({k: _objectify(v) for k, v in j.items()})
    elif isinstance(j, list):
        return [_objectify(v) for v in j]
    else:
        return j


# First create project
# Then create Bundle, injecting project
# Leave the Dictionaries empty.
# Then go through biomaterials, processes and protocols, and create their respective objects.
# Add the objects to the object dictionaries in the Bundle object
# Then walk thorough links and fill out the edges.


@mydataclass
class AgeRange:
    """
    >>> AgeRange("", "second")
    AgeRange(min=None, max=None)

    >>> AgeRange(" 1 - 2 ", "second")
    AgeRange(min=1, max=2)

    >>> AgeRange(" - ", "second")
    AgeRange(min=0, max=315360000000)

    >>> AgeRange("1-", "seconds")
    AgeRange(min=1, max=315360000000)

    >>> AgeRange("-2", "seconds")
    AgeRange(min=0, max=2)
    """
    min: int
    max: int

    FACTORS = dict(year=365 * 24 * 3600,
                   month=365 * 24 * 3600 / 12,
                   week=7 * 24 * 3600,
                   day=24 * 3600,
                   hour=3600,
                   minute=60,
                   second=1)

    MAX_AGE = 10000 * FACTORS["year"]

    def __init__(self, age: str, unit: str) -> None:
        age = [s.strip() for s in age.split("-")]

        def cvt(value: str, default: Optional[int]) -> Optional[int]:
            if value:
                u = unit.lower().strip()
                try:
                    f = self.FACTORS[u]
                except KeyError:
                    if u.endswith("s"):
                        try:
                            f = self.FACTORS[u[:-1]]
                        except KeyError:
                            return None
                    else:
                        return None
                return f * int(value)
            else:
                return default

        if len(age) == 1:
            self.min = cvt(age[0], None)
            self.max = self.min
        elif len(age) == 2:
            self.min = cvt(age[0], 0)
            self.max = cvt(age[1], self.MAX_AGE)
        else:
            self.min = None
            self.max = None
