from abc import ABCMeta, abstractmethod
import logging
from typing import Any, Callable, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Set, Tuple

from humancellatlas.data.metadata import api
from humancellatlas.data.metadata.helpers.json import as_json

from azul import reject
from azul.transformer import (Accumulator,
                              AggregatingTransformer,
                              Contribution,
                              DistinctAccumulator,
                              Document,
                              EntityReference,
                              GroupingAggregator,
                              ListAccumulator,
                              MandatoryValueAccumulator,
                              MaxAccumulator,
                              MinAccumulator,
                              OptionalValueAccumulator,
                              PriorityOptionalValueAccumulator,
                              SetAccumulator,
                              PrioritySetAccumulator,
                              SimpleAggregator,
                              SumAccumulator)
from azul.types import JSON

log = logging.getLogger(__name__)


def _project_dict(project: api.Project) -> JSON:
    # Store lists of all values of each of these facets to allow facet filtering
    # and term counting on the webservice
    laboratories: Set[str] = set()
    institutions: Set[str] = set()
    contact_names: Set[str] = set()
    publication_titles: Set[str] = set()

    for contributor in project.contributors:
        if contributor.laboratory:
            laboratories.add(contributor.laboratory)
        if contributor.contact_name:
            contact_names.add(contributor.contact_name)
        if contributor.institution:
            institutions.add(contributor.institution)

    for publication in project.publications:
        if publication.publication_title:
            publication_titles.add(publication.publication_title)

    return {
        'project_title': project.project_title,
        'project_description': project.project_description,
        'project_shortname': project.project_short_name,
        'laboratory': list(laboratories),
        'institutions': list(institutions),
        'contact_names': list(contact_names),
        'contributors': as_json(project.contributors),
        'document_id': str(project.document_id),
        'publication_titles': list(publication_titles),
        'publications': as_json(project.publications),
        '_type': 'project'
    }


def _specimen_dict(entity: api.LinkedEntity, visited_entities) -> JSON:
    visitor = SpecimenVisitor(visited_entities)
    entity.accept(visitor)
    entity.ancestors(visitor)
    return visitor.merged_specimen


def _cell_suspension_dict(cell_suspension: api.CellSuspension) -> JSON:
    visitor = CellSuspensionVisitor()
    # Visit the cell suspension but don't descend. We're only interested in parent biomaterials
    visitor.visit(cell_suspension)
    cell_suspension.ancestors(visitor)
    return visitor.merged_cell_suspension


def _file_dict(file: api.File) -> JSON:
    return {
        'content-type': file.manifest_entry.content_type,
        'indexed': file.manifest_entry.indexed,
        'name': file.manifest_entry.name,
        'sha256': file.manifest_entry.sha256,
        'size': file.manifest_entry.size,
        'uuid': file.manifest_entry.uuid,
        'version': file.manifest_entry.version,
        'document_id': str(file.document_id),
        'file_format': file.file_format,
        '_type': 'file',
        **(
            {
                'read_index': file.read_index,
                'lane_index': file.lane_index
            } if isinstance(file, api.SequenceFile) else {
            }
        )
    }


def _protocol_dict(protocol: api.Protocol) -> JSON:
    protocol_dict = {"document_id": protocol.document_id}
    if isinstance(protocol, api.LibraryPreparationProtocol):
        protocol_dict['library_construction_approach'] = protocol.library_construction_approach
    elif isinstance(protocol, api.SequencingProtocol):
        protocol_dict['instrument_manufacturer_model'] = protocol.instrument_manufacturer_model
    else:
        assert False
    return protocol_dict


class TransformerVisitor(api.EntityVisitor):
    # Entities are tracked by ID to ensure uniqueness if an entity is visited twice while descending the entity DAG
    specimens: MutableMapping[api.UUID4, api.SpecimenFromOrganism]
    cell_suspensions: MutableMapping[api.UUID4, api.CellSuspension]
    protocols: MutableMapping[api.UUID4, api.Protocol]
    files: MutableMapping[api.UUID4, api.File]
    visited_entities: Set[api.UUID4]

    def __init__(self) -> None:
        self.specimens = {}
        self.cell_suspensions = {}
        self.protocols = {}
        self.files = {}
        self.visited_entities = set()

    def visit(self, entity: api.Entity) -> None:
        self.visited_entities.add(entity.document_id)
        if isinstance(entity, api.SpecimenFromOrganism):
            self.specimens[entity.document_id] = entity
        elif isinstance(entity, api.CellSuspension):
            self.cell_suspensions[entity.document_id] = entity
        elif isinstance(entity, api.Process):
            for protocol in entity.protocols.values():
                if isinstance(protocol, (api.SequencingProtocol, api.LibraryPreparationProtocol)):
                    self.protocols[protocol.document_id] = protocol
        elif isinstance(entity, api.File):
            if entity.file_format == 'unknown' and '.zarr!' in entity.manifest_entry.name:
                # FIXME: Remove once https://github.com/HumanCellAtlas/metadata-schema/issues/579 is resolved
                #
                return
            self.files[entity.document_id] = entity


class BiomaterialVisitor(api.EntityVisitor, metaclass=ABCMeta):

    def __init__(self, visited_entities = None) -> None:
        self.visited_entities = visited_entities
        self._accumulators: MutableMapping[str, Accumulator] = {}
        self._biomaterials: MutableMapping[api.UUID4, api.Biomaterial] = dict()

    def _set(self, field: str, accumulator_factory: Callable[[], Accumulator], value: Any):
        try:
            accumulator = self._accumulators[field]
        except KeyError:
            self._accumulators[field] = accumulator = accumulator_factory()
        accumulator.accumulate(value)

    def visit(self, entity: api.Entity) -> None:
        if isinstance(entity, api.Biomaterial) and entity.document_id not in self._biomaterials:
            self._biomaterials[entity.document_id] = entity
            self._visit(entity)

    @abstractmethod
    def _visit(self, entity: api.Biomaterial) -> None:
        raise NotImplementedError()


class SpecimenVisitor(BiomaterialVisitor):

    def _visit(self, entity: api.Biomaterial) -> None:
        if entity.document_id in self.visited_entities:
            self._set('has_input_biomaterial', SetAccumulator, entity.has_input_biomaterial)
            self._set('_source', SetAccumulator, api.schema_names[type(entity)])
            if isinstance(entity, api.SpecimenFromOrganism):
                self._set('document_id', MandatoryValueAccumulator, str(entity.document_id))
                self._set('biomaterial_id', MandatoryValueAccumulator, entity.biomaterial_id)
                self._set('disease', SetAccumulator, entity.diseases)
                self._set('organ', PriorityOptionalValueAccumulator, (0, entity.organ))
                self._set('organ_part', PriorityOptionalValueAccumulator, (0, entity.organ_part))
                self._set('preservation_method', OptionalValueAccumulator, entity.preservation_method)
                self._set('_type', MandatoryValueAccumulator, 'specimen')
            elif isinstance(entity, api.DonorOrganism):
                self._set('donor_document_id', SetAccumulator, str(entity.document_id))
                self._set('donor_biomaterial_id', SetAccumulator, entity.biomaterial_id)
                self._set('genus_species', SetAccumulator, entity.genus_species)
                self._set('organism_age', ListAccumulator, entity.organism_age)
                self._set('organism_age_unit', ListAccumulator, entity.organism_age_unit)
                if entity.organism_age_in_seconds:
                    self._set('min_organism_age_in_seconds', MinAccumulator, entity.organism_age_in_seconds.min)
                    self._set('max_organism_age_in_seconds', MaxAccumulator, entity.organism_age_in_seconds.max)
                self._set('biological_sex', SetAccumulator, entity.sex)
            elif isinstance(entity, api.Organoid):
                self._set('organ', PriorityOptionalValueAccumulator, (1, entity.model_organ))
                self._set('organ_part', PriorityOptionalValueAccumulator, (1, entity.model_organ_part))

    @property
    def merged_specimen(self) -> JSON:
        assert 'biomaterial_id' in self._accumulators
        assert 'document_id' in self._accumulators
        return {field: accumulator.get() for field, accumulator in self._accumulators.items()}


class CellSuspensionVisitor(BiomaterialVisitor):

    def _visit(self, entity: api.Biomaterial) -> None:

        if isinstance(entity, api.CellSuspension):
            self._set('document_id', MandatoryValueAccumulator, str(entity.document_id))
            self._set('total_estimated_cells', OptionalValueAccumulator, entity.total_estimated_cells)
        elif isinstance(entity, api.SpecimenFromOrganism):
            self._set('organ', PrioritySetAccumulator, (0, entity.organ))
            self._set('organ_part', PrioritySetAccumulator, (0, entity.organ_part))
        elif isinstance(entity, api.Organoid):
            self._set('organ', PrioritySetAccumulator, (1, entity.model_organ))
            self._set('organ_part', PrioritySetAccumulator, (1, entity.model_organ_part))

    @property
    def merged_cell_suspension(self) -> JSON:
        assert 'document_id' in self._accumulators
        return {field: accumulator.get() for field, accumulator in self._accumulators.items()}


class FileAggregator(GroupingAggregator):

    def _transform_entity(self, entity: JSON) -> JSON:
        return dict(size=((entity['uuid'], entity['version']), entity['size']),
                    file_format=entity['file_format'],
                    count=((entity['uuid'], entity['version']), 1))

    def _group_keys(self, entity) -> Iterable[Any]:
        return [entity['file_format']]

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        if field == 'file_format':
            return SetAccumulator()
        elif field in ('size', 'count'):
            return DistinctAccumulator(SumAccumulator(0))
        else:
            return None


class SpecimenAggregator(SimpleAggregator):

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        return SetAccumulator(max_size=100)


class CellSuspensionAggregator(GroupingAggregator):

    def _group_keys(self, entity) -> Iterable[Any]:
        return entity['organ']

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        if field == 'total_estimated_cells':
            return SumAccumulator(0)
        elif field == 'document_id':
            return None
        else:
            return SetAccumulator(max_size=100)


class ProjectAggregator(SimpleAggregator):

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        if field == 'document_id':
            return ListAccumulator(max_size=100)
        elif field in ('project_description',
                       'contact_names',
                       'contributors',
                       'publication_titles',
                       'publications'):
            return None
        else:
            return SetAccumulator(max_size=100)


class ProtocolAggregator(SimpleAggregator):
    def _get_accumulator(self, field) -> Optional[Accumulator]:
        if field == 'document_id':
            return None
        else:
            return SetAccumulator()


class Transformer(AggregatingTransformer, metaclass=ABCMeta):

    def get_aggregator(self, entity_type):
        if entity_type == 'files':
            return FileAggregator()
        elif entity_type == 'specimens':
            return SpecimenAggregator()
        elif entity_type == 'cell_suspensions':
            return CellSuspensionAggregator()
        elif entity_type == 'projects':
            return ProjectAggregator()
        elif entity_type == 'protocols':
            return ProtocolAggregator()
        else:
            return super().get_aggregator(entity_type)

    def _get_project(self, bundle) -> api.Project:
        project, *additional_projects = bundle.projects.values()
        reject(additional_projects, "Azul can currently only handle a single project per bundle")
        assert isinstance(project, api.Project)
        return project

    def _contribution(self, bundle, contents, entity):
        entity_reference = EntityReference(entity_type=self.entity_type(),
                                           entity_id=str(entity.document_id))
        # noinspection PyArgumentList
        # https://youtrack.jetbrains.com/issue/PY-28506
        return Contribution(entity=entity_reference,
                            version=None,
                            contents=contents,
                            bundle_uuid=str(bundle.uuid),
                            bundle_version=bundle.version)


class FileTransformer(Transformer):

    def entity_type(self) -> str:
        return 'files'

    def transform(self,
                  uuid: str,
                  version: str,
                  manifest: List[JSON],
                  metadata_files: Mapping[str, JSON]
                  ) -> Iterable[Document]:
        bundle = api.Bundle(uuid=uuid,
                            version=version,
                            manifest=manifest,
                            metadata_files=metadata_files)
        project = self._get_project(bundle)
        for file in bundle.files.values():
            if file.file_format == 'unknown' and '.zarr!' in file.manifest_entry.name:
                # FIXME: Remove once https://github.com/HumanCellAtlas/metadata-schema/issues/579 is resolved
                #
                continue
            visitor = TransformerVisitor()
            file.accept(visitor)
            file.ancestors(visitor)
            contents = dict(specimens=[_specimen_dict(s, visitor.visited_entities) for s in visitor.specimens.values()],
                            cell_suspensions=[_cell_suspension_dict(cs) for cs in visitor.cell_suspensions.values()],
                            files=[_file_dict(file)],
                            protocols=[_protocol_dict(pl) for pl in visitor.protocols.values()],
                            projects=[_project_dict(project)])
            yield self._contribution(bundle, contents, file)


class SpecimenTransformer(Transformer):

    def entity_type(self) -> str:
        return 'specimens'

    def transform(self,
                  uuid: str,
                  version: str,
                  manifest: List[JSON],
                  metadata_files: Mapping[str, JSON]
                  ) -> Sequence[Document]:
        bundle = api.Bundle(uuid=uuid,
                            version=version,
                            manifest=manifest,
                            metadata_files=metadata_files)
        project = self._get_project(bundle)
        for specimen in bundle.specimens:
            visitor = TransformerVisitor()
            specimen.accept(visitor)
            specimen.ancestors(visitor)
            contents = dict(specimens=[_specimen_dict(specimen, visitor.visited_entities)],
                            cell_suspensions=[_cell_suspension_dict(cs) for cs in visitor.cell_suspensions.values()],
                            files=[_file_dict(f) for f in visitor.files.values()],
                            protocols=[_protocol_dict(pl) for pl in visitor.protocols.values()],
                            projects=[_project_dict(project)])
            yield self._contribution(bundle, contents, specimen)


class ProjectTransformer(Transformer):

    def entity_type(self) -> str:
        return 'projects'

    def transform(self,
                  uuid: str,
                  version: str,
                  manifest: List[JSON],
                  metadata_files: Mapping[str, JSON]
                  ) -> Sequence[Document]:
        bundle = api.Bundle(uuid=uuid,
                            version=version,
                            manifest=manifest,
                            metadata_files=metadata_files)
        # Project entities are not explicitly linked in the graph. The mere presence of project metadata in a bundle
        # indicates that all other entities in that bundle belong to that project. Because of that we can't rely on a
        # visitor to collect the related entities but have to enumerate the explicitly:
        #
        visitor = TransformerVisitor()
        for specimen in bundle.specimens:
            specimen.accept(visitor)
            specimen.ancestors(visitor)
        for file in bundle.files.values():
            file.accept(visitor)
            file.ancestors(visitor)
        project = self._get_project(bundle)

        contents = dict(specimens=[_specimen_dict(s, visitor.visited_entities) for s in visitor.specimens.values()],
                        cell_suspensions=[_cell_suspension_dict(cs) for cs in visitor.cell_suspensions.values()],
                        files=[_file_dict(f) for f in visitor.files.values()],
                        protocols=[_protocol_dict(pl) for pl in visitor.protocols.values()],
                        projects=[_project_dict(project)])
        yield self._contribution(bundle, contents, project)
