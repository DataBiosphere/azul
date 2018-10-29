from abc import ABCMeta, abstractmethod
import logging
from typing import Any, List, Mapping, MutableMapping, Optional, Sequence, Set, Callable, Tuple, Iterable

from humancellatlas.data.metadata import api
from humancellatlas.data.metadata.helpers.json import as_json

from azul import reject
from azul.transformer import (Accumulator,
                              AggregatingTransformer,
                              Bundle,
                              ElasticSearchDocument,
                              OptionalValueAccumulator,
                              GroupingAggregator,
                              ListAccumulator,
                              MaxAccumulator,
                              MinAccumulator,
                              SumAccumulator,
                              MandatoryValueAccumulator,
                              SetAccumulator,
                              SimpleAggregator,
                              DistinctAccumulator)
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


def _specimen_dict(specimen: api.SpecimenFromOrganism) -> JSON:
    visitor = SpecimenVisitor()
    # Visit the specimen but don't descend. Cell suspensions are handled as separate entities.
    visitor.visit(specimen)
    specimen.ancestors(visitor)
    return visitor.merged_specimen


def _cell_suspension_dict(cell_suspension: api.CellSuspension) -> JSON:
    visitor = CellSuspensionVisitor()
    # Visit the cell suspension but don't descend. We're only interested in the parent specimen.
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


def _process_dict(process: api.Process, protocol: api.Protocol) -> JSON:
    return {
        'document_id': f"{process.document_id}.{protocol.document_id}",
        'process_id': process.process_id,
        'process_name': process.process_name,
        'protocol_id': protocol.protocol_id,
        'protocol_name': protocol.protocol_name,
        '_type': "process",
        **(
            {
                'library_construction_approach': protocol.library_construction_approach
            } if isinstance(protocol, api.LibraryPreparationProtocol) else {
                'instrument_manufacturer_model': protocol.instrument_manufacturer_model
            } if isinstance(protocol, api.SequencingProtocol) else {
                'library_construction_approach': process.library_construction_approach
            } if isinstance(process, api.LibraryPreparationProcess) else {
                'instrument_manufacturer_model': process.instrument_manufacturer_model
            } if isinstance(process, api.SequencingProcess) else {
            }
        )
    }


class TransformerVisitor(api.EntityVisitor):
    # Entities are tracked by ID to ensure uniqueness if an entity is visited twice while descending the entity DAG
    specimens: MutableMapping[api.UUID4, api.SpecimenFromOrganism]
    cell_suspensions: MutableMapping[api.UUID4, api.CellSuspension]
    processes: MutableMapping[Tuple[api.UUID4, api.UUID4], Tuple[api.Process, api.Protocol]]
    files: MutableMapping[api.UUID4, api.File]

    def __init__(self) -> None:
        self.specimens = {}
        self.cell_suspensions = {}
        self.processes = {}
        self.files = {}

    def visit(self, entity: api.Entity) -> None:
        if isinstance(entity, api.SpecimenFromOrganism):
            self.specimens[entity.document_id] = entity
        elif isinstance(entity, api.CellSuspension):
            self.cell_suspensions[entity.document_id] = entity
        elif isinstance(entity, api.Process):
            for protocol in entity.protocols.values():
                self.processes[entity.document_id, protocol.document_id] = entity, protocol
        elif isinstance(entity, api.File):
            if entity.file_format == 'unknown' and '.zarr!' in entity.manifest_entry.name:
                # FIXME: Remove once https://github.com/HumanCellAtlas/metadata-schema/issues/579 is resolved
                #
                return
            self.files[entity.document_id] = entity


class BiomaterialVisitor(api.EntityVisitor, metaclass=ABCMeta):

    def __init__(self) -> None:
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
        self._set('has_input_biomaterial', SetAccumulator, entity.has_input_biomaterial)
        self._set('_source', SetAccumulator, api.schema_names[type(entity)])
        if isinstance(entity, api.SpecimenFromOrganism):
            self._set('document_id', MandatoryValueAccumulator, str(entity.document_id))
            self._set('biomaterial_id', MandatoryValueAccumulator, entity.biomaterial_id)
            self._set('disease', SetAccumulator, entity.diseases)
            self._set('organ', OptionalValueAccumulator, entity.organ)
            self._set('organ_part', OptionalValueAccumulator, entity.organ_part)
            self._set('storage_method', OptionalValueAccumulator, entity.storage_method)
            self._set('_type', MandatoryValueAccumulator, 'specimen')
        elif isinstance(entity, api.DonorOrganism):
            self._set('donor_document_id', SetAccumulator, str(entity.document_id))
            self._set('donor_biomaterial_id', SetAccumulator, entity.biomaterial_id)
            self._set('genus_species', SetAccumulator, entity.genus_species)
            self._set('disease', SetAccumulator, entity.diseases)
            self._set('organism_age', ListAccumulator, entity.organism_age)
            self._set('organism_age_unit', ListAccumulator, entity.organism_age_unit)
            if entity.organism_age_in_seconds:
                self._set('min_organism_age_in_seconds', MinAccumulator, entity.organism_age_in_seconds.min)
                self._set('max_organism_age_in_seconds', MaxAccumulator, entity.organism_age_in_seconds.max)
            self._set('biological_sex', SetAccumulator, entity.sex)

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
            self._set('organ', SetAccumulator, entity.organ)
            self._set('organ_part', SetAccumulator, entity.organ_part)

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


class ProcessAggregator(GroupingAggregator):

    def _group_keys(self, entity) -> Iterable[Any]:
        return [entity.get('library_construction_approach')]

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        if field == 'document_id':
            return None
        elif field in ('process_id', 'protocol_id'):
            return ListAccumulator(max_size=10)
        else:
            return SetAccumulator(max_size=10)


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
        elif entity_type == 'processes':
            return ProcessAggregator()
        else:
            return super().get_aggregator(entity_type)

    def _get_project(self, bundle) -> api.Project:
        project, *additional_projects = bundle.projects.values()
        reject(additional_projects, "Azul can currently only handle a single project per bundle")
        assert isinstance(project, api.Project)
        return project


class FileTransformer(Transformer):

    def entity_type(self) -> str:
        return 'files'

    def create_documents(self,
                         uuid: str,
                         version: str,
                         manifest: List[JSON],
                         metadata_files: Mapping[str, JSON]
                         ) -> Sequence[ElasticSearchDocument]:
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
            contents = dict(specimens=[_specimen_dict(s) for s in visitor.specimens.values()],
                            cell_suspensions=[_cell_suspension_dict(cs) for cs in visitor.cell_suspensions.values()],
                            files=[_file_dict(file)],
                            processes=[_process_dict(pr, pl) for pr, pl in visitor.processes.values()],
                            projects=[_project_dict(project)])
            es_document = ElasticSearchDocument(entity_type=self.entity_type(),
                                                entity_id=str(file.document_id),
                                                bundles=[Bundle(uuid=str(bundle.uuid),
                                                                version=bundle.version,
                                                                contents=contents)])
            yield es_document


class SpecimenTransformer(Transformer):

    def entity_type(self) -> str:
        return 'specimens'

    def create_documents(self,
                         uuid: str,
                         version: str,
                         manifest: List[JSON],
                         metadata_files: Mapping[str, JSON]
                         ) -> Sequence[ElasticSearchDocument]:
        bundle = api.Bundle(uuid=uuid,
                            version=version,
                            manifest=manifest,
                            metadata_files=metadata_files)
        project = self._get_project(bundle)
        for specimen in bundle.specimens:
            visitor = TransformerVisitor()
            specimen.accept(visitor)
            specimen.ancestors(visitor)
            contents = dict(specimens=[_specimen_dict(specimen)],
                            cell_suspensions=[_cell_suspension_dict(cs) for cs in visitor.cell_suspensions.values()],
                            files=[_file_dict(f) for f in visitor.files.values()],
                            processes=[_process_dict(pr, pl) for pr, pl in visitor.processes.values()],
                            projects=[_project_dict(project)])
            es_document = ElasticSearchDocument(entity_type=self.entity_type(),
                                                entity_id=str(specimen.document_id),
                                                bundles=[Bundle(uuid=str(bundle.uuid),
                                                                version=bundle.version,
                                                                contents=contents)])
            yield es_document


class ProjectTransformer(Transformer):

    def entity_type(self) -> str:
        return 'projects'

    def create_documents(self,
                         uuid: str,
                         version: str,
                         manifest: List[JSON],
                         metadata_files: Mapping[str, JSON]
                         ) -> Sequence[ElasticSearchDocument]:
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

        contents = dict(specimens=[_specimen_dict(s) for s in visitor.specimens.values()],
                        cell_suspensions=[_cell_suspension_dict(cs) for cs in visitor.cell_suspensions.values()],
                        files=[_file_dict(f) for f in visitor.files.values()],
                        processes=[_process_dict(pr, pl) for pr, pl in visitor.processes.values()],
                        projects=[_project_dict(project)])
        yield ElasticSearchDocument(entity_type=self.entity_type(),
                                    entity_id=str(project.document_id),
                                    bundles=[Bundle(uuid=str(bundle.uuid),
                                                    version=bundle.version,
                                                    contents=contents)])
