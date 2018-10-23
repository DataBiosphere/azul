from abc import ABCMeta
import logging
from typing import Any, List, Mapping, MutableMapping, Optional, Sequence, Set, Type

from humancellatlas.data.metadata import api
from humancellatlas.data.metadata.helpers.json import as_json

from azul import reject
from azul.transformer import (Accumulator,
                              AggregatingTransformer,
                              Bundle,
                              ElasticSearchDocument,
                              FirstValueAccumulator,
                              GroupingAggregator,
                              ListAccumulator,
                              MaxAccumulator,
                              MinAccumulator,
                              SumAccumulator,
                              OneValueAccumulator,
                              SetAccumulator,
                              SimpleAggregator, DistinctValueCountAccumulator)
from azul.types import JSON

log = logging.getLogger(__name__)


def _project_dict(bundle: api.Bundle) -> dict:
    project, *additional_projects = bundle.projects.values()
    reject(additional_projects, "Azul can currently only handle a single project per bundle")
    assert isinstance(project, api.Project)

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


def _specimen_dict(specimen: api.SpecimenFromOrganism) -> MutableMapping[str, Any]:
    visitor = BiomaterialVisitor()
    specimen.accept(visitor)
    specimen.ancestors(visitor)
    return visitor.merged_specimen


def _file_dict(f: api.File) -> MutableMapping[str, Any]:
    return {
        'content-type': f.manifest_entry.content_type,
        'indexed': f.manifest_entry.indexed,
        'name': f.manifest_entry.name,
        'sha1': f.manifest_entry.sha1,
        'size': f.manifest_entry.size,
        'uuid': f.manifest_entry.uuid,
        'version': f.manifest_entry.version,
        'document_id': str(f.document_id),
        'file_format': f.file_format,
        '_type': 'file',
        **(
            {
                'read_index': f.read_index,
                'lane_index': f.lane_index
            } if isinstance(f, api.SequenceFile) else {
            }
        )
    }


class TransformerVisitor(api.EntityVisitor):
    specimens: MutableMapping[api.UUID4, api.SpecimenFromOrganism]
    processes: MutableMapping[str, Any]  # Merges process with protocol
    files: MutableMapping[api.UUID4, Any]  # Merges manifest + file metadata

    def _merge_process_protocol(self, pc: api.Process, pl: api.Protocol) -> MutableMapping[str, Any]:
        return {
            'document_id': f"{pc.document_id}.{pl.document_id}",
            'process_id': pc.process_id,
            'process_name': pc.process_name,
            'protocol_id': pl.protocol_id,
            'protocol_name': pl.protocol_name,
            '_type': "process",
            **(
                {
                    'library_construction_approach': pl.library_construction_approach
                } if isinstance(pl, api.LibraryPreparationProtocol) else {
                    'instrument_manufacturer_model': pl.instrument_manufacturer_model
                } if isinstance(pl, api.SequencingProtocol) else {
                    'library_construction_approach': pc.library_construction_approach
                } if isinstance(pc, api.LibraryPreparationProcess) else {
                    'instrument_manufacturer_model': pc.instrument_manufacturer_model
                } if isinstance(pc, api.SequencingProcess) else {
                }
            )
        }

    def __init__(self) -> None:
        self.specimens = {}
        self.processes = {}
        self.files = {}

    def visit(self, entity: api.Entity) -> None:
        # Track entities by ID to ensure uniqueness if an entity is visited twice while descending the entity DAG
        if isinstance(entity, api.SpecimenFromOrganism):
            self.specimens[entity.document_id] = entity
        elif isinstance(entity, api.Process):
            for pl in entity.protocols.values():
                process_protocol = self._merge_process_protocol(entity, pl)
                self.processes[process_protocol['document_id']] = process_protocol
        elif isinstance(entity, api.File):
            if entity.file_format == 'unknown' and '.zarr!' in entity.manifest_entry.name:
                # FIXME: Remove once https://github.com/HumanCellAtlas/metadata-schema/issues/579 is resolved
                #
                return
            self.files[entity.document_id] = _file_dict(entity)


class BiomaterialVisitor(api.EntityVisitor):

    def __init__(self) -> None:
        self._accumulators: MutableMapping[str, Accumulator] = {}
        self._biomaterials: MutableMapping[api.UUID4, api.Biomaterial] = dict()

    def _set(self, field: str, accumulator_type: Type[Accumulator], value: Any):
        try:
            accumulator = self._accumulators[field]
        except KeyError:
            self._accumulators[field] = accumulator = accumulator_type()
        accumulator.accumulate(value)

    def visit(self, entity: api.Entity) -> None:
        if isinstance(entity, api.Biomaterial) and entity.document_id not in self._biomaterials:
            self._biomaterials[entity.document_id] = entity
            self._set('has_input_biomaterial', SetAccumulator, entity.has_input_biomaterial)
            self._set('_source', SetAccumulator, api.schema_names[type(entity)])
            if isinstance(entity, api.CellSuspension):
                self._set('total_estimated_cells', SumAccumulator, entity.total_estimated_cells)
            elif isinstance(entity, api.SpecimenFromOrganism):
                self._set('document_id', OneValueAccumulator, str(entity.document_id))
                self._set('biomaterial_id', OneValueAccumulator, entity.biomaterial_id)
                self._set('disease', SetAccumulator, entity.disease)
                self._set('organ', FirstValueAccumulator, entity.organ)
                self._set('organ_part', FirstValueAccumulator, entity.organ_part)
                self._set('storage_method', FirstValueAccumulator, entity.storage_method)
                self._set('_type', OneValueAccumulator, 'specimen')
            elif isinstance(entity, api.DonorOrganism):
                self._set('donor_document_id', SetAccumulator, str(entity.document_id))
                self._set('donor_biomaterial_id', SetAccumulator, entity.biomaterial_id)
                self._set('genus_species', SetAccumulator, entity.genus_species)
                self._set('disease', SetAccumulator, entity.disease)
                self._set('organism_age', ListAccumulator, entity.organism_age)
                self._set('organism_age_unit', ListAccumulator, entity.organism_age_unit)
                if entity.organism_age_in_seconds:
                    self._set('min_organism_age_in_seconds', MinAccumulator, entity.organism_age_in_seconds.min)
                    self._set('max_organism_age_in_seconds', MaxAccumulator, entity.organism_age_in_seconds.max)
                self._set('biological_sex', SetAccumulator, entity.biological_sex)

    @property
    def merged_specimen(self) -> MutableMapping[str, Any]:
        assert 'biomaterial_id' in self._accumulators
        assert 'document_id' in self._accumulators
        return {field: accumulator.close() for field, accumulator in self._accumulators.items()}


class FileAggregator(GroupingAggregator):

    def _transform_entity(self, entity: JSON) -> JSON:
        return dict(size=entity['size'],
                    file_format=entity['file_format'],
                    count=(entity['uuid'], entity['version']))

    def _group_key(self, entity):
        return entity['file_format']

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        if field == 'size':
            return SumAccumulator()
        elif field == 'count':
            return DistinctValueCountAccumulator()
        else:
            return None


class SpecimenAggregator(GroupingAggregator):

    def _group_key(self, entity):
        return entity['organ']

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        if field == 'total_estimated_cells':
            return SumAccumulator()
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

    def _group_key(self, entity) -> Any:
        return entity.get('library_construction_approach')

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
        elif entity_type == 'projects':
            return ProjectAggregator()
        elif entity_type == 'processes':
            return ProcessAggregator()
        else:
            return super().get_aggregator(entity_type)


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
        for file in bundle.files.values():
            if file.file_format == 'unknown' and '.zarr!' in file.manifest_entry.name:
                # FIXME: Remove once https://github.com/HumanCellAtlas/metadata-schema/issues/579 is resolved
                #
                continue
            visitor = TransformerVisitor()
            file.accept(visitor)
            file.ancestors(visitor)
            contents = dict(specimens=[_specimen_dict(s) for s in visitor.specimens.values()],
                            files=[_file_dict(file)],
                            processes=list(visitor.processes.values()),
                            projects=[_project_dict(bundle)])
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
        for specimen in bundle.specimens:
            visitor = TransformerVisitor()
            specimen.accept(visitor)
            specimen.ancestors(visitor)
            contents = dict(specimens=[_specimen_dict(specimen)],
                            files=list(visitor.files.values()),
                            processes=list(visitor.processes.values()),
                            projects=[_project_dict(bundle)])
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
        bundle_uuid = str(bundle.uuid)
        simplified_project = _project_dict(bundle)
        data_visitor = TransformerVisitor()
        for specimen in bundle.specimens:
            specimen.accept(data_visitor)
            specimen.ancestors(data_visitor)
        for file in bundle.files.values():
            file.accept(data_visitor)
            file.ancestors(data_visitor)
        for project in bundle.projects.values():
            contents = dict(specimens=[_specimen_dict(s) for s in data_visitor.specimens.values()],
                            files=list(data_visitor.files.values()),
                            processes=list(data_visitor.processes.values()),
                            projects=[simplified_project])
            yield ElasticSearchDocument(entity_type=self.entity_type(),
                                        entity_id=str(project.document_id),
                                        bundles=[Bundle(uuid=bundle_uuid,
                                                        version=bundle.version,
                                                        contents=contents)])
