from collections import defaultdict
import logging
from typing import Any, List, Mapping, MutableMapping, Sequence, Set, Iterable

from humancellatlas.data import metadata as api
from humancellatlas.data.metadata import AgeRange

from azul import reject
from azul.transformer import ElasticSearchDocument, Transformer, Bundle
from azul.types import JSON

log = logging.getLogger(__name__)


def _project_dict(bundle: api.Bundle) -> dict:
    project: api.Project
    laboratories: Set[str]

    project, *additional_projects = bundle.projects.values()
    reject(additional_projects, "Azul can currently only handle a single project per bundle")

    laboratories = set()\

    # Extract the list of laboratories from contributors.
    # NOTE: This is for backward compatibility. The list of contact names and institutes
    #       can be extract from project.contributors.
    for contributor in project.contributors:
        if contributor.laboratory:
            laboratories.add(contributor.laboratory)

    return {
        'project_title': project.project_title,
        'project_shortname': project.project_short_name,
        'laboratory': sorted(laboratories),
        'contributors': sorted(project.contributors, key=lambda contributor: contributor.email.lower() if contributor.email else None),
        'document_id': project.document_id,
        'publications': sorted(project.publications),
        '_type': 'project'
    }


def _specimen_dict(biomaterials: Mapping[api.UUID4, api.Biomaterial]) -> List[Mapping[str, Any]]:
    specimen_list = []
    for specimen in biomaterials.values():
        if isinstance(specimen, api.SpecimenFromOrganism):
            bio_visitor = BiomaterialVisitor()
            specimen.accept(bio_visitor)
            specimen.ancestors(bio_visitor)
            merged_specimen = defaultdict(set)
            for b in bio_visitor.biomaterial_lineage:
                for key, value in b.items():
                    if isinstance(value, (list, set)):
                        merged_specimen[key].update(value)
                    else:
                        merged_specimen[key].add(value)
            merged_specimen = {k: list(sorted(v, key=lambda x: (x is not None, x))) for k, v in merged_specimen.items()}
            merged_specimen['biomaterial_id'] = specimen.biomaterial_id
            specimen_list.append(merged_specimen)
    return specimen_list


class TransformerVisitor(api.EntityVisitor):
    specimens: MutableMapping[api.UUID4, api.SpecimenFromOrganism]
    processes: MutableMapping[str, Any]  # Merges process with protocol
    files: MutableMapping[api.UUID4, Any]  # Merges manifest + file metadata

    @staticmethod
    def merge_process_protocol(pc: api.Process, pl: api.Protocol) -> MutableMapping[str, Any]:
        return {
            'document_id': (pc.document_id, pl.document_id),
            'process_id': pc.process_id,
            'process_name': pc.process_name,
            'protocol_id': pl.protocol_id,
            'protocol_name': pl.protocol_name,
            '_type': "process",
            **(
                {
                    'library_construction_approach': pc.library_construction_approach
                } if isinstance(pc, api.LibraryPreparationProcess) else {
                    'instrument_manufacturer_model': pc.instrument_manufacturer_model
                } if isinstance(pc, api.SequencingProcess) else {
                }
            )
        }

    @staticmethod
    def file_dict(f: api.File) -> MutableMapping[str, Any]:
        return {
            'content-type': f.manifest_entry.content_type,
            'crc32c': f.manifest_entry.crc32c,
            'indexed': f.manifest_entry.indexed,
            'name': f.manifest_entry.name,
            's3_etag': f.manifest_entry.s3_etag,
            'sha1': f.manifest_entry.sha1,
            'sha256': f.manifest_entry.sha256,
            'size': f.manifest_entry.size,
            'uuid': f.manifest_entry.uuid,
            'version': f.manifest_entry.version,
            'document_id': f.document_id,
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

    def __init__(self) -> None:
        self.specimens = {}
        self.processes = {}
        self.files = {}

    def visit(self, entity: api.Entity) -> None:
        if isinstance(entity, api.SpecimenFromOrganism):
            # This is to ensure that we have a unique set of specimens.
            self.specimens[entity.document_id] = entity
        elif isinstance(entity, api.Process):
            for pl in entity.protocols.values():
                pl_pr_id = f"{pl.document_id}-{entity.document_id}"
                # This is to ensure that we have a unique set of processes.
                self.processes[pl_pr_id] = TransformerVisitor.merge_process_protocol(entity, pl)
        elif isinstance(entity, api.File):
            # This is to ensure that we have a unique set of files.
            self.files[entity.document_id] = TransformerVisitor.file_dict(entity)


class BiomaterialVisitor(api.EntityVisitor):
    specimen: MutableMapping[str, Any]
    biomaterial_lineage: List[MutableMapping[str, Any]]

    def __init__(self) -> None:
        self.biomaterial_lineage = []

    def visit(self, entity: api.Entity) -> None:
        if isinstance(entity, api.Biomaterial):
            # As more facets are required by the browser, handle each biomaterial as appropriate
            self.biomaterial_lineage.append(
                {
                    'document_id': entity.document_id,
                    'has_input_biomaterial': entity.has_input_biomaterial,
                    '_source': api.schema_names[type(entity)],
                    **(
                        {
                            'biomaterial_id': entity.biomaterial_id,
                            'disease': list(entity.disease),
                            'organ': entity.organ,
                            'organ_part': entity.organ_part,
                            'storage_method': entity.storage_method,
                            '_type': "specimen",
                        } if isinstance(entity, api.SpecimenFromOrganism) else {
                            'donor_biomaterial_id': entity.biomaterial_id,
                            'genus_species': entity.genus_species,
                            'disease': list(entity.disease),
                            'organism_age': entity.organism_age,
                            'organism_age_unit': entity.organism_age_unit,
                            **self._age_range(entity),
                            'biological_sex': entity.biological_sex
                        } if isinstance(entity, api.DonorOrganism) else {
                            'total_estimated_cells': entity.total_estimated_cells
                        } if isinstance(entity, api.CellSuspension) else {
                        }
                    )
                }
            )

    def _age_range(self, entity: api.DonorOrganism):
        age = entity.organism_age_in_seconds or AgeRange.any
        return {
            'max_organism_age_in_seconds': age.max,
            'min_organism_age_in_seconds': age.min,
        }


class FileTransformer(Transformer):
    @property
    def entity_name(self):
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

        # Create ElasticSearch documents
        for file in bundle.files.values():
            visitor = TransformerVisitor()

            # Visit the relatives of file
            file.accept(visitor)  # Visit descendants
            file.ancestors(visitor)

            # Assign the contents to the ES doc
            contents = dict(specimens=_specimen_dict(visitor.specimens),
                            files=list(visitor.files.values()),
                            processes=list(visitor.processes.values()),
                            project=_project_dict(bundle))
            es_document = ElasticSearchDocument(entity_type=self.entity_name,
                                                entity_id=str(file.document_id),
                                                bundles=[Bundle(uuid=str(bundle.uuid),
                                                                version=bundle.version,
                                                                contents=contents)])
            yield es_document


class SpecimenTransformer(Transformer):
    @property
    def entity_name(self):
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

        # Create ElasticSearch documents
        for specimen in bundle.specimens:
            visitor = TransformerVisitor()

            # Visit the relatives of file
            specimen.accept(visitor)  # Visit descendants
            specimen.ancestors(visitor)

            # Assign the contents to the ES doc
            contents = dict(specimens=_specimen_dict(visitor.specimens),
                            files=list(visitor.files.values()),
                            processes=list(visitor.processes.values()),
                            project=_project_dict(bundle))
            es_document = ElasticSearchDocument(entity_type=self.entity_name,
                                                entity_id=str(specimen.document_id),
                                                bundles=[Bundle(uuid=str(bundle.uuid),
                                                                version=bundle.version,
                                                                contents=contents)])
            yield es_document


class ProjectTransformer(Transformer):
    @property
    def entity_name(self):
        return 'projects'

    def create_documents(self,
                         uuid: str,
                         version: str,
                         manifest: List[JSON],
                         metadata_files: Mapping[str, JSON]
                         ) -> Sequence[ElasticSearchDocument]:
        # Create a bundle object.
        bundle = api.Bundle(uuid=uuid,
                            version=version,
                            manifest=manifest,
                            metadata_files=metadata_files)

        bundle_uuid = str(bundle.uuid)
        simplified_project = _project_dict(bundle)

        # Gather information on specimens and files in the bundle with a separate visitor
        data_visitor = TransformerVisitor()

        for specimen in bundle.specimens:
            # Visit the relatives
            specimen.accept(data_visitor)  # Visit descendants
            specimen.ancestors(data_visitor)

        for file in bundle.files.values():
            # Visit the relatives
            file.accept(data_visitor)  # Visit descendants
            file.ancestors(data_visitor)

        # Create ElasticSearch documents
        for project in bundle.projects.values():
            contents = dict(specimens=_specimen_dict(data_visitor.specimens),
                            files=list(data_visitor.files.values()),
                            processes=list(data_visitor.processes.values()),
                            project=simplified_project)

            yield ElasticSearchDocument(entity_type=self.entity_name,
                                        entity_id=str(project.document_id),
                                        bundles=[Bundle(uuid=bundle_uuid,
                                                        version=bundle.version,
                                                        contents=contents)])
