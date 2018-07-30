from collections import defaultdict
import logging
from typing import Any, List, Mapping, MutableMapping, Sequence

from humancellatlas.data import metadata as api

from azul.transformer import Document, ElasticSearchDocument, Transformer
from azul.types import JSON

log = logging.getLogger(__name__)


def _project_dict(bundle: api.Bundle) -> dict:
    project = bundle.project
    return {
        'project_shortname': project.project_shortname,
        'laboratory': list(project.laboratory_names),
        'document_id': project.document_id,
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
            merged_specimen = {k: list(v) for k, v in merged_specimen.items()}
            merged_specimen['biomaterial_id'] = specimen.biomaterial_id
            specimen_list.append(merged_specimen)
    return specimen_list


class TransformerVisitor(api.EntityVisitor):
    specimens: MutableMapping[api.UUID4, api.SpecimenFromOrganism]
    processes: MutableMapping[str, Any]  # Merges process with protocol
    files: MutableMapping[api.UUID4, Any]  # Merges manifest + file metadata

    def _merge_process_protocol(self, pc: api.Process, pl: api.Protocol) -> MutableMapping[str, Any]:
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

    def _file_dict(self, f: api.File) -> MutableMapping[str, Any]:
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
            self.specimens[entity.document_id] = entity
        elif isinstance(entity, api.Process):
            for pl in entity.protocols.values():
                pl_pr_id = f"{str(pl.document_id)}-{str(entity.document_id)}"
                self.processes[pl_pr_id] = self._merge_process_protocol(entity, pl)
        elif isinstance(entity, api.File):
            self.files[entity.document_id] = self._file_dict(entity)


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
                            'max_organism_age_in_seconds': entity.organism_age_in_seconds.max,
                            'min_organism_age_in_seconds': entity.organism_age_in_seconds.min,
                            'biological_sex': entity.biological_sex
                        } if isinstance(entity, api.DonorOrganism) else {
                            'total_estimated_cells': entity.total_estimated_cells
                        } if isinstance(entity, api.CellSuspension) else {
                        }
                    )
                }
            )


class FileTransformer(Transformer):
    def __init__(self):
        super().__init__()

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
            entity_id = str(file.document_id)
            document_contents = Document(entity_id,
                                         str(bundle.uuid),
                                         bundle.version,
                                         contents)
            es_document = ElasticSearchDocument(entity_id, document_contents, self.entity_name)
            yield es_document


class SpecimenTransformer(Transformer):
    def __init__(self):
        super().__init__()

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
        for specimen in bundle.specimens:
            visitor = TransformerVisitor()
            # Visit the relatives of file
            specimen.accept(visitor)  # Visit descendants
            specimen.ancestors(visitor)
            # Assign the contents to the ES doc
            entity_id = str(specimen.document_id)
            contents = dict(specimens=_specimen_dict(visitor.specimens),
                            files=list(visitor.files.values()),
                            processes=list(visitor.processes.values()),
                            project=_project_dict(bundle))
            document_contents = Document(entity_id,
                                         str(bundle.uuid),
                                         bundle.version,
                                         contents)
            es_document = ElasticSearchDocument(entity_id, document_contents, self.entity_name)
            yield es_document
