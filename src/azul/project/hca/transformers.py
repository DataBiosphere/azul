import logging
from collections import defaultdict
from typing import Any, List, MutableMapping, Sequence

from azul.dss_bundle import DSSBundle
from azul.transformer import Document, ElasticSearchDocument, Transformer

from azul.project.hca import metadata_api as api


log = logging.getLogger(__name__)


def _project_dict(bundle: api.Bundle) -> dict:
    project = bundle.project
    project = dict(project_shortname=project.project_shortname,
                   laboratory=list(project.laboratory_names),
                   document_id=project.document_id,
                   _type="project")
    return project


def _specimen_dict(
        biomaterials: MutableMapping[api.UUID4, api.Biomaterial]
) -> List[MutableMapping[str, Any]]:
    specimen_list = []
    for specimen in (s for s in biomaterials.values() if isinstance(s, api.SpecimenFromOrganism)):
        bio_visitor = BiomaterialVisitor()
        specimen.accept(bio_visitor)
        specimen.ancestors(bio_visitor)
        merged_specimen = defaultdict(list)
        for b in bio_visitor.biomaterial_lineage:
            for key, value in b.items():
                if isinstance(value, list) or isinstance(value, set):
                    merged_specimen[key] += value
                else:
                    merged_specimen[key].extend([value])
        # Make unique
        for key, value in merged_specimen.items():
            if isinstance(value, list):
                merged_specimen[key] = list(set(value))
        merged_specimen['biomaterial_id'] = specimen.biomaterial_id
        specimen_list.append(merged_specimen)
    return specimen_list


class TransformerVisitor(api.EntityVisitor):
    specimens: MutableMapping[api.UUID4, api.SpecimenFromOrganism]
    processes: MutableMapping[str, Any]  # Merges process with protocol
    files: MutableMapping[api.UUID4, Any]  # Merges manifest + file metadata

    @staticmethod
    def _merge_process_protocol(pc: api.Process, pl: api.Protocol) -> MutableMapping[str, Any]:
        d = dict(documet_id=[pc.document_id, pl.document_id],
                 process_id=pc.process_id,
                 process_name=pc.process_name,
                 protocol_id=pl.protocol_id,
                 protocol_name=pl.protocol_name,
                 _type="process")
        if isinstance(pc, api.LibraryPreparationProcess):
            d["library_construction_approach"] = pc.library_construction_approach
        elif isinstance(pc, api.SequencingProcess):
            d["instrument_manufacturer_model"] = pc.instrument_manufacturer_model
        return d

    @staticmethod
    def _file_dict(f: api.File) -> MutableMapping[str, Any]:
        d = {
            "content-type": f.manifest_entry.content_type,
            "crc32c": f.manifest_entry.crc32c,
            "indexed": f.manifest_entry.indexed,
            "name": f.manifest_entry.name,
            "s3_etag": f.manifest_entry.s3_etag,
            "sha1": f.manifest_entry.sha1,
            "sha256": f.manifest_entry.sha256,
            "size": f.manifest_entry.size,
            "uuid": f.manifest_entry.uuid,
            "version": f.manifest_entry.version,
            "document_id": f.document_id,
            "file_format": f.file_format,
            "_type": "file"
        }
        if isinstance(f, api.SequenceFile):
            d = {
                **d,
                "read_index": f.read_index,
                "lane_index": f.lane_index
            }
        return d

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
            # noinspection PyProtectedMember
            b = dict(document_id=entity.document_id,
                     has_input_biomaterial=entity.has_input_biomaterial,
                     _source=entity._source)
            if isinstance(entity, api.SpecimenFromOrganism):
                b["biomaterial_id"] = entity.biomaterial_id
                b["disease"] = list(entity.disease)
                b["organ"] = entity.organ
                b["organ_part"] = entity.organ_part
                b["storage_method"] = entity.storage_method
                b["_type"] = "specimen"
            elif isinstance(entity, api.DonorOrganism):
                b["donor_biomaterial_id"] = entity.biomaterial_id
                b["genus_species"] = entity.genus_species
                b["disease"] = list(entity.disease)
                b["organism_age"] = entity.organism_age
                b["organism_age_unit"] = entity.organism_age_unit
                age_range = entity.organism_age_in_seconds
                b["max_organism_age_in_seconds"] = age_range.max if age_range else None
                b["min_organism_age_in_seconds"] = age_range.min if age_range else None
                b["biological_sex"] = entity.biological_sex
            elif isinstance(entity, api.CellSuspension):
                b["total_estimated_cells"] = entity.total_estimated_cells
            # As more facets are required by the browser, handle each biomateiral as appropriate
            self.biomaterial_lineage.append(b)


class FileTransformer(Transformer):
    def __init__(self):
        super().__init__()

    @property
    def entity_name(self):
        return "files"

    def create_documents(self, dss_bundle: DSSBundle) -> Sequence[ElasticSearchDocument]:
        bundle = api.Bundle(dss_bundle)
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
        return "specimens"

    def create_documents(self, dss_bundle: DSSBundle) -> Sequence[ElasticSearchDocument]:
        bundle = api.Bundle(dss_bundle)
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


class ProjectTransformer(Transformer):
    def create_documents(self, dss_bundle: DSSBundle) -> Sequence[ElasticSearchDocument]:
        pass
