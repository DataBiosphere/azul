import jmespath


class FileExtractor:
    @staticmethod
    def v1_2(metadata_file: dict) -> dict:
        # jmespath within the file objects
        _format = "content.file_core.file_format || `null`"
        lane = "content.lane_index || `null`"
        read = "content.read_index || `null`"
        hca_id = "hca_ingest.document_id || `null`"
        files = {}
        for _file in metadata_file["files"]:
            temp = {
                _file["content"]["file_core"]["file_name"]: {
                    "format": jmespath.search(_format, _file),
                    "lane": jmespath.search(lane, _file),
                    "read": jmespath.search(read, _file),
                    "hca_id": jmespath.search(hca_id, _file),
                    "_type": "files"
                }
            }
            files.update(temp)
        return files

    @staticmethod
    def v1_1(metadata_file: dict) -> dict:
        # jmespath within the file objects
        _format = "content.file_core.file_format || `null`"
        lane = "content.lane_index || `null`"
        read = "content.read_index || `null`"
        hca_id = "hca_ingest.document_id || `null`"
        files = {}
        for _file in metadata_file["files"]:
            temp = {
                _file["content"]["file_core"]["file_name"]: {
                    "format": jmespath.search(_format, _file),
                    "lane": jmespath.search(lane, _file),
                    "read": jmespath.search(read, _file),
                    "hca_id": jmespath.search(hca_id, _file),
                    "_type": "files"
                }
            }
            files.update(temp)
        return files

    @staticmethod
    def v1_0(metadata_file: dict) -> dict:
        # jmespath within the file objects
        _format = "content.file_core.file_format || `null`"
        lane = "content.lane_index || `null`"
        read = "content.read_index || `null`"
        hca_id = "hca_ingest.document_id || `null`"
        files = {}
        for _file in metadata_file["files"]:
            temp = {
                _file["content"]["file_core"]["file_name"]: {
                    "format": jmespath.search(_format, _file),
                    "lane": jmespath.search(lane, _file),
                    "read": jmespath.search(read, _file),
                    "hca_id": jmespath.search(hca_id, _file),
                    "_type": "files"
                }
            }
            files.update(temp)
        return files


class BiomaterialExtractor:
    @staticmethod
    def v5_2(metadata_file: dict) -> dict:
        # Jmespath within the file objects
        biomaterial_id = "content.biomaterial_core.biomaterial_id || `null`"
        species = "content.genus_species[*].text || `null`"
        organ = "content.organ.text || `null`"
        organ_part = "content.organ_part.text || `null`"
        age = "content.organism_age || `null`"
        age_unit = "content.organism_age_unit.text || `null`"
        sex = "content.biological_sex || `null`"
        disease = "content.disease[*].text || `null`"
        storage_method = "content.preservation_storage.storage_method" \
                         " || `null`"
        source = "content.describedBy || `null`"
        total_cells = "content.total_estimated_cells || `null`"
        parent = "content.biomaterial_core.has_input_biomaterial || `null`"
        hca_id = "hca_ingest.document_id || `null`"
        biomaterial = {
            "biomaterial_id": jmespath.search(biomaterial_id, metadata_file),
            "species": jmespath.search(species, metadata_file),
            "organ": jmespath.search(organ, metadata_file),
            "organ_part": jmespath.search(organ_part, metadata_file),
            "age": jmespath.search(age, metadata_file),
            "age_unit": jmespath.search(age_unit, metadata_file),
            "sex": jmespath.search(sex, metadata_file),
            "disease": jmespath.search(disease, metadata_file),
            "storage_method": jmespath.search(storage_method, metadata_file),
            "source": jmespath.search(source,
                                      metadata_file).rpartition('/')[2],
            "total_cells": jmespath.search(total_cells, metadata_file),
            "parent": jmespath.search(parent, metadata_file),
            "hca_id": jmespath.search(hca_id, metadata_file),
            "_type": "specimens"
        }
        return biomaterial

    @staticmethod
    def v5_1(metadata_file: dict) -> dict:
        # Jmespath within the file objects
        biomaterial_id = "content.biomaterial_core.biomaterial_id || `null`"
        species = "content.genus_species[*].text || `null`"
        organ = "content.organ.text || `null`"
        organ_part = "content.organ_part.text || `null`"
        age = "content.organism_age || `null`"
        age_unit = "content.organism_age_unit.text || `null`"
        sex = "content.biological_sex || `null`"
        disease = "content.disease[*].text || `null`"
        storage_method = "content.preservation_storage.storage_method" \
                         " || `null`"
        source = "content.describedBy || `null`"
        total_cells = "content.total_estimated_cells || `null`"
        parent = "content.biomaterial_core.has_input_biomaterial || `null`"
        hca_id = "hca_ingest.document_id || `null`"
        biomaterial = {
            "biomaterial_id": jmespath.search(biomaterial_id, metadata_file),
            "species": jmespath.search(species, metadata_file),
            "organ": jmespath.search(organ, metadata_file),
            "organ_part": jmespath.search(organ_part, metadata_file),
            "age": jmespath.search(age, metadata_file),
            "age_unit": jmespath.search(age_unit, metadata_file),
            "sex": jmespath.search(sex, metadata_file),
            "disease": jmespath.search(disease, metadata_file),
            "storage_method": jmespath.search(storage_method, metadata_file),
            "source": jmespath.search(source,
                                      metadata_file).rpartition('/')[2],
            "total_cells": jmespath.search(total_cells, metadata_file),
            "parent": jmespath.search(parent, metadata_file),
            "hca_id": jmespath.search(hca_id, metadata_file),
            "_type": "specimens"
        }
        return biomaterial

    @staticmethod
    def v5_0(metadata_file: dict) -> dict:
        # Jmespath within the file objects
        biomaterial_id = "content.biomaterial_core.biomaterial_id || `null`"
        species = "content.genus_species[*].text || `null`"
        organ = "content.organ.text || `null`"
        organ_part = "content.organ_part.text || `null`"
        age = "content.organism_age || `null`"
        age_unit = "content.organism_age_unit.text || `null`"
        sex = "content.biological_sex || `null`"
        disease = "content.disease[*].text || `null`"
        storage_method = "content.preservation_storage.storage_method" \
                         " || `null`"
        source = "content.describedBy || `null`"
        total_cells = "content.total_estimated_cells || `null`"
        parent = "content.biomaterial_core.has_input_biomaterial || `null`"
        hca_id = "hca_ingest.document_id || `null`"
        biomaterial = {
            "biomaterial_id": jmespath.search(biomaterial_id, metadata_file),
            "species": jmespath.search(species, metadata_file),
            "organ": jmespath.search(organ, metadata_file),
            "organ_part": jmespath.search(organ_part, metadata_file),
            "age": jmespath.search(age, metadata_file),
            "age_unit": jmespath.search(age_unit, metadata_file),
            "sex": jmespath.search(sex, metadata_file),
            "disease": jmespath.search(disease, metadata_file),
            "storage_method": jmespath.search(storage_method, metadata_file),
            "source": jmespath.search(source,
                                      metadata_file).rpartition('/')[2],
            "total_cells": jmespath.search(total_cells, metadata_file),
            "parent": jmespath.search(parent, metadata_file),
            "hca_id": jmespath.search(hca_id, metadata_file),
            "_type": "specimens"
        }
        return biomaterial


class ProcessExtractor:
    @staticmethod
    def v5_2(metadata_file: dict) -> dict:
        process_id = "content.process_core.process_id || `null`"
        process_name = "content.process_core.process_name || `null`"
        instrument = "content.instrument_manufacturer_model.text || `null`"
        library_construction = "content.library_construction_approach " \
                               "|| `null`"
        hca_id = "hca_ingest.document_id || `null`"
        process = {
            "process_id": jmespath.search(process_id, metadata_file),
            "process_name": jmespath.search(process_name, metadata_file),
            "instrument": jmespath.search(instrument, metadata_file),
            "library_construction": jmespath.search(library_construction,
                                                    metadata_file),
            "hca_id": jmespath.search(hca_id, metadata_file),
            "_type": "processes"
        }
        return process

    @staticmethod
    def v5_1(metadata_file: dict) -> dict:
        process_id = "content.process_core.process_id || `null`"
        process_name = "content.process_core.process_name || `null`"
        instrument = "content.instrument_manufacturer_model.text || `null`"
        library_construction = "content.library_construction_approach " \
                               "|| `null`"
        hca_id = "hca_ingest.document_id || `null`"
        process = {
            "process_id": jmespath.search(process_id, metadata_file),
            "process_name": jmespath.search(process_name, metadata_file),
            "instrument": jmespath.search(instrument, metadata_file),
            "library_construction": jmespath.search(library_construction,
                                                    metadata_file),
            "hca_id": jmespath.search(hca_id, metadata_file),
            "_type": "processes"
        }
        return process

    @staticmethod
    def v5_0(metadata_file: dict) -> dict:
        process_id = "content.process_core.process_id || `null`"
        process_name = "content.process_core.process_name || `null`"
        instrument = "content.instrument_manufacturer_model.text || `null`"
        library_construction = "content.library_construction_approach " \
                               "|| `null`"
        hca_id = "hca_ingest.document_id || `null`"
        process = {
            "process_id": jmespath.search(process_id, metadata_file),
            "process_name": jmespath.search(process_name, metadata_file),
            "instrument": jmespath.search(instrument, metadata_file),
            "library_construction": jmespath.search(library_construction,
                                                    metadata_file),
            "hca_id": jmespath.search(hca_id, metadata_file),
            "_type": "processes"
        }
        return process


class ProtocolExtractor:
    @staticmethod
    def v5_2(metadata_file: dict) -> dict:
        protocol_id = "content.protocol_core.protocol_id || `null`"
        protocol = "content.protocol_core.protocol_name || `null`"
        hca_id = "hca_ingest.document_id || `null`"
        protocol = {
            "protocol_id": jmespath.search(protocol_id, metadata_file),
            "protocol": jmespath.search(protocol, metadata_file),
            "hca_id": jmespath.search(hca_id, metadata_file),
            "_type": "processes"
        }
        return protocol

    @staticmethod
    def v5_1(metadata_file: dict) -> dict:
        protocol_id = "content.protocol_core.protocol_id || `null`"
        protocol = "content.protocol_core.protocol_name || `null`"
        hca_id = "hca_ingest.document_id || `null`"
        protocol = {
            "protocol_id": jmespath.search(protocol_id, metadata_file),
            "protocol": jmespath.search(protocol, metadata_file),
            "hca_id": jmespath.search(hca_id, metadata_file),
            "_type": "processes"
        }
        return protocol

    @staticmethod
    def v5_0(metadata_file: dict) -> dict:
        protocol_id = "content.protocol_core.protocol_id || `null`"
        protocol = "content.protocol_core.protocol_name || `null`"
        hca_id = "hca_ingest.document_id || `null`"
        protocol = {
            "protocol_id": jmespath.search(protocol_id, metadata_file),
            "protocol": jmespath.search(protocol, metadata_file),
            "hca_id": jmespath.search(hca_id, metadata_file),
            "_type": "processes"
        }
        return protocol


class ProjectExtractor:
    @staticmethod
    def v5_2(metadata_file: dict) -> dict:
        project = "content.project_core.project_shortname || `null`"
        laboratory = "content.contributors[*].laboratory || `null`"
        hca_id = "hca_ingest.document_id || `null`"
        project = {
            "project": jmespath.search(project, metadata_file),
            "laboratory": jmespath.search(laboratory, metadata_file),
            "hca_id": jmespath.search(hca_id, metadata_file),
            "_type": "project"
        }
        return project

    @staticmethod
    def v5_1(metadata_file: dict) -> dict:
        project = "content.project_core.project_shortname || `null`"
        laboratory = "content.contributors[*].laboratory || `null`"
        hca_id = "hca_ingest.document_id || `null`"
        project = {
            "project": jmespath.search(project, metadata_file),
            "laboratory": jmespath.search(laboratory, metadata_file),
            "hca_id": jmespath.search(hca_id, metadata_file),
            "_type": "project"
        }
        return project

    @staticmethod
    def v5_0(metadata_file: dict) -> dict:
        project = "content.project_core.project_shortname || `null`"
        laboratory = "content.contributors[*].laboratory || `null`"
        hca_id = "hca_ingest.document_id || `null`"
        project = {
            "project": jmespath.search(project, metadata_file),
            "laboratory": jmespath.search(laboratory, metadata_file),
            "hca_id": jmespath.search(hca_id, metadata_file),
            "_type": "project"
        }
        return project
