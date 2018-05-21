from abc import ABC, abstractmethod
from collections import defaultdict
from functools import partial, reduce
from itertools import chain, filterfalse, tee
import jmespath
import project.hca.extractors as extractors
import re
from typing import Mapping, Sequence, Iterable

# TODO: consider moving the current "create_specimens", etc. to "extract_speciments" and then make a new method called "assign_specimens", etc


class Document:
    def __init__(self, entity_id: str, bundle_uuid: str, bundle_version: str, content: dict) -> None:
        self.entity_id = entity_id
        self.bundle_uuid = bundle_uuid
        self.bundle_version = bundle_version
        self.content = content

    @property
    def document(self) -> dict:
        constructed_dict = {
            "entity_id": self.entity_id,
            "bundles": [
                {
                    "uuid": self.bundle_uuid,
                    "version": self.bundle_version,
                    "contents": self.content
                }
            ]
        }
        return constructed_dict


class ElasticSearchDocument:
    def __init__(self, elastic_search_id: str, content: Document) -> None:
        self.elastic_search_id = elastic_search_id
        self.content = content

    @property
    def document_id(self) -> str:
        return self.elastic_search_id

    @property
    def document_content(self) -> dict:
        return self.content.document


class Transformer(ABC):
    def __init__(self):
        pass

    @staticmethod
    def partition(predicate, iterable):
        """
        Use a predicate to partition entries into false entries and
        true entries
        """
        t1, t2 = tee(iterable)
        return filterfalse(predicate, t1), filter(predicate, t2)

    @classmethod
    def get_version(cls, metadata_json: dict) -> str:
        schema_url = metadata_json["describedBy"]
        version_match = re.search(r'\d\.\d\.\d', schema_url)
        version = version_match.group()
        version = version.replace('.', '_')
        return version

    @abstractmethod
    def _create_files(
            self,
            files_dictionary: dict,
            metadata_dictionary: dict=None) -> Sequence[dict]:
        pass

    @abstractmethod
    def _create_specimens(self, metadata_dictionary: dict) -> Sequence[dict]:
        pass

    @abstractmethod
    def _create_project(self, metadata_dictionary: dict) -> Sequence[dict]:
        pass

    @abstractmethod
    def create_documents(
            self,
            metadata_files: Mapping[str, dict],
            data_files: Mapping[str, dict],
            bundle_uuid: str,
            bundle_version: str,
    ) -> Sequence[ElasticSearchDocument]:
        pass


class FileTransformer(Transformer):
    def __init__(self):
        super().__init__()

    def _create_files(
            self,
            files_dictionary: Mapping[str, dict],
            metadata_dictionary: dict=None
    ) -> Sequence[dict]:
        # Handle the file.json if it's present
        if metadata_dictionary is not None:
            metadata_version = self.get_version(metadata_dictionary)
            file_extractor = getattr(
                extractors, "FileExtractor.v{}".format(metadata_version))
            fields_from_metadata = file_extractor(metadata_dictionary)
            for _file, contents in fields_from_metadata.items():
                files_dictionary[_file].update(contents)
        return [_file for _file in files_dictionary.values()]

    def _create_specimens(self, metadata_dictionary: dict) -> Sequence[dict]:
        metadata_version = self.get_version(metadata_dictionary)
        biomaterial_extractor = getattr(
            extractors, "BiomaterialExtractor.v{}".format(metadata_version))
        biomaterials = [biomaterial_extractor(biomaterial) for biomaterial
                        in metadata_dictionary["biomaterials"]]
        # Now mangle the samples into separate entities as appropriate.
        # Separate biomaterials into roots and not_roots
        not_roots, roots = self.partition(
            lambda x: "specimen_from_organism" in x["source"], biomaterials)

        def find_descendants(nodes: Iterable[dict],
                             parent_id: str=None) -> Iterable[dict]:
            # TODO: Add code to break under some cyclic condition
                for child in filter(lambda x: parent_id in x["parent"], nodes):
                    yield from find_descendants(nodes, child["biomaterial_id"])
                    yield child

        def find_ancestors(nodes: Iterable[dict],
                           parent_id: str=None) -> Iterable[dict]:
            for parent in filter(lambda x: parent_id in x["biomaterial_id"],
                                 nodes):
                if "parent" in parent:
                    yield from find_ancestors(nodes, parent["parent"])
                yield parent

        # Add ancestors and descendants fields to each sample
        samples_list = []
        for root in roots:
            ancestors = find_ancestors(not_roots, root["parent"])
            descendants = find_descendants(not_roots, root["parent"])
            root_id = root["biomaterial_id"]
            merged_sample = defaultdict(list)
            for node in chain(ancestors, descendants, root):
                for key, value in node.items():
                    merged_sample[key] += value
            merged_sample["biomaterial_id"] = root_id
            samples_list.append(merged_sample)
        return samples_list

    def _create_processes(self, metadata_dictionary: dict) -> Sequence[dict]:
        metadata_version = self.get_version(metadata_dictionary)
        process_extractor = getattr(
            extractors, "ProcessExtractor.v{}".format(metadata_version))
        processes = [process_extractor(process)
                     for process in metadata_dictionary["processes"]]
        return processes

    def _create_protocols(self, metadata_dictionary: dict) -> Sequence[dict]:
        metadata_version = self.get_version(metadata_dictionary)
        protocol_extractor = getattr(
            extractors, "ProtocolExtractor.v{}".format(metadata_version))
        protocols = [protocol_extractor(protocol)
                     for protocol in metadata_dictionary["protocols"]]
        return protocols

    def _create_project(self, metadata_dictionary: dict) -> dict:
        metadata_version = self.get_version(metadata_dictionary)
        project_extractor = getattr(
            extractors, "ProjectExtractor.v{}".format(metadata_version))
        project = project_extractor(metadata_dictionary)
        return project

    def create_documents(
            self,
            metadata_files: Mapping[str, dict],
            data_files: Mapping[str, dict],
            bundle_uuid: str,
            bundle_version: str,
    ) -> Sequence[ElasticSearchDocument]:
        # Get basic units
        project = self._create_project(metadata_files['project.json'])
        specimens = self._create_specimens(metadata_files['biomaterial.json'])
        processes = self._create_processes(metadata_files['process.json'])
        protocol = self._create_protocols(metadata_files["protocol.json"])
        files = self._create_files(data_files,
                                   metadata_dictionary=metadata_files[
                                       "file.json"])
        # all_units = {x["hca_id"]: x for x in chain(specimens, processes, protocol, files)}
        all_units = {}
        for unit in chain(specimens, processes, protocol, files):
            if isinstance(unit["hca_id"], list):
                reduce(partial(lambda x, y, z: x.update((z, y)), all_units, unit), unit["hca_id"])
            else:
                # It's a string
                all_units[unit["hca_id"]] = unit

        def get_relatives(root_id: str, links_array: list) -> Iterable[str]:
            """
            Get the ancestors and descendants of the root_id
            """
            for parent in jmespath.search("[?destination_id=='{}'].source_id".format(root_id), links_array):
                yield from get_relatives(parent, links_array)
                yield parent

            for child in jmespath.search("[?source_id=='{}'].destination_id".format(root_id), links_array):
                yield from get_relatives(child, links_array)
                yield child

        # Get the links
        links = metadata_files['links.json']
        # Begin merging.
        for _file in files:
            contents = defaultdict[list]
            entity_id = _file['hca_id']
            entity_type = _file.pop("_type")
            relatives = get_relatives(entity_id, links['links'])
            for relative in relatives:
                unit_type = all_units[relative].pop("_type")
                if unit_type == entity_type:
                    continue
                contents[unit_type] += relative
            # Add missing project field and append the current entity
            contents["project"] = project
            contents[entity_type] += _file
            document_contents = Document(entity_id,
                                         bundle_uuid,
                                         bundle_version,
                                         contents)
            es_document = ElasticSearchDocument(entity_id, document_contents)
            yield es_document


class SampleTransformer:
    pass


class ProjectTransformer:
    pass
