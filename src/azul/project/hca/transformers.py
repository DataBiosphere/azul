
from collections import defaultdict
from itertools import chain, tee
import jmespath
import operator
from typing import Mapping, Sequence, Iterable

# pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'chalicelib'))  # noqa
# sys.path.insert(0, pkg_root)  # noqa

import project.hca.extractors as extractors
from utils.transformer import Transformer, ElasticSearchDocument, Document

# TODO: consider moving the current "create_specimens", etc. to "extract_speciments" and then make a new method called "assign_specimens", etc
# TODO: Refactor the creation of the subunits.


class FileTransformer(Transformer):
    def __init__(self):
        super().__init__()

    @property
    def entity_name(self):
        return "files"

    def _create_files(
            self,
            files_dictionary: Mapping[str, dict],
            metadata_dictionary: dict=None
    ) -> Sequence[dict]:
        # Handle the file.json if it's present
        if metadata_dictionary is not None:
            metadata_version = self.get_version(metadata_dictionary)
            file_extractor = operator.attrgetter(
                "FileExtractor.v{}".format(metadata_version))(extractors)
            fields_from_metadata = file_extractor(metadata_dictionary)
            for _file, contents in fields_from_metadata.items():
                files_dictionary[_file].update(contents)
        return [_file for _file in files_dictionary.values()]

    def _create_specimens(self, metadata_dictionary: dict) -> Sequence[dict]:
        metadata_version = self.get_version(metadata_dictionary)
        biomaterial_extractor = operator.attrgetter(
            "BiomaterialExtractor.v{}".format(metadata_version))(extractors)
        biomaterials = [biomaterial_extractor(biomaterial) for biomaterial
                        in metadata_dictionary["biomaterials"]]
        # Now mangle the biomaterials into separate entities as appropriate.
        # Separate biomaterials into roots and not_roots
        not_roots, roots = self.partition(
            lambda x: "specimen_from_organism" in x["source"], biomaterials)

        def find_descendants(nodes: Iterable[dict],
                             parent_id: str) -> Iterable[dict]:
            # TODO: Add code to break under some cyclic condition
            for child in filter(lambda x: parent_id == x["parent"], nodes):
                yield from find_descendants(nodes, child["biomaterial_id"])
                yield child

        def find_ancestors(nodes: Iterable[dict],
                           parent_id: str) -> Iterable[dict]:
            for parent in filter(lambda x: parent_id == x["biomaterial_id"],
                                 nodes):
                if "parent" in parent and bool(parent["parent"]):
                    yield from find_ancestors(nodes, parent["parent"])
                yield parent

        # Add ancestors and descendants fields to each sample
        specimen_list = []
        for root in roots:
            not_roots, not_roots_1, not_roots_2 = tee(not_roots, 3)
            ancestors = find_ancestors(list(not_roots_1), root["parent"])
            descendants = find_descendants(list(not_roots_2), root["biomaterial_id"])
            root_id = root["biomaterial_id"]
            merged_sample = defaultdict(list)
            for node in chain(ancestors, descendants, [root]):
                for key, value in node.items():
                    if isinstance(value, list):
                        merged_sample[key] += value
                    else:
                        merged_sample[key].extend([value])
            merged_sample["biomaterial_id"] = root_id
            specimen_list.append(merged_sample)
        return specimen_list

    def _create_processes(self, metadata_dictionary: dict) -> Sequence[dict]:
        metadata_version = self.get_version(metadata_dictionary)
        process_extractor = operator.attrgetter(
            "ProcessExtractor.v{}".format(metadata_version))(extractors)
        processes = [process_extractor(process)
                     for process in metadata_dictionary["processes"]]
        return processes

    def _create_protocols(self, metadata_dictionary: dict) -> Sequence[dict]:
        metadata_version = self.get_version(metadata_dictionary)
        protocol_extractor = operator.attrgetter(
            "ProtocolExtractor.v{}".format(metadata_version))(extractors)
        protocols = [protocol_extractor(protocol)
                     for protocol in metadata_dictionary["protocols"]]
        return protocols

    def _create_project(self, metadata_dictionary: dict) -> dict:
        metadata_version = self.get_version(metadata_dictionary)
        project_extractor = operator.attrgetter(
            "ProjectExtractor.v{}".format(metadata_version))(extractors)
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
        protocols = self._create_protocols(metadata_files["protocol.json"])
        files = self._create_files(data_files,
                                   metadata_dictionary=metadata_files[
                                       "file.json"])
        all_units = {}
        # Create dictionary with each key being the unit's hca_id and the value
        # their contents
        for unit in chain(specimens, processes, protocols, files):
            if isinstance(unit["hca_id"], list):
                current_units = {u: unit for u in unit["hca_id"]}
                all_units.update(current_units)
            else:
                # It's a string
                all_units[unit["hca_id"]] = unit

        def get_parents(root_id: str, links_array: list) -> Iterable[str]:
            """
            Get the parents of the root_id
            """
            for _parent in (link['source_id'] for link in links_array if link['destination_id'] == root_id):
                yield from get_parents(_parent, links_array)
                yield _parent

        def get_children(root_id: str, links_array: list) -> Iterable[str]:
            """
            Get the children of the root_id
            """
            for child in (link['destination_id'] for link in links_array if link['source_id'] == root_id):
                yield from get_children(child, links_array)
                yield child

        # Get the links
        links = metadata_files['links.json']
        # Merge protocol into process
        for protocol in protocols:
            protocol_copy = protocol.copy()
            protocol_id = protocol_copy.pop("hca_id")
            edges = filter(lambda x: x["destination_id"] == protocol_id,
                           links['links'])
            parent_processes = {_process["source_id"] for _process in edges}
            for parent in parent_processes:
                all_units[parent] = {**all_units[parent], **protocol_copy}
                if isinstance(all_units[parent]['hca_id'], list):
                    all_units[parent]['hca_id'].append(protocol_id)
                else:
                    all_units[parent]['hca_id'] = [all_units[parent]['hca_id'],
                                                   protocol_id]
        # Begin merging.
        for _file in files:
            contents = defaultdict(list)
            entity_id = _file['hca_id']
            entity_type = _file["_type"]
            # Get relatives and create a set
            relatives = set(chain(get_parents(entity_id, links['links']),
                                  get_children(entity_id, links['links'])))
            # Keep track of the sets that get added
            added = set()
            for relative in relatives:
                relative_type = all_units[relative]["_type"]
                if isinstance(relative_type, str):
                    relative_type = [relative_type]
                unit_type = relative_type[0]
                # If we encounter an ancestor that's of the same type, skip it
                if unit_type == entity_type:
                    continue

                if relative in added:
                    continue
                if isinstance(unit_type, str):
                    contents[unit_type] += [all_units[relative]]
                else:
                    contents[unit_type[0]] += [all_units[relative]]
                hca_id = all_units[relative]["hca_id"]
                hca_id = [hca_id] if isinstance(hca_id, str) else hca_id
                added.update(hca_id)
            # Add missing project field and append the current entity
            contents["project"] = project
            contents[entity_type] += [_file]
            document_contents = Document(entity_id,
                                         bundle_uuid,
                                         bundle_version,
                                         contents)
            es_document = ElasticSearchDocument(entity_id, document_contents, entity_type)
            yield es_document


class SpecimenTransformer(Transformer):
    def __init__(self):
        super().__init__()

    @property
    def entity_name(self):
        return "specimens"

    def _create_files(
            self,
            files_dictionary: Mapping[str, dict],
            metadata_dictionary: dict=None
    ) -> Sequence[dict]:
        # Handle the file.json if it's present
        if metadata_dictionary is not None:
            metadata_version = self.get_version(metadata_dictionary)
            file_extractor = operator.attrgetter(
                "FileExtractor.v{}".format(metadata_version))(extractors)
            fields_from_metadata = file_extractor(metadata_dictionary)
            for _file, contents in fields_from_metadata.items():
                files_dictionary[_file].update(contents)
        return [_file for _file in files_dictionary.values()]

    def _create_specimens(self, metadata_dictionary: dict) -> Sequence[dict]:
        metadata_version = self.get_version(metadata_dictionary)
        biomaterial_extractor = operator.attrgetter(
            "BiomaterialExtractor.v{}".format(metadata_version))(extractors)
        biomaterials = [biomaterial_extractor(biomaterial) for biomaterial
                        in metadata_dictionary["biomaterials"]]
        # Now mangle the biomaterials into separate entities as appropriate.
        # Separate biomaterials into roots and not_roots
        not_roots, roots = self.partition(
            lambda x: "specimen_from_organism" in x["source"], biomaterials)

        def find_descendants(nodes: Iterable[dict],
                             parent_id: str) -> Iterable[dict]:
            # TODO: Add code to break under some cyclic condition
            for child in filter(lambda x: parent_id == x["parent"], nodes):
                yield from find_descendants(nodes, child["biomaterial_id"])
                yield child

        def find_ancestors(nodes: Iterable[dict],
                           parent_id: str) -> Iterable[dict]:
            for parent in filter(lambda x: parent_id == x["biomaterial_id"],
                                 nodes):
                if "parent" in parent and bool(parent["parent"]):
                    yield from find_ancestors(nodes, parent["parent"])
                yield parent

        # Add ancestors and descendants fields to each sample
        specimen_list = []
        for root in roots:
            not_roots, not_roots_1, not_roots_2 = tee(not_roots, 3)
            ancestors = find_ancestors(list(not_roots_1), root["parent"])
            descendants = find_descendants(list(not_roots_2), root["biomaterial_id"])
            root_id = root["biomaterial_id"]
            merged_sample = defaultdict(list)
            for node in chain(ancestors, descendants, [root]):
                for key, value in node.items():
                    if isinstance(value, list):
                        merged_sample[key] += value
                    else:
                        merged_sample[key].extend([value])
            merged_sample["biomaterial_id"] = root_id
            merged_sample["biomaterial_document_id"] = root["hca_id"]
            specimen_list.append(merged_sample)
        return specimen_list

    def _create_processes(self, metadata_dictionary: dict) -> Sequence[dict]:
        metadata_version = self.get_version(metadata_dictionary)
        process_extractor = operator.attrgetter(
            "ProcessExtractor.v{}".format(metadata_version))(extractors)
        processes = [process_extractor(process)
                     for process in metadata_dictionary["processes"]]
        return processes

    def _create_protocols(self, metadata_dictionary: dict) -> Sequence[dict]:
        metadata_version = self.get_version(metadata_dictionary)
        protocol_extractor = operator.attrgetter(
            "ProtocolExtractor.v{}".format(metadata_version))(extractors)
        protocols = [protocol_extractor(protocol)
                     for protocol in metadata_dictionary["protocols"]]
        return protocols

    def _create_project(self, metadata_dictionary: dict) -> dict:
        metadata_version = self.get_version(metadata_dictionary)
        project_extractor = operator.attrgetter(
            "ProjectExtractor.v{}".format(metadata_version))(extractors)
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
        protocols = self._create_protocols(metadata_files["protocol.json"])
        files = self._create_files(data_files,
                                   metadata_dictionary=metadata_files[
                                       "file.json"])
        all_units = {}
        # Create dictionary with each key being the unit's hca_id and the value
        # their contents
        for unit in chain(specimens, processes, protocols, files):
            if isinstance(unit["hca_id"], list):
                current_units = {u: unit for u in unit["hca_id"]}
                all_units.update(current_units)
            else:
                # It's a string
                all_units[unit["hca_id"]] = unit

        def get_parents(root_id: str, links_array: list) -> Iterable[str]:
            """
            Get the parents of the root_id
            """
            for _parent in jmespath.search("[?destination_id=='{}'].source_id".format(root_id), links_array):
                yield from get_parents(_parent, links_array)
                yield _parent

        def get_children(root_id: str, links_array: list) -> Iterable[str]:
            """
            Get the children of the root_id
            """
            for child in jmespath.search("[?source_id=='{}'].destination_id".format(root_id), links_array):
                yield from get_children(child, links_array)
                yield child

        # Get the links
        links = metadata_files['links.json']
        # Merge protocol into process
        for protocol in protocols:
            protocol_copy = protocol.copy()
            protocol_id = protocol_copy.pop("hca_id")
            edges = filter(lambda x: x["destination_id"] == protocol_id,
                           links['links'])
            parent_processes = {_process["source_id"] for _process in edges}
            for parent in parent_processes:
                all_units[parent] = {**all_units[parent], **protocol_copy}
                if isinstance(all_units[parent]['hca_id'], list):
                    all_units[parent]['hca_id'].append(protocol_id)
                else:
                    all_units[parent]['hca_id'] = [all_units[parent]['hca_id'],
                                                   protocol_id]
        # Begin merging.
        for _specimen in specimens:
            contents = defaultdict(list)
            entity_id = _specimen['biomaterial_document_id']
            entity_type = _specimen["_type"][0]
            # Get relatives and create a set
            relatives = set(chain(get_parents(entity_id, links['links']),
                                  get_children(entity_id, links['links'])))
            # Keep track of the sets that get added
            added = set()
            for relative in relatives:
                relative_type = all_units[relative]["_type"]
                if isinstance(relative_type, str):
                    relative_type = [relative_type]
                unit_type = relative_type[0]
                # If we encounter an ancestor that's of the same type, skip it
                if unit_type == entity_type:
                    continue

                if relative in added:
                    continue
                if isinstance(unit_type, str):
                    contents[unit_type] += [all_units[relative]]
                else:
                    contents[unit_type[0]] += [all_units[relative]]
                hca_id = all_units[relative]["hca_id"]
                hca_id = [hca_id] if isinstance(hca_id, str) else hca_id
                added.update(hca_id)
            # Add missing project field and append the current entity
            contents["project"] = project
            contents[entity_type] += [_specimen]
            document_contents = Document(entity_id,
                                         bundle_uuid,
                                         bundle_version,
                                         contents)
            es_document = ElasticSearchDocument(entity_id,
                                                document_contents,
                                                entity_type)
            yield es_document


class ProjectTransformer(Transformer):
    pass
