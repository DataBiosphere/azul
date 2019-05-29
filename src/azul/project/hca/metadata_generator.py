# Adapted from https://github.com/HumanCellAtlas/\
# hca_bundles_to_csv/blob/b516a3a4de96ea3e97a698e7a603faec48ae97ec/hca_bundle_tools/file_metadata_to_csv.py
__author__ = "simonjupp"
__license__ = "Apache 2.0"
__date__ = "15/02/2019"

from typing import List, Any
from more_itertools import one
from azul.types import JSON
import re


class MetadataGenerator:
    def __init__(self, order=None, ignore=None, format_filter=None):
        self.all_objects_by_project_id = {}
        self.all_keys = []
        self.uuid4hex = re.compile('^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', re.I)

        self.default_order = order if order else [
            "path",
            "^\\*\\.file_core\\.file_name",
            "^\\*\\.file_core\\.file_format",
            "^sequence_file.*",
            "^analysis_file.*",
            "^donor_organism.*",
            "^specimen_from_organism.*",
            "^cell_suspension.*",
            "^.*protocol.*",
            "^project.",
            "^analysis_process.*",
            "^process.*",
            "^bundle_.*",
            "^\\*\\.provenance\\.update_date"
        ]

        self.default_ignore = ignore if ignore else [
            "describedBy",
            "schema_type",
            "submission_date",
            "update_date",
            "biomaterial_id",
            "process_id",
            "contributors",
            "publications",
            "protocol_id",
            "project_description",
            "file_format",
            "file_name"
        ]

        self.default_format_filter = format_filter
        # TODO temp until block filetype is needed
        self.default_blocked_file_ext = {'csv', 'txt', 'pdf'}

    def _get_file_uuids_from_objects(self, list_of_metadata_objects: List[JSON]) -> JSON:
        file_uuids = {}
        for _object in list_of_metadata_objects:
            if "schema_type" not in _object:
                raise MissingSchemaTypeError("JSON objects must declare a schema type")

            if _object["schema_type"] == "file" and self._deep_get(_object, ["provenance", "document_id"]):
                file_uuid = self._deep_get(_object, ["provenance", "document_id"])
                file_uuids[file_uuid] = _object

        if not file_uuids:
            raise MissingFileTypeError("Bundle contains no data files")

        return file_uuids

    def _deep_get(self, d: JSON, keys: List):
        if not keys or d is None:
            return d
        return self._deep_get(d.get(keys[0]), keys[1:])

    @staticmethod
    def _set_value(master: JSON, key: str, value: Any) -> None:

        if key not in master:
            master[key] = str(value)
        else:
            existing_values = master[key].split("||")
            existing_values.append(str(value))
            uniq = sorted(list(set(existing_values)))
            master[key] = "||".join(uniq)

    def _flatten(self, master: JSON, obj: JSON, parent: str) -> None:
        for key, value in obj.items():
            if key in self.default_ignore:
                continue

            newkey = parent + "." + key
            if isinstance(value, dict):
                self._flatten(master, obj[key], newkey)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        self._flatten(master, item, newkey)
                    else:
                        self._set_value(master, newkey, item)
            else:
                self._set_value(master, newkey, value)

    @staticmethod
    def _get_schema_name_from_object(_object: JSON):
        if "describedBy" in _object:
            return _object["describedBy"].rsplit('/', 1)[-1]
        raise MissingDescribedByError("found a metadata without a describedBy property")

    def add_bundle(self,
                   bundle_uuid: str,
                   bundle_version: str,
                   metadata_files: List[JSON]) -> None:

        file_uuids = self._get_file_uuids_from_objects(metadata_files)

        for file, content in file_uuids.items():
            obj = {
                'bundle_uuid': bundle_uuid,
                'bundle_version': bundle_version,
                '*.provenance.update_date': self._deep_get(content, ["provenance", "update_date"]),
                "*.file_core.file_name": self._deep_get(content, ["file_core", "file_name"]),
                "*.file_core.file_format": self._deep_get(content, ["file_core", "file_format"])
            }

            file_segments = obj["*.file_core.file_name"].split('.')

            if len(file_segments) > 1 and file_segments[-1] in self.default_blocked_file_ext:
                continue

            def handle_zarray(anchor):
                file_name = obj['*.file_core.file_name']
                try:
                    i = file_name.index(anchor)
                except ValueError:
                    return False
                else:
                    i += len(anchor) - 1
                    dir_name, file_name = file_name[0:i], file_name[i + 1:]
                    if file_name == '.zattrs':
                        obj['*.file_core.file_name'] = dir_name + '/'
                        return False
                return True

            if handle_zarray('.zarr/') or handle_zarray('.zarr!'):
                continue

            if not (obj["*.file_core.file_name"] or obj["*.file_core.file_format"]):
                raise MissingFileNameError("expecting file_core.file_name")

            if self.default_format_filter and obj["*.file_core.file_format"] not in self.default_format_filter:
                continue

            schema_name = self._get_schema_name_from_object(content)
            self._flatten(obj, content, schema_name)

            project_uuid = None
            for metadata in metadata_files:

                # ignore files
                if metadata["schema_type"] == "file" or metadata["schema_type"] == "link_bundle":
                    continue
                elif metadata["schema_type"] == 'project':
                    project_uuid = metadata['provenance']['document_id']

                schema_name = self._get_schema_name_from_object(metadata)
                self._flatten(obj, metadata, schema_name)

            self.all_keys.extend(obj.keys())
            self.all_keys = list(set(self.all_keys))
            assert project_uuid is not None
            self.all_objects_by_project_id.setdefault(project_uuid, []).append(obj)

    def dump(self) -> List[JSON]:
        """
        :return: A list of metadata row entries in key, value format
        """
        return one(list(self.all_objects_by_project_id.values()))


class Error(Exception):
    pass


class MissingSchemaTypeError(Error):
    pass


class MissingDescribedByError(Error):
    pass


class MissingFileTypeError(Error):
    pass


class MissingFileNameError(Error):
    pass
