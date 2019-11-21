# Adapted from prior work by Simon Jupp @ EMBL-EBI
#
# https://github.com/HumanCellAtlas/hca_bundles_to_csv/blob/b516a3a/hca_bundle_tools/file_metadata_to_csv.py

import re
from typing import (
    Any,
    List,
)

from more_itertools import one

from azul.types import (
    JSON,
    MutableJSON,
)


class MetadataGenerator:
    """
    Generates a more or less verbatim but unharmonized JSON representation of
    the metadata in a bundle.
    """

    column_order = [
        'path',
        '^\\*\\.file_core\\.file_name',
        '^\\*\\.file_core\\.file_format',
        '^sequence_file.*',
        '^analysis_file.*',
        '^donor_organism.*',
        '^specimen_from_organism.*',
        '^cell_suspension.*',
        '^.*protocol.*',
        '^project.',
        '^analysis_process.*',
        '^process.*',
        '^bundle_.*',
        '^file_.*'
    ]

    ignored_fields = [
        'describedBy',
        'schema_type',
        'submission_date',
        'update_date',
        'biomaterial_id',
        'process_id',
        'contributors',
        'publications',
        'protocol_id',
        'project_description',
        'file_format',
        'file_name'
    ]

    format_filter = None

    uuid4hex = re.compile('^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', re.I)

    def __init__(self):
        self.all_objects_by_project_id = {}
        self.all_keys = []
        # TODO temp until block filetype is needed
        self.default_blocked_file_ext = {'csv', 'txt', 'pdf'}

    def _get_file_uuids_from_objects(self, manifest: List[JSON], list_of_metadata_objects: List[JSON]) -> JSON:
        file_info = {}
        file_manifests = {file_manifest['name']: file_manifest
                          for file_manifest in manifest if not file_manifest['indexed']}

        for _object in list_of_metadata_objects:
            if 'schema_type' not in _object:
                raise MissingSchemaTypeError('JSON objects must declare a schema type')

            if _object['schema_type'] == 'file':
                file_name = self._deep_get(_object, ['file_core', 'file_name'])
                if file_name is None:
                    raise MissingFileNameError('expecting file_core.file_name')

                file_manifest = file_manifests[file_name]
                file_info[file_manifest['uuid']] = {'metadata': _object, 'manifest': file_manifest}

        if not file_info:
            raise MissingFileTypeError('Bundle contains no data files')

        return file_info

    def _deep_get(self, d: JSON, keys: List[str]):
        if not keys or d is None:
            return d
        return self._deep_get(d.get(keys[0]), keys[1:])

    @staticmethod
    def _set_value(master: MutableJSON, key: str, value: Any) -> None:

        if key not in master:
            master[key] = str(value)
        else:
            existing_values = master[key].split('||')
            existing_values.append(str(value))
            uniq = sorted(list(set(existing_values)))
            master[key] = '||'.join(uniq)

    def _flatten(self, master: MutableJSON, obj: JSON, parent: str) -> None:
        for key, value in obj.items():
            if key in self.ignored_fields:
                continue

            newkey = parent + '.' + key
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
    def _get_schema_name_from_object(obj: JSON):
        if 'describedBy' in obj:
            return obj['describedBy'].rsplit('/', 1)[-1]
        raise MissingDescribedByError('found a metadata without a describedBy property')

    def add_bundle(self,
                   bundle_uuid: str,
                   bundle_version: str,
                   manifest: List[JSON],
                   metadata_files: List[JSON]) -> None:

        file_info = self._get_file_uuids_from_objects(manifest, metadata_files)

        for content in file_info.values():
            file_metadata = content['metadata']
            file_manifest = content['manifest']
            obj = {
                'bundle_uuid': bundle_uuid,
                'bundle_version': bundle_version,
                'file_uuid': file_manifest['uuid'],
                'file_version': file_manifest['version'],
                'file_sha256': file_manifest['sha256'],
                'file_size': file_manifest['size'],
                'file_name': self._deep_get(file_metadata, ['file_core', 'file_name']),
                'file_format': self._deep_get(file_metadata, ['file_core', 'file_format']),
            }

            file_segments = obj['file_name'].split('.')

            if len(file_segments) > 1 and file_segments[-1] in self.default_blocked_file_ext:
                continue

            def handle_zarray(anchor):
                file_name = obj['file_name']
                try:
                    i = file_name.index(anchor)
                except ValueError:
                    return False
                else:
                    i += len(anchor) - 1
                    dir_name, file_name = file_name[0:i], file_name[i + 1:]
                    if file_name == '.zattrs':
                        obj['file_name'] = dir_name + '/'
                        return False
                return True

            if handle_zarray('.zarr/') or handle_zarray('.zarr!'):
                continue

            if self.format_filter and obj['file_format'] not in self.format_filter:
                continue

            schema_name = self._get_schema_name_from_object(file_metadata)
            self._flatten(obj, file_metadata, schema_name)

            project_uuid = None
            for file_metadata in metadata_files:

                # ignore files
                if file_metadata['schema_type'] == 'file' or file_metadata['schema_type'] == 'link_bundle':
                    continue
                elif file_metadata['schema_type'] == 'project':
                    project_uuid = file_metadata['provenance']['document_id']

                schema_name = self._get_schema_name_from_object(file_metadata)
                self._flatten(obj, file_metadata, schema_name)

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
