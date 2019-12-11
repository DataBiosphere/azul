# Adapted from prior work by Simon Jupp @ EMBL-EBI
#
# https://github.com/HumanCellAtlas/hca_bundles_to_csv/blob/b516a3a/hca_bundle_tools/file_metadata_to_csv.py
import os
import re
from typing import (
    Any,
    List,
    Mapping,
    MutableMapping,
    MutableSet,
    Optional,
    Tuple,
    Union,
)

from azul.types import (
    JSON,
)

Output = MutableMapping[Union[str, Tuple[str]], Union[str, MutableSet[str]]]


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
        self.output: List[JSON] = []
        # TODO temp until block filetype is needed
        self.default_blocked_file_ext = {'.csv', '.txt', '.pdf'}

    def _resolve_data_file_names(self,
                                 manifest: List[JSON],
                                 metadata_files: Mapping[str, JSON]) -> MutableMapping[str, Tuple[JSON, JSON]]:
        """
        Associate each metadata document describing a data file with the UUID
        of the data file it describes. The metadata only refers to data files by
        name and we need the manifest to resolve those into UUIDs.
        """
        manifest = {entry['name']: entry for entry in manifest if not entry['indexed']}
        file_info = {}
        for metadata_file in metadata_files.values():
            try:
                schema_type = metadata_file['schema_type']
            except KeyError:
                raise MissingSchemaTypeError()
            else:
                if schema_type == 'file':
                    file_name = self._deep_get(metadata_file, 'file_core', 'file_name')
                    if file_name is None:
                        raise MissingFileNameError()
                    else:
                        manifest_entry = manifest[file_name]
                        file_info[manifest_entry['uuid']] = manifest_entry, metadata_file
        return file_info

    def _deep_get(self, d: JSON, *path: str) -> Optional[JSON]:
        if d is not None and path:
            key, *path = path
            return self._deep_get(d.get(key), *path)
        else:
            return d

    @staticmethod
    def _set_value(output: Output, value: Any, *path: str) -> None:
        value = str(value)
        try:
            old_value = output[path]
        except KeyError:
            output[path] = value
        else:
            if isinstance(old_value, set):
                old_value.add(value)
            else:
                output[path] = {old_value, value}

    def _flatten(self, output: Output, obj: JSON, *path: str) -> None:
        for key, value in obj.items():
            if key not in self.ignored_fields:
                new_path = *path, key
                if isinstance(value, dict):
                    self._flatten(output, obj[key], *new_path)
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            self._flatten(output, item, *new_path)
                        else:
                            self._set_value(output, item, *new_path)
                else:
                    self._set_value(output, value, *new_path)

    @classmethod
    def _get_schema_name(cls, obj: JSON):
        try:
            described_by = obj['describedBy']
        except KeyError:
            raise MissingDescribedByError()
        else:
            return described_by.rsplit('/', 1)[-1]

    def _handle_zarray_members(self, obj, anchor):
        """
        Returns True, if and only if the the given document describes a zarray
        member that should be ignored. If the given file describes a zarray
        member that represents the entire zarray store, the file_name property
        of the document is changed to the path to the zarray file and False is
        returned. For all other files, this method just returns False.
        """
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

    def add_bundle(self,
                   bundle_uuid: str,
                   bundle_version: str,
                   manifest: List[JSON],
                   metadata_files: Mapping[str, JSON]) -> None:

        file_info = self._resolve_data_file_names(manifest, metadata_files)

        for file_manifest, metadata_file in file_info.values():
            output = {
                'bundle_uuid': bundle_uuid,
                'bundle_version': bundle_version,
                'file_uuid': file_manifest['uuid'],
                'file_version': file_manifest['version'],
                'file_sha256': file_manifest['sha256'],
                'file_size': file_manifest['size'],
                'file_name': self._deep_get(metadata_file, 'file_core', 'file_name'),
                'file_format': self._deep_get(metadata_file, 'file_core', 'file_format'),
            }

            file_name, extension = os.path.splitext(output['file_name'])
            if extension in self.default_blocked_file_ext:
                continue

            if any(self._handle_zarray_members(output, anchor) for anchor in ('.zarr/', '.zarr!')):
                continue

            if self.format_filter and output['file_format'] not in self.format_filter:
                continue

            schema_name = self._get_schema_name(metadata_file)
            self._flatten(output, metadata_file, schema_name)

            for other_metadata_file in metadata_files.values():
                if other_metadata_file['schema_type'] not in ('file', 'link_bundle'):
                    schema_name = self._get_schema_name(other_metadata_file)
                    self._flatten(output, other_metadata_file, schema_name)

            self.output.append(self._output_to_json(output))

    def _output_to_json(self, output: Output) -> JSON:
        """
        Convert output to JSON by joining tuple keys with '.' and set values with '||'.
        """
        return {
            ('.'.join(k) if isinstance(k, Tuple) else k): ('||'.join(sorted(v)) if isinstance(v, set) else v)
            for k, v in output.items()
        }

    def dump(self) -> List[JSON]:
        return self.output


class Error(Exception):
    msg = None

    def __init__(self, *args: object) -> None:
        super().__init__(self.msg, *args)


class MissingSchemaTypeError(Error):
    """
    Prove that the traceback includes the message from `msg`

    >>> raise MissingSchemaTypeError()
    Traceback (most recent call last):
    ...
    azul.project.hca.metadata_generator.MissingSchemaTypeError: Metadata document lacks `schema_type` property
    """
    msg = 'Metadata document lacks `schema_type` property'


class MissingDescribedByError(Error):
    msg = 'Metadata document lacks a `describedBy` property'


class EmptyBundleError(Error):
    msg = 'Bundle contains no data files'


class MissingFileNameError(Error):
    msg = 'Metadata document for data file lacks `file_core.file_name` property'
