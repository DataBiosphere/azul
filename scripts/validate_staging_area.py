"""
Runs a pre-check of a staging area to identify issues that might cause the
snapshot or indexing processes to fail.
"""
import argparse
import base64
import json
import logging
import sys
from typing import (
    List,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
)
from urllib import (
    parse,
)
import uuid

import google.cloud.storage as gcs
from jsonschema import (
    FormatChecker,
    ValidationError,
    validate,
)
from more_itertools import (
    one,
)
import requests

from azul import (
    cache,
    cached_property,
    reject,
    require,
)
from azul.logging import (
    configure_script_logging,
)
from azul.types import (
    JSON,
)

log = logging.getLogger(__name__)


class StagingAreaValidator:

    def main(self):
        self._run()
        exit_code = 0
        for file_name, e in self.file_errors.items():
            log.error('Error with file: %s', file_name, exc_info=e)
        for file_name in self.extra_files:
            log.warning('File is not part of a subgraph: %s', file_name)
        if self.file_errors:
            exit_code |= 1
            log.error('Encountered %i files with errors',
                      len(self.file_errors))
        return exit_code

    @classmethod
    def _parse_args(cls, argv):
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument('--staging-area', '-s',
                            required=True,
                            help='The Google Cloud Storage URL of the staging area. '
                                 'Syntax is gs://<bucket>[/<path>].')
        parser.add_argument('--no-json-validation', '-J',
                            action='store_false',
                            default=True,
                            dest='validate_json',
                            help='Do not validate JSON documents against their schema.')
        return parser.parse_args(argv)

    date_format = '%Y-%m-%dT%H:%M:%S.%fZ'

    def __init__(self, argv: List[str]) -> None:
        super().__init__()
        self.args = self._parse_args(argv)
        self.gcs = gcs.Client()
        # A mapping of data file name to metadata id
        self.names_to_id: MutableMapping[str, str] = {}
        # The status of each metadata file checked
        self.metadata_files: MutableMapping[str, JSON] = {}
        # A mapping of file name to validation error
        self.file_errors: MutableMapping[str, BaseException] = {}
        # Any files found that are not part of a subgraph link
        self.extra_files: Sequence[str] = []
        self.bucket, self.sa_path = self._parse_gcs_url(self.args.staging_area)

    @cached_property
    def validator(self):
        return SchemaValidator()

    def _parse_gcs_url(self, gcs_url: str) -> Tuple[gcs.Bucket, str]:
        """
        Parse a GCS URL into its Bucket and path components
        """
        split_url = parse.urlsplit(gcs_url)
        require(split_url.scheme == 'gs' and split_url.netloc,
                'Google Cloud Storage URL must be in gs://<bucket>[/<path>] format')
        reject(split_url.path.endswith('/'),
               'Google Cloud Storage URL must not end with a "/"')
        if split_url.path:
            path = split_url.path.lstrip('/') + '/'
        else:
            path = ''
        bucket = gcs.Bucket(self.gcs, split_url.netloc)
        return bucket, path

    def _run(self):
        self.file_errors.clear()
        self.validate_files('links')
        self.validate_files('metadata')
        self.validate_files('descriptors')
        self.validate_files('data')
        self.check_results()

    def validate_files(self, path: str) -> None:
        log.info(f'Checking files in {self.sa_path}{path}')
        validate_file_fn = getattr(self, f'validate_{path}_file')
        for blob in self.bucket.list_blobs(prefix=f'{self.sa_path}{path}'):
            try:
                validate_file_fn(blob)
            except KeyboardInterrupt:
                exit()
            except BaseException as e:
                log.error('File error: %s', blob.name)
                self.file_errors[blob.name] = e

    def download_blob_as_json(self, blob: gcs.Blob) -> Optional[JSON]:
        file_json = json.loads(blob.download_as_string())
        return file_json

    def validate_links_file(self, blob: gcs.Blob) -> None:
        # Expected syntax: links/{bundle_uuid}_{version}_{project_uuid}.json
        _, _, file_name = blob.name.rpartition('/')
        assert file_name.count('_') == 2
        assert file_name.endswith('.json')
        _, _, project_uuid = file_name[:-5].split('_')
        file_json = self.download_blob_as_json(blob)
        self.validate_file_json(file_json, blob.name)
        for link in file_json['links']:
            if link['link_type'] == 'process_link':
                self.add_metadata_file(link['process_id'], link['process_type'], project_uuid)
                for link_type in ('input', 'output', 'protocol'):
                    for file in link[f'{link_type}s']:
                        file_type = file[f'{link_type}_type']
                        file_id = file[f'{link_type}_id']
                        self.add_metadata_file(file_id, file_type, project_uuid)
            elif link['link_type'] == 'supplementary_file_link':
                assert link['entity']['entity_type'] == 'project', link['entity']
                assert link['entity']['entity_id'] == project_uuid, link['entity']
                for file in link['files']:
                    file_type = file['file_type']
                    file_id = file['file_id']
                    self.add_metadata_file(file_id, file_type, project_uuid)
        if project_uuid not in self.metadata_files:
            self.add_metadata_file(project_uuid, 'project', project_uuid)

    def add_metadata_file(self, file_id, file_type, project_uuid):
        try:
            self.metadata_files[file_id]['projects'].add(project_uuid)
        except KeyError:
            self.metadata_files[file_id] = {
                'name': None,
                'version': None,
                'file_type': file_type,
                'projects': {project_uuid},
                'found_metadata': False,
            }
        assert self.metadata_files[file_id]['file_type'] == file_type

    def validate_metadata_file(self, blob: gcs.Blob) -> None:
        # Expected syntax: metadata/{metadata_type}/{metadata_id}_{version}.json
        metadata_type, metadata_file = blob.name.split('/')[-2:]
        assert metadata_file.count('_') == 1
        assert metadata_file.endswith('.json')
        metadata_id, metadata_version = metadata_file.split('_')
        file_json = self.download_blob_as_json(blob)
        self.validate_file_json(file_json, blob.name)
        if provenance := file_json.get('provenance'):
            assert metadata_id == provenance['document_id']
        if metadata_file := self.metadata_files.get(metadata_id):
            metadata_file['name'] = blob.name
            metadata_file['version'] = metadata_version
            metadata_file['found_metadata'] = True
            if metadata_type.endswith('_file'):
                metadata_file['data_file_name'] = file_json['file_core']['file_name']
                metadata_file['found_descriptor'] = False
                metadata_file['found_data_file'] = False
            if metadata_type == 'supplementary_file' and file_json.get('provenance', {}).get('submitter_id'):
                try:
                    self.validate_file_description(file_json.get('file_description'))
                except BaseException as e:
                    self.file_errors[blob.name] = BaseException(e)
                    metadata_file['valid_stratification'] = False
                    log.error('Invalid file_description in %s.', blob.name)
                else:
                    metadata_file['valid_stratification'] = True
        else:
            self.extra_files.append(blob.name)

    def validate_file_description(self, file_description: str) -> None:
        if not file_description:
            return
        strata = [
            {
                dimension: values.split(',')
                for dimension, values in (point.split('=')
                                          for point in stratum.split(';'))
            } for stratum in file_description.split('\n')
        ]
        log.debug(strata)
        valid_keys = [
            'genusSpecies',
            'developmentStage',
            'organ',
            'libraryConstructionApproach',
        ]
        for stratum in strata:
            for dimension, values in stratum.items():
                assert dimension in valid_keys, stratum
                assert len(values) > 0, stratum

    def validate_descriptors_file(self, blob: gcs.Blob) -> None:
        # Expected syntax: descriptors/{metadata_type}/{metadata_id}_{version}.json
        metadata_type, metadata_file = blob.name.split('/')[-2:]
        assert metadata_file.count('_') == 1
        assert metadata_file.endswith('.json')
        metadata_id, metadata_version = metadata_file.split('_')
        file_json = self.download_blob_as_json(blob)
        self.validate_file_json(file_json, blob.name)
        file_name = file_json['file_name']
        self.names_to_id[file_name] = metadata_id
        if metadata_file := self.metadata_files.get(metadata_id):
            metadata_file['found_descriptor'] = True
            metadata_file['crc32c'] = file_json['crc32c']
            assert metadata_file['version'] == metadata_version
        else:
            self.extra_files.append(blob.name)

    def validate_data_file(self, blob: gcs.Blob) -> None:
        # Expected syntax: data/{file_path}
        prefix = self.sa_path + 'data/'
        assert blob.name.startswith(prefix)
        file_name = blob.name[len(prefix):]
        metadata_file = None
        if metadata_id := self.names_to_id.get(file_name):
            if metadata_file := self.metadata_files.get(metadata_id):
                metadata_file['found_data_file'] = True
                assert metadata_file['crc32c'] == base64.b64decode(blob.crc32c).hex()
        if metadata_file is None:
            self.extra_files.append(blob.name)

    def validate_file_json(self, file_json: JSON, file_name: str) -> None:
        if self.args.validate_json:
            try:
                self.validator.validate_json(file_json, file_name)
            except BaseException as e:
                log.error('File %s failed json validation.', file_name)
                self.file_errors[file_name] = e

    def check_results(self):
        log.info('Checking results')
        for metadata_id, metadata_file in self.metadata_files.items():
            try:
                self.check_result(metadata_file)
            except BaseException as e:
                log.error('File error: %s', metadata_file)
                self.file_errors[metadata_id] = e
        if not self.file_errors and not self.extra_files:
            log.info('No errors found')

    def check_result(self, metadata_file):
        if metadata_file['file_type'] == 'project':
            if not metadata_file['found_metadata']:
                log.warning('A metadata file was not found for project %s',
                            one(metadata_file['projects']))
        else:
            assert metadata_file['found_metadata'], metadata_file
        if metadata_file['file_type'].endswith('_file'):
            assert metadata_file['found_descriptor'], metadata_file
            assert metadata_file['found_data_file'], metadata_file
        if 'valid_stratification' in metadata_file:
            assert metadata_file['valid_stratification']

    def validate_uuid(self, value: str) -> None:
        """
        Verify given value is a valid UUID string.
        """
        try:
            uuid.UUID(value)
        except ValueError as e:
            raise ValueError('Invalid uuid value', value) from e


class SchemaValidator:

    @classmethod
    def validate_json(cls, file_json: JSON, file_name: str):
        log.debug('Validating JSON of %s', file_name)
        try:
            schema = cls._download_schema(file_json['describedBy'])
        except json.decoder.JSONDecodeError as e:
            raise Exception(f"Unable to parse json from {file_json['describedBy']} for file {file_name}") from e
        try:
            validate(file_json, schema, format_checker=FormatChecker())
        except ValidationError as e:
            # Add filename to exception message but also keep original args[0]
            raise ValidationError(f'File {file_name} caused: {e.args[0]}') from e

    @classmethod
    @cache
    def _download_schema(cls, schema_url: str) -> JSON:
        log.debug('Downloading schema %s', schema_url)
        response = requests.get(schema_url, allow_redirects=False)
        response.raise_for_status()
        return response.json()


if __name__ == '__main__':
    configure_script_logging(log)
    adapter = StagingAreaValidator(sys.argv[1:])
    sys.exit(adapter.main())
