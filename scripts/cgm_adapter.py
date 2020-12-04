"""
Copy Contributor-Generated Matrices (CGM) data files to a DCP/2 staging bucket
along with generated supplementary_file, links, and project_0 JSON files.
"""
import argparse
import base64
from copy import (
    deepcopy,
)
import csv
from datetime import (
    datetime,
)
import json
import logging
import sys
from typing import (
    Any,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Tuple,
)
from urllib import (
    parse,
)
import uuid

# PyCharm doesn't seem to recognize PEP 420 namespace packages
# noinspection PyPackageRequirements
from furl import (
    furl,
)
import google.cloud.storage as gcs
from jsonschema import (
    FormatChecker,
    ValidationError,
    validate,
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


class File:
    file_namespace = uuid.UUID('5767014a-c431-4019-8703-0ab1b3e9e4d0')

    def __init__(self, file_name, file_source):
        self.name = file_name
        self.uuid = str(uuid.uuid5(self.file_namespace, file_name))
        self.source = file_source
        self.description = ''

    def set_file_description(self,
                             line_num: int,
                             species: str,
                             stage: str,
                             organ: str,
                             library: str) -> None:
        parsed = self.parse_stratification(line_num, species, stage, organ, library)
        lines = []
        for d in parsed:
            lines.append(';'.join(f'{k}={",".join(v)}' for k, v in d.items()))
        self.description = '\n'.join(lines)

    def parse_strat(self, string: str) -> Mapping[Optional[str], List[str]]:
        """
        >>> file = File('foo.txt', '')
        >>> file.parse_strat('human: adult, human: child, mouse: juvenile')
        {'human': ['adult', 'child'], 'mouse': ['juvenile']}

        >>> file.parse_strat('adult, child')
        {None: ['adult', 'child']}
        """

        strat = {}
        for val in [s.strip() for s in string.split(',')]:
            if ':' in val:
                key, _, val = val.partition(':')
                key = key.strip().lower()
            else:
                key = None
            if key not in strat:
                strat[key] = []
            val = val.strip().lower()
            strat[key].append(val)
        return strat

    def parse_stratification(self,
                             line_num: int,
                             species: str,
                             stage: str,
                             organ: str,
                             library: str) -> List[Mapping[str, List[str]]]:
        """
        >>> file = File('foo.txt', '')
        >>> file.parse_stratification(9, 'human', 'adult', 'blood', '10x')
        [{'species': ['human'], 'stage': ['adult'], 'organ': ['blood'], 'library': ['10x']}]

        >>> file.parse_stratification(9, 'human, mouse', 'adult', 'blood', '10x')
        [{'species': ['human', 'mouse'], 'stage': ['adult'], 'organ': ['blood'], 'library': ['10x']}]

        >>> file.parse_stratification(9, 'human, mouse', 'human: adult, mouse: child', 'blood', '10x')
        [{'species': ['human'], 'stage': ['adult'], 'organ': ['blood'], 'library': ['10x']}, \
{'species': ['mouse'], 'stage': ['child'], 'organ': ['blood'], 'library': ['10x']}]

        >>> file.parse_stratification(9, 'human, mouse', 'human: adult', 'blood', '10x')
        Traceback (most recent call last):
        ...
        azul.RequirementError: Error with row 9 'stage' keys ['human'].

        >>> file.parse_stratification(9, 'human, mouse', 'human: adult, mouse: child, cat: kitten', 'blood', '10x')
        Traceback (most recent call last):
        ...
        azul.RequirementError: Error with row 9 'stage' keys ['cat', 'human', 'mouse'].
        """
        strats = [{}]
        pairs = (('species', species), ('stage', stage), ('organ', organ), ('library', library))
        for category, value in pairs:
            if value:
                parsed = self.parse_strat(value)
                if None in parsed:
                    # value applies to all
                    assert len(parsed) == 1, parsed
                    for strat in strats:
                        strat[category] = parsed[None]
                else:
                    # value applies to one
                    # find the dict with a multi-value field we need to split
                    keys = list(parsed.keys())
                    for strat in strats:
                        for cat, val in strat.items():
                            if set(keys) == set(val):
                                strat[cat] = [keys.pop(0)]
                                while len(keys) > 0:
                                    new_strat = deepcopy(strat)
                                    new_strat[cat] = [keys.pop(0)]
                                    strats.append(new_strat)
                    # put each value in the appropriate dict
                    keys = set(parsed.keys())
                    for strat in strats:
                        for k, v in parsed.items():
                            if [k] in strat.values():
                                strat[category] = v
                                keys -= {k}
                    require(len(keys) == 0,
                            f'Error with line {line_num} {category!r} keys {sorted(keys)}.')
        return strats

    def file_extension(self) -> str:
        """
        Return the file extension from a file. e.g. '.pdf', '.fastq.gz'
        """
        parts = self.name.split('.')
        if parts[-1] == 'gz' and len(parts) > 2:
            return f'{parts[-2]}.{parts[-1]}'
        else:
            return parts[-1]


class CGMAdapter:

    def main(self):
        self._run()
        exit_code = 0
        for file_uuid, msg in self.file_errors.items():
            log.error(msg, file_uuid)
        for project_uuid, e in self.validation_exceptions.items():
            log.error('Validation error in project: %s', project_uuid, exc_info=e)
        if self.file_errors:
            exit_code |= 2
            log.error('Encountered %i errors with files in the source bucket',
                      len(self.file_errors))
        if self.validation_exceptions:
            exit_code |= 4
            log.error('Encountered %i projects with invalid json files',
                      len(self.validation_exceptions))
        return exit_code

    @classmethod
    def _parse_args(cls, argv):
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument('--source-area', '-s',
                            required=True,
                            help='The Google Cloud Storage URL of the source area. '
                                 'Syntax is gs://<bucket>[/<path>].')
        parser.add_argument('--staging-area', '-t',
                            required=True,
                            help='The Google Cloud Storage URL of the staging area. '
                                 'Syntax is gs://<bucket>[/<path>].')
        parser.add_argument('--csv-file', '-f',
                            required=True,
                            help='CSV spreadsheet file.')
        parser.add_argument('--version', '-v',
                            default=None,
                            help='(Optional) Use the specified value for the '
                                 'file versions instead of the current date. '
                                 'Example: ' + datetime.now().strftime(cls.date_format))
        parser.add_argument('--catalog', '-c',
                            default=None,
                            help='(Optional) Only process projects that exist '
                                 'in the specified catalog.')
        parser.add_argument('--no-json-validation', '-J',
                            action='store_false', default=True, dest='validate_json',
                            help='Do not validate JSON documents against their schema before staging them.')
        args = parser.parse_args(argv)
        return args

    link_namespace = uuid.UUID('82371164-6359-49f1-a9a3-913ae5d17f29')
    submitter_namespace = uuid.UUID('382415e5-67a6-49be-8f3c-aaaa707d82db')

    # As Contributor-generated matrices have no bundle UUID that can serve as
    # the link id (part of the links.json object name), a UUID is generated
    # from `link_namespace` and project id concatenated with `generation`.
    # This allows future runs to either produce replacements (same link id with
    # later version) or updates (same project id with a different link id) by
    # incrementing `generation`.
    # TODO: parametrize `generation` variable
    generation = 0

    date_format = '%Y-%m-%dT%H:%M:%S.%fZ'

    def __init__(self, argv: List[str]) -> None:
        super().__init__()
        self.args = self._parse_args(argv)
        self.file_errors: MutableMapping[str, str] = {}
        self.validation_exceptions: MutableMapping[str, BaseException] = {}
        if self.args.version is None:
            self.timestamp = datetime.now().strftime(self.date_format)
        else:
            version = datetime.strptime(self.args.version, self.date_format)
            assert self.args.version == version.strftime(self.date_format)
            self.timestamp = self.args.version
        self.gcs = gcs.Client()
        self.src_bucket, self.src_path = self._parse_gcs_url(self.args.source_area)
        self.dst_bucket, self.dst_path = self._parse_gcs_url(self.args.staging_area)

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
        """
        Process each row in the CSV.
        """
        self.file_errors.clear()
        self.validation_exceptions.clear()
        log.info(f'Version set to: {self.timestamp}')
        projects = self.parse_csv()
        completed, skipped = 0, 0
        for project in projects.values():
            project_id = project['project_uuid']
            if self.args.catalog and not self.project_in_catalog(project_id):
                skipped += 1
                log.info(f'Skipped project {project_id!r} not found in {self.args.catalog!r}')
            else:
                result = self.process_project(project)
                completed += 1 if result else 0
        log.info(f'Completed staging {completed} projects.')
        if skipped:
            log.info(f'Skipped {skipped} projects not found in {self.args.catalog!r}')

    def project_in_catalog(self, project_id: str) -> bool:
        url = furl('https://service.dev.singlecell.gi.ucsc.edu',
                   path=f'/index/projects/{project_id}',
                   args=dict(catalog=self.args.catalog)).url
        response = requests.get(url)
        if response.status_code == 200:
            return True
        elif response.status_code == 404:
            return False
        else:
            raise Exception(f'Unexpected response status {response.status_code!r} from {url!r}')

    def parse_csv(self) -> Mapping[str, Any]:
        """
        Parse the CSV file for project and file information.
        """
        projects = {}
        with open(self.args.csv_file) as csv_file:
            reader = csv.DictReader(csv_file)
            line_num = 1  # row 1 is column titles
            for row in reader:
                line_num += 1
                project_uuid = row['project_uuid']
                project_shortname = row['project_shortname']
                file_name = row['file_name']
                file_source = row['file_source']
                self.validate_uuid(project_uuid)
                require('.' in file_name, file_name)
                if project_uuid in projects:
                    require(project_shortname == projects[project_uuid]['shortname'],
                            'Rows for same project must have same shortname', project_uuid)
                else:
                    projects[project_uuid] = {
                        'project_uuid': project_uuid,
                        'shortname': project_shortname,
                        'files': {}
                    }
                if file_name not in projects[project_uuid]['files']:
                    file = File(file_name, file_source)
                    projects[project_uuid]['files'][file_name] = file
                else:
                    file = projects[project_uuid]['files'][file_name]
                file.set_file_description(line_num, row['species'], row['stage'], row['organ'], row['library'])
        return projects

    def validate_uuid(self, value: str) -> None:
        """
        Verify given value is a valid UUID string.
        """
        try:
            uuid.UUID(value)
        except ValueError as e:
            raise ValueError('Invalid uuid value', value) from e

    def process_project(self, project: Mapping[str, Any]) -> bool:
        """
        Create and upload file JSON and copy Blob data to the staging area.
        """
        project_uuid = project['project_uuid']
        log.info(f'Processing project {project_uuid}')
        files, blobs = dict(), dict()
        file_name = self.links_json_file_name(project_uuid)
        files[file_name] = self.links_json(project)
        for file in project['files'].values():
            blob_path = self.blob_path(project_uuid, project['shortname'], file.name)
            blob = self.get_blob(blob_path)
            if blob is None:
                msg = f'File not found gs://{self.src_bucket.name}/{blob_path}'
                self.file_errors[file.uuid] = msg
                log.error(msg)
                return False
            file_name = self.metadata_file_name('supplementary_file', file.uuid)
            files[file_name] = self.supplementary_file_json(file)
            file_name = self.file_descriptor_file_name(file.uuid)
            files[file_name] = self.file_descriptor_json(blob, file)
            blob_name = self.new_blob_path(file.name)
            blobs[blob_name] = blob
        if self.args.validate_json:
            for file_name, file_json in files.items():
                try:
                    self.validator.validate_json(file_json, file_name)
                except BaseException as e:
                    log.error('File %s failed json validation.', file_name)
                    self.validation_exceptions[project_uuid] = e
                    return False
        for file_name, file_json in files.items():
            self.upload_json(file_json, file_name)
        for blob_name, blob in blobs.items():
            self.copy_blob(blob, blob_name, file.uuid)
        return True

    def blob_path(self, project_uuid: str, shortname: str, file_name: str):
        """
        Return the path to a blob in the source bucket.
        """
        return f'{project_uuid}-{shortname}/{file_name}'

    def get_blob(self, blob_path: str) -> gcs.Blob:
        """
        Return the blob from the source bucket.
        """
        return self.src_bucket.get_blob(blob_path)

    def links_json_file_name(self, project_uuid: str) -> str:
        """
        Return the bucket layout compliant object name for a links.json file.
        """
        link_id = str(uuid.uuid5(self.link_namespace, f'{project_uuid}{self.generation}'))
        return f'{self.dst_path}links/{link_id}_{self.timestamp}_{project_uuid}.json'

    def metadata_file_name(self, metadata_type: str, metadata_id: str) -> str:
        """
        Return the bucket layout compliant object name for a metadata file.
        """
        return f'{self.dst_path}metadata/{metadata_type}/{metadata_id}_{self.timestamp}.json'

    def file_descriptor_file_name(self, file_uuid: str) -> str:
        """
        Return the bucket layout compliant object name for a file descriptor file.
        """
        return f'{self.dst_path}descriptors/supplementary_file/{file_uuid}_{self.timestamp}.json'

    def new_blob_path(self, file_name) -> str:
        """
        Return the bucket layout compliant object name for a data file.
        """
        return f'{self.dst_path}data/{file_name}'

    def links_json(self, project: Mapping[str, Any]) -> JSON:
        """
        Return the body of the links.json file.
        """
        schema_version = '2.1.1'
        return {
            'describedBy': f'https://schema.humancellatlas.org/system/{schema_version}/links',
            'schema_type': 'links',
            'schema_version': schema_version,
            'links': [
                {
                    'link_type': 'supplementary_file_link',
                    'entity': {
                        'entity_type': 'project',
                        'entity_id': project['project_uuid']
                    },
                    'files': [
                        {
                            'file_id': file.uuid,
                            'file_type': 'supplementary_file'
                        }
                        for file in project['files'].values()
                    ]
                }
            ]
        }

    def supplementary_file_json(self, file: File) -> JSON:
        """
        Return the body of a supplementary file type metadata file.
        """
        schema_version = '2.2.0'
        return {
            'describedBy': f'https://schema.humancellatlas.org/type/file/{schema_version}/supplementary_file',
            'schema_type': 'file',
            'schema_version': schema_version,
            'file_core': {
                'format': file.file_extension(),
                'content_description': [
                    {
                        'text': 'Contributor-generated matrix',
                        'ontology': 'data:2082',
                        'ontology_label': 'Matrix'
                    }
                ],
                'file_name': file.name
            },
            'file_description': file.description,
            'provenance': {
                'document_id': file.uuid,
                'submission_date': self.timestamp,
                'submitter_id': str(uuid.uuid5(self.submitter_namespace, file.source)),
                'update_date': self.timestamp
            }
        }

    def file_descriptor_json(self, blob: gcs.Blob, file: File) -> JSON:
        """
        Return the body of a file descriptor type file.
        """
        schema_version = '2.0.0'
        return {
            'describedBy': f'https://schema.humancellatlas.org/system/{schema_version}/file_descriptor',
            'schema_type': 'file_descriptor',
            'schema_version': schema_version,
            'file_name': file.name,
            'size': blob.size,
            'file_id': file.uuid,
            'file_version': self.timestamp,
            'content_type': blob.content_type,
            'crc32c': base64.b64decode(blob.crc32c).hex(),
            'sha256': None if blob.metadata is None else blob.metadata.get('sha256'),
        }

    def upload_json(self, file_contents: JSON, blob_path: str) -> None:
        """
        Perform an upload of JSON data to a file in the staging bucket.
        """
        log.info(f'Uploading {blob_path}')
        self.dst_bucket.blob(blob_path).upload_from_string(
            data=json.dumps(file_contents, indent=4),
            content_type='application/json'
        )

    def copy_blob(self, src_blob: gcs.Blob, blob_path: str, file_uuid: str) -> None:
        """
        Perform a bucket to bucket copy of a blob file.
        """
        log.info(f'Copying blob to {blob_path}')
        dst_blob = self.dst_bucket.get_blob(blob_path)
        if dst_blob and src_blob.md5_hash != dst_blob.md5_hash:
            msg = f'MDF mismatch for {src_blob.name}, {src_blob.md5_hash} != {dst_blob.md5_hash}'
            self.file_errors[file_uuid] = msg
            log.error(msg)
        elif not dst_blob:
            dst_blob = gcs.Blob(name=blob_path, bucket=self.dst_bucket)
            token, _, _ = dst_blob.rewrite(source=src_blob)
            while token is not None:
                token, _, _ = dst_blob.rewrite(source=src_blob, token=token)


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
    adapter = CGMAdapter(sys.argv[1:])
    sys.exit(adapter.main())
