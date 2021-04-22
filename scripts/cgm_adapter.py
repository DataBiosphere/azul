"""
Copy Contributor-Generated Matrices (CGM) data files to a DCP/2 staging bucket
along with generated supplementary_file, links, and project_0 JSON files.

Requires a 'csv' file with one line per matrix file and the following columns:
    - project_uuid: A project UUID.
    - project_shortname: A project shortname.
    - file_name: The full file name with extension and without path.
    - file_source: The source of the matrix file, used to generate submitter_id.
    - catalog: (column optional) The catalog the row is associated with.
    - genusSpecies: Stratification values for this dimension.
    - developmentStage: Stratification values for this dimension.
    - organ: Stratification values for this dimension.
    - libraryConstructionApproach: Stratification values for this dimension.
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
                             row_num: int,
                             row: JSON) -> None:
        points = {
            'genusSpecies': row['genusSpecies'],
            'developmentStage': row['developmentStage'],
            'organ': row['organ'],
            'libraryConstructionApproach': row['libraryConstructionApproach']
        }
        strata = self.parse_stratification(row_num, points)
        lines = []
        for stratum in strata:
            line = ';'.join(f'{dimension}={",".join(values)}'
                            for dimension, values in stratum.items())
            lines.append(line)
        self.description = '\n'.join(lines)

    def parse_values(self, values: str) -> Mapping[Optional[str], List[str]]:
        """
        >>> file = File('foo.txt', '')
        >>> file.parse_values('human: adult, human: child, mouse: juvenile')
        {'human': ['adult', 'child'], 'mouse': ['juvenile']}

        >>> file.parse_values('adult, child')
        {None: ['adult', 'child']}
        """

        parsed = {}
        split_values = [s.strip() for s in values.split(',')]
        for value in split_values:
            if ':' in value:
                parent, _, value = value.partition(':')
                parent = parent.strip()
            else:
                parent = None
            if parent not in parsed:
                parsed[parent] = []
            value = value.strip()
            parsed[parent].append(value)
        return parsed

    def parse_stratification(self,
                             row_num: int,
                             points: JSON) -> List[Mapping[str, List[str]]]:
        """
        >>> file = File('foo.txt', '')
        >>> file.parse_stratification(1, {'species': 'human', 'organ': 'blood'})
        [{'species': ['human'], 'organ': ['blood']}]

        >>> file.parse_stratification(2, {'species': 'human, mouse', 'organ': 'blood'})
        [{'species': ['human', 'mouse'], 'organ': ['blood']}]

        >>> file.parse_stratification(3, {'species': 'human, mouse', 'organ': 'human: blood, mouse: brain'})
        [{'species': ['human'], 'organ': ['blood']}, {'species': ['mouse'], 'organ': ['brain']}]

        >>> file.parse_stratification(4, {'species': 'human, mouse', 'organ': 'human: blood'})
        Traceback (most recent call last):
        ...
        azul.RequirementError: Row 4 'organ' values ['human'] differ from parent dimension.

        >>> file.parse_stratification(5, {'species': 'human, mouse', 'organ': 'human: blood, mouse: brain, cat: brain'})
        Traceback (most recent call last):
        ...
        azul.RequirementError: Row 5 'organ' values ['cat', 'human', 'mouse'] differ from parent dimension.
        """
        strata = [{}]
        for dimension, values in points.items():
            if values:
                parsed_values = self.parse_values(values)
                if None in parsed_values:
                    # Add the value to all stratum
                    assert len(parsed_values) == 1, parsed_values
                    for stratum in strata:
                        stratum[dimension] = parsed_values[None]
                else:
                    # Each value belongs to a separate stratum. Find the stratum
                    # with the matching multi-value point and split it into
                    # separate stratum.
                    parents = list(parsed_values.keys())
                    for stratum in strata:
                        for dimension_, values_ in stratum.items():
                            if set(parents) == set(values_):
                                stratum[dimension_] = [parents.pop(0)]
                                while len(parents) > 0:
                                    new_stratum = deepcopy(stratum)
                                    new_stratum[dimension_] = [parents.pop(0)]
                                    strata.append(new_stratum)
                    # Put each value in its specified stratum
                    parents = set(parsed_values.keys())
                    for stratum in strata:
                        for parent, values_ in parsed_values.items():
                            if [parent] in stratum.values():
                                stratum[dimension] = values_
                                parents -= {parent}
                    require(len(parents) == 0,
                            f"Row {row_num} {dimension!r} values {sorted(parents)} differ from parent dimension.")
        return strata

    def file_extension(self) -> str:
        """
        Return the file extension from a file. e.g. 'pdf', 'fastq.gz'
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
        catalog = self.args.catalog
        completed, skipped = 0, 0
        for project in projects.values():
            if catalog and not self.project_in_catalog(project, catalog):
                skipped += 1
                project_id = project['project_uuid']
                log.info(f'Skipped project {project_id!r} not found in {catalog!r}')
            else:
                result = self.process_project(project)
                completed += 1 if result else 0
        log.info(f'Completed staging {completed} projects.')
        if skipped:
            log.info(f'Skipped {skipped} projects not found in {catalog!r}')

    def project_in_catalog(self, project: Mapping[str, Any], catalog: str) -> bool:
        # If project's catalog was given on spreadsheet, check against that
        project_catalog = project['catalog']
        if project_catalog:
            return project_catalog == catalog
        # Otherwise ping Azul to check if the project exists in the catalog.
        else:
            project_id = project['project_uuid']
            url = furl('https://service.azul.data.humancellatlas.org/',
                       path=f'/index/projects/{project_id}',
                       args=dict(catalog=catalog)).url
            response = requests.get(url)
            if response.status_code == 200:
                return True
            elif response.status_code in (400, 404):
                return False
            else:
                raise Exception(f'Unexpected response status {response.status_code!r} from {url!r}')

    def parse_csv(self) -> Mapping[str, Any]:
        """
        Parse the CSV file for project and file information.
        """
        projects = {}
        file_names = set()
        with open(self.args.csv_file) as csv_file:
            reader = csv.DictReader(csv_file)
            row_num = 1  # row 1 is column titles
            for row in reader:
                row_num += 1
                project_uuid = row['project_uuid']
                project_shortname = row['project_shortname']
                file_name = row['file_name']
                file_source = row['file_source']
                catalog = row.get('catalog')  # Optional column
                self.validate_uuid(project_uuid)
                require('.' in file_name, f'File {file_name!r} has an invalid name')
                require(file_name not in file_names, f'File {file_name!r} is not unique')
                file_names.add(file_name)
                if project_uuid in projects:
                    require(project_shortname == projects[project_uuid]['shortname'],
                            'Rows for same project must have same shortname', project_uuid)
                else:
                    projects[project_uuid] = {
                        'project_uuid': project_uuid,
                        'shortname': project_shortname,
                        'catalog': catalog,
                        'files': {}
                    }
                file = File(file_name, file_source)
                projects[project_uuid]['files'][file_name] = file
                file.set_file_description(row_num, row)
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
            self.copy_blob(blob, blob_name)
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

    def copy_blob(self, src_blob: gcs.Blob, blob_path: str) -> None:
        """
        Perform a bucket to bucket copy of a blob file.
        """
        log.info(f'Copying blob to {blob_path}')
        dst_blob = self.dst_bucket.get_blob(blob_path)
        if dst_blob and src_blob.md5_hash != dst_blob.md5_hash:
            msg = f'MDF mismatch for {src_blob.name}, {src_blob.md5_hash} != {dst_blob.md5_hash}'
            self.file_errors[src_blob.name] = msg
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
