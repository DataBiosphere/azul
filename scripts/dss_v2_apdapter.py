"""
Copies data and metadata files from a source DSS to a DCP/2 staging bucket.
"""
import argparse
from concurrent.futures import (
    CancelledError,
    Future,
    ThreadPoolExecutor,
    as_completed,
)
import copy
from datetime import datetime
from functools import (
    cached_property,
    lru_cache,
)
import json
import logging
import sys
from threading import RLock
import time
from typing import (
    Dict,
    List,
    MutableMapping,
    Set,
    Tuple,
)
from urllib import parse

from botocore.config import Config
# PyCharm doesn't seem to recognize PEP 420 namespace packages
# noinspection PyPackageRequirements
import google.cloud.storage as gcs
from jsonschema import (
    FormatChecker,
    ValidationError,
    validate,
)
import requests

from azul import config
import azul.dss
from azul.indexer import BundleFQID
from azul.logging import configure_script_logging
from azul.types import (
    AnyJSON,
    JSON,
    MutableJSON,
)

log = logging.getLogger(__name__)


class DSSv2Adapter:

    def main(self):
        self._run()
        exit_code = 0
        for bundle_fqid, e in self.skipped_bundles.items():
            log.warning('Invalid input bundle: %s', bundle_fqid, exc_info=e)
        for bundle_fqid, e in self.errors.items():
            log.error('Invalid output bundle: %s', bundle_fqid, exc_info=e)
        if self.analysis_bundles:
            log.info('The following %i analysis bundles were skipped: %s',
                     len(self.analysis_bundles), self.analysis_bundles)
        if self.skipped_bundles:
            exit_code |= 2  # lowest bit reserved for uncaught exceptions
            log.warning('The DSS instance contains invalid bundles (%i). '
                        'These were skipped entirely and nothing was staged for them. ',
                        len(self.skipped_bundles))
        if self.errors:
            exit_code |= 4
            log.error('Unexpected errors occurred while processing bundles (%i). '
                      'It is likely that the staging is now corrupted with partial data. ',
                      len(self.errors))
        return exit_code

    @classmethod
    def _parse_args(cls, argv):
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument('--dss-endpoint', '-f',
                            default=config.dss_endpoint,
                            help='The URL of the source DSS REST API endpoint. '
                                 'Default is %(default)s).')
        parser.add_argument('--staging-area', '-t',
                            required=True,
                            help='The Google Cloud Storage URL of the staging area. '
                                 'Syntax is gs://<bucket>[/<path>].')
        parser.add_argument('--bundle-uuid-prefix', '-p',
                            default='',
                            help='Process only bundles whose UUID starts with the given prefix. '
                                 'Default is the empty string.')
        parser.add_argument('--bundle-uuid-start-prefix', '-s',
                            default='',
                            help='Copy only bundles whose UUID is lexicographically greater than or equal to the '
                                 'given prefix. Must be less than eight characters and start with the prefix specified '
                                 'via --bundle-uuid-prefix.')
        parser.add_argument('--skip-analysis-bundles', '-A',
                            action='store_true', default=False,
                            help='Do not copy any analysis bundles.')
        parser.add_argument('--no-input-validation', '-I',
                            action='store_false', default=True, dest='validate_input',
                            help='Do not validate JSON documents against their schema after fetching them from DSS.')
        parser.add_argument('--no-output-validation', '-O',
                            action='store_false', default=True, dest='validate_output',
                            help='Do not validate JSON documents against their schema before staging them.')
        parser.add_argument('--num-workers', '-w',
                            type=int, default=32,
                            help='Number of worker threads to use. Each worker will process one bundle at a time.')
        args = parser.parse_args(argv)
        return args

    dss_src_replica = 'aws'

    def __init__(self, argv: List[str]) -> None:
        super().__init__()
        self.args = self._parse_args(argv)
        self.analysis_bundles: Set[BundleFQID] = set()
        self.skipped_bundles: Dict[BundleFQID, BaseException] = {}
        self.errors: Dict[BundleFQID, BaseException] = {}

        self._mini_dss = None
        self._mini_dss_expiration = None
        self._mini_dss_lock = RLock()
        _ = self.mini_dss  # Avoid lazy loading to fail early if any issue allocating client

        self.gcs = gcs.Client()
        self.src_bucket = self._get_bucket('org-hca-dss-prod')
        dst_bucket_name, self.dst_path = self._parse_staging_area()
        self.dst_bucket = self._get_bucket(dst_bucket_name)

    @property
    def mini_dss(self):
        with self._mini_dss_lock:
            if self._mini_dss is None or self._mini_dss_expiration < time.time():
                dss_client_timeout = 30 * 60  # DSS session credentials timeout after 1 hour
                log.info('Allocating new DSS client for %s to expire in %d seconds',
                         self.args.dss_endpoint, dss_client_timeout)
                self._mini_dss = azul.dss.MiniDSS(dss_endpoint=self.args.dss_endpoint,
                                                  config=Config(max_pool_connections=self.args.num_workers))
                self._mini_dss_expiration = time.time() + dss_client_timeout
            return self._mini_dss

    def _parse_staging_area(self) -> Tuple[str, str]:
        """
        Validate and parse the given staging area URL into bucket and path values.
        Path value will not have a prefix '/' and will have a postfix '/' if not empty.
        """
        split_url = parse.urlsplit(self.args.staging_area)
        if split_url.scheme != 'gs' or not split_url.netloc:
            raise ValueError('Staging area must be in gs://<bucket>[/<path>] format')
        elif split_url.path.endswith('/'):
            raise ValueError('Staging area URL must not end with a "/"')
        else:
            path = f"{split_url.path.lstrip('/')}/" if split_url.path else ''
            return split_url.netloc, path

    def _get_bucket(self, bucket_name: str) -> gcs.Bucket:
        """
        Returns a reference to a GCS bucket.
        """
        log.info('Getting bucket: %s', bucket_name)
        bucket = self.gcs.get_bucket(bucket_name)
        return bucket

    def _run(self):
        """
        Request a list of all bundles in the DSS and process each bundle.
        """
        self.analysis_bundles.clear()
        self.skipped_bundles.clear()
        self.errors.clear()

        prefixes = self.get_prefix_list(self.args.bundle_uuid_prefix, self.args.bundle_uuid_start_prefix)

        params = {
            'replica': self.dss_src_replica,
            'per_page': 500,  # limits how many bundles per request min=10 max=500
        }
        if prefixes:
            params['prefix'] = prefixes.pop(0)
        url_base = self.args.dss_endpoint + '/bundles/all'
        url = url_base + '?' + parse.urlencode(params)

        future_to_bundle: MutableMapping[Future, BundleFQID] = {}
        with ThreadPoolExecutor(max_workers=self.args.num_workers) as tpe:
            while True:
                log.info('Requesting list of bundles: %s', url)
                response = requests.get(url)
                response.raise_for_status()
                response_json = response.json()
                for bundle in response_json['bundles']:
                    bundle_fqid = BundleFQID(uuid=bundle['uuid'], version=bundle['version'])
                    future = tpe.submit(self._process_bundle, bundle_fqid)
                    future_to_bundle[future] = bundle_fqid
                if response_json.get('has_more'):
                    url = response_json.get('link')
                elif prefixes:
                    params['prefix'] = prefixes.pop(0)
                    url = url_base + '?' + parse.urlencode(params)
                else:
                    break
            for future in as_completed(future_to_bundle):
                bundle_fqid = future_to_bundle[future]
                try:
                    future.result()
                except CancelledError:
                    pass  # ignore exception raised when a future is cancelled
                except BaseException as e:
                    # Bundles are read and checked first and skipped if an error
                    # occurs at that time, so an exception here means some of a
                    # bundle's content was probably transferred, leaving the
                    # staging area in an inconsistent state. To prevent further
                    # inconsistencies we do an early and graceful exit cancelling
                    # all pending futures. Should any other future already in
                    # progress fail, it will be caught here as well and added to
                    # self.errors to be reported at the end of the script.
                    self.errors[bundle_fqid] = e
                    for f in future_to_bundle:
                        f.cancel()

    def _process_bundle(self, bundle_fqid: BundleFQID):
        """
        Transfer all files from the given bundle to a GCS staging bucket.
        """
        log.info('Requesting Bundle: %s', bundle_fqid)
        bundle_manifest = self.mini_dss.get_bundle(uuid=bundle_fqid.uuid,
                                                   version=bundle_fqid.version,
                                                   replica=self.dss_src_replica)
        indexed_files: MutableJSON = {}
        manifest_entry: AnyJSON
        for manifest_entry in bundle_manifest['files']:
            if manifest_entry['indexed']:
                file_name = manifest_entry['name']
                blob_key = self._build_blob_key(manifest_entry)
                indexed_files[file_name] = json.loads(self._download_blob_as_string(blob_key))

        try:
            bundle = BundleConverter(bundle_fqid,
                                     bundle_manifest,
                                     indexed_files,
                                     self.args.validate_input,
                                     self.args.validate_output)
            if self.args.skip_analysis_bundles and bundle.is_analysis_bundle:
                log.info('Skipping analysis bundle %s', bundle_fqid)
                self.analysis_bundles.add(bundle_fqid)
                return
        except Exception as e:
            # Since we encountered an exception before any of the bundle's
            # content was transferred, only catch and log the error here as a
            # warning to allow the processing of other bundles to continue.
            log.warning(e.args[0])
            self.skipped_bundles[bundle_fqid] = e
            return

        for old_name, manifest_entry in bundle.manifest_entries.items():
            # links.json: upload new links.json
            if old_name == 'links.json':
                new_name = f'{self.dst_path}links/' + bundle.links_json_new_name()
                log.debug('Uploading file %s to %s', 'links.json', new_name)
                try:
                    self._upload_file_contents(bundle.new_links_json, new_name, manifest_entry['content-type'])
                except Exception as e:
                    raise Exception(bundle_fqid, old_name, 'Failed to upload file contents') from e

            # Metadata files: modify and upload
            elif manifest_entry['indexed']:
                new_name = f'{self.dst_path}metadata/' + bundle.metadata_file_new_name(old_name)
                file_contents = bundle.build_new_metadata_json(old_name)
                log.debug('Uploading file %s to %s', old_name, new_name)
                try:
                    self._upload_file_contents(file_contents, new_name, manifest_entry['content-type'])
                except Exception as e:
                    raise Exception(bundle_fqid, old_name, 'Failed to upload file contents') from e

            # data files: perform a bucket to bucket copy
            else:
                blob_key = self._build_blob_key(manifest_entry)
                new_name = f'{self.dst_path}data/' + bundle.data_file_new_name(old_name)
                src_blob = self.src_bucket.get_blob(f'blobs/{blob_key}')
                dst_blob = self.dst_bucket.get_blob(new_name)
                log.debug('Copying file %s to %s', old_name, new_name)
                if dst_blob and src_blob.md5_hash != dst_blob.md5_hash:
                    raise Exception(bundle_fqid, manifest_entry['name'],
                                    f'Existing file mismatch {src_blob.md5_hash} != {dst_blob.md5_hash}')
                else:
                    self._copy_file(src_blob, new_name)

        # file descriptors
        for metadata_file_name, metadata_json in bundle.indexed_files.items():
            metadata_id = bundle.manifest_entries[metadata_file_name]['uuid']
            metadata_type = bundle.schema_types[metadata_id]
            if metadata_type.endswith('_file'):
                file_name = f'{self.dst_path}descriptors/' + bundle.descriptor_file_new_name(metadata_file_name)
                file_json = bundle.build_descriptor_json(metadata_file_name, file_name)
                log.debug('Uploading file %s', file_name)
                try:
                    self._upload_file_contents(file_json, file_name, 'application/json')
                except Exception as e:
                    raise Exception(bundle_fqid, file_name, 'Failed to upload file contents') from e

    def _build_blob_key(self, manifest_entry: JSON) -> str:
        """
        Return the blob key of the file described by the given manifest_entry.
        """
        return '.'.join(manifest_entry[key] for key in ['sha256', 'sha1', 's3_etag', 'crc32c'])

    def _download_blob_as_string(self, blob_key: str) -> bytes:
        """
        Perform a file download from the source bucket.
        """
        return self.src_bucket.blob(f'blobs/{blob_key}').download_as_string()

    def _upload_file_contents(self, file_contents: JSON, new_name: str, content_type: str):
        """
        Perform an upload of JSON data to a file in the staging bucket.
        """
        self.dst_bucket.blob(new_name).upload_from_string(data=json.dumps(file_contents, indent=4),
                                                          content_type=content_type)

    def _copy_file(self, src_blob: gcs.Blob, new_name: str):
        """
        Perform a bucket to bucket copy of a file.
        """
        dst_blob = gcs.Blob(name=new_name, bucket=self.dst_bucket)
        token, bytes_rewritten, total_bytes = dst_blob.rewrite(source=src_blob)
        while token is not None:
            token, bytes_rewritten, total_bytes = dst_blob.rewrite(source=src_blob, token=token)

    def get_prefix_list(self, prefix: str = None, start_prefix: str = None):
        """
        Generate ascending hex prefixes.

        >>> self.get_prefix_list(prefix='aa', start_prefix=None)
        ['aa']

        >>> self.get_prefix_list(prefix='a', start_prefix='aa')
        ['aa', 'ab', 'ac', 'ad', 'ae', 'af']

        >>> self.get_prefix_list(prefix=None, start_prefix='aa')
        ['aa', 'ab', 'ac', 'ad', 'ae', 'af', 'b', 'c', 'd', 'e', 'f']

        >>> self.get_prefix_list(prefix=None, start_prefix=None)
        None
        """
        if prefix and start_prefix and not start_prefix.startswith(prefix):
            raise ValueError('Start prefix must start with prefix')
        if not prefix and not start_prefix:
            return None
        start = start_prefix or prefix
        if prefix:
            fill_count = len(start_prefix) - len(prefix) if start_prefix else 0
            end = prefix + 'f' * fill_count
        else:
            fill_count = len(start_prefix) if start_prefix else 1
            end = 'f' * fill_count
        trim = 0
        prefixes = []
        for prefix in [hex(i)[2:] for i in range(int(start, 16), int(end, 16) + 1)]:
            # If a start_prefix was used all the prefixes will have the same
            # length as start_prefix however this list can be trimmed down.
            # example: ['be', 'bf, 'c0, 'c1'...] to ['be', 'bf', 'c', 'd'...]
            next_prefix = prefix[:-trim] if trim > 0 else prefix
            if prefixes and next_prefix[-1] == '0':
                trim += 1
                prefixes.append(next_prefix[:-1])
            elif not prefixes or next_prefix != prefixes[-1]:
                prefixes.append(next_prefix)
        return prefixes


class BundleConverter:
    file_version_format = '%Y-%m-%dT%H:%M:%S.%fZ'

    def __init__(self,
                 bundle_fqid: BundleFQID,
                 bundle_manifest: JSON,
                 indexed_files: MutableJSON,
                 validate_input: bool,
                 validate_output: bool):
        self.bundle_fqid = bundle_fqid
        self.project_uuid: str = ''
        self.schema_types: MutableMapping[str, str] = {}  # Mapping of file uuid to schema type
        self.indexed_files = indexed_files
        self.new_links_json: MutableJSON = {}
        self.manifest_entries: MutableJSON = {}  # Mapping of file names to manifest entry
        manifest_entry: AnyJSON
        for manifest_entry in bundle_manifest['files']:
            file_name = manifest_entry['name']
            self.manifest_entries[file_name] = manifest_entry
        self.validate_output = validate_output
        self.check_bundle_manifest()
        self.clean_input_json()
        if validate_input:
            self.validate_input_json()
        self._setup()

    @cached_property
    def validator(self):
        return SchemaValidator()

    def _setup(self):
        """
        Extract info required for links.json and metadata file modifications.
        """
        self.project_uuid = self.indexed_files['project_0.json']['provenance']['document_id']
        manifest_entry: MutableJSON
        for file_name, manifest_entry in self.manifest_entries.items():
            file_uuid = manifest_entry['uuid']
            if manifest_entry['indexed']:  # Metadata files
                described_by = self.indexed_files[file_name]['describedBy']
                _, _, schema_type = described_by.rpartition('/')
                self.schema_types[file_uuid] = schema_type
                if schema_type == 'project' and self.project_uuid != file_uuid:
                    raise Exception(f'"document_id" from "project_0.json" ({self.project_uuid}) '
                                    f'does not match "uuid" from manifest entry ({file_uuid})')
            else:  # Data files
                self.schema_types[file_uuid] = 'data'
        self.new_links_json = self.build_new_links_json()

    @property
    def is_analysis_bundle(self):
        return 'analysis_process' in self.schema_types.values()

    def check_bundle_manifest(self):
        """
        Verify bundle manifest contains required files
        """
        missing_files = []
        if 'project_0.json' not in self.manifest_entries:
            missing_files.append('project_0.json')
        if 'links.json' not in self.manifest_entries:
            missing_files.append('links.json')
        if missing_files:
            raise Exception(f'No {" or ".join(missing_files)} found in bundle {self.bundle_fqid}')
        for file_name, file_content in self.indexed_files.items():
            if not file_content.get('describedBy'):
                raise Exception(f'"describedBy" missing in {file_name} of bundle {self.bundle_fqid}')

    def clean_input_json(self):
        """
        Remove known invalid properties from metadata json files
        """
        # These two properties were put in some documents even though the
        # schema wasn't updated to support them. Remove them if found.
        props = ('schema_major_version', 'schema_minor_version')
        for file_name, file_contents in self.indexed_files.items():
            found_props = {prop for prop in props if prop in file_contents.get('provenance', [])}
            if found_props:
                log.debug('Removing properties %s from %s', found_props, file_name)
                for prop in found_props:
                    file_contents['provenance'].pop(prop, None)

    def validate_input_json(self):
        """
        Validates all indexed JSON files against their 'describedBy' schema.
        """
        for file_name, file_contents in self.indexed_files.items():
            self.validator.validate_json(file_contents, file_name)

    def build_new_links_json(self) -> JSON:
        """
        Construct the new links.json.
        """
        old_json = self.indexed_files['links.json']
        supplementary_files = [uuid for uuid, schema in self.schema_types.items() if schema == 'supplementary_file']
        schema_version = '2.1.1'
        new_links_json = {
            'describedBy': f'https://schema.humancellatlas.org/system/{schema_version}/links',
            'schema_type': 'links',
            'schema_version': schema_version,
            'links': [],
        }
        # process links
        link: JSON
        for link in old_json['links']:
            new_link = {
                'link_type': 'process_link',
                'process_type': self.schema_types[link['process']],
                'process_id': link['process'],
                'inputs': [],
                'outputs': [],
                'protocols': [],
            }
            for prop in ('input', 'output', 'protocol'):
                for value in link[f'{prop}s']:
                    if isinstance(value, str):  # a uuid
                        new_link[f'{prop}s'].append(
                            {
                                f'{prop}_type': self.schema_types[value],
                                f'{prop}_id': value,
                            }
                        )
                    elif isinstance(value, dict):
                        new_link[f'{prop}s'].append(
                            {
                                f'{prop}_type': value[f'{prop}_type'],
                                f'{prop}_id': value[f'{prop}_id'],
                            }
                        )
                    else:
                        raise Exception(f"Unknown value type in links.json {prop} field.")
            new_links_json['links'].append(new_link)
        # supplementary file links
        if supplementary_files:
            new_link = {
                'link_type': 'supplementary_file_link',
                'entity': {
                    'entity_type': 'project',
                    'entity_id': self.project_uuid,
                },
                'files': [
                    {'file_id': uuid, 'file_type': 'supplementary_file'}
                    for uuid in supplementary_files
                ],
            }
            new_links_json['links'].append(new_link)

        if self.validate_output:
            self.validator.validate_json(new_links_json, self.links_json_new_name())
        return new_links_json

    def build_new_metadata_json(self, metadata_file_name: str):
        """
        Return the new metadata json for the given file name
        """
        metadata_id = self.manifest_entries[metadata_file_name]['uuid']
        metadata_type = self.schema_types[metadata_id]
        new_json: MutableJSON = copy.deepcopy(self.indexed_files[metadata_file_name])
        if metadata_type.endswith('_file'):
            metadata_json = self.indexed_files[metadata_file_name]
            data_file_name = metadata_json['file_core']['file_name']
            new_json['file_core']['file_name'] = data_file_name.replace('!', '/')  # Undo DCP v1 swap of '/' with '!'
        if self.validate_output:
            self.validator.validate_json(new_json, self.metadata_file_new_name(metadata_file_name))
        return new_json

    def build_descriptor_json(self, metadata_file_name: str, descriptor_file_name: str) -> JSON:
        """
        Returns the file descriptor json for a data file referenced by the given metadata file
        """
        metadata_json = self.indexed_files[metadata_file_name]
        data_file_name = metadata_json['file_core']['file_name']
        data_file_manifest_entry = self.manifest_entries[data_file_name]
        descriptor_json = {
            'describedBy': 'https://schema.humancellatlas.org/system/1.0.0/file_descriptor',
            'schema_type': 'file_descriptor',
            'schema_version': '1.0.0',
            'file_name': self.data_file_new_name(data_file_name),
            'size': data_file_manifest_entry['size'],
            'file_id': data_file_manifest_entry['uuid'],
            'file_version': self._format_file_version(data_file_manifest_entry['version']),
            # Only use base type (e.g. "application/gzip") from full value (e.g. "application/gzip; dcp-type=data")
            'content_type': data_file_manifest_entry['content-type'].partition(';')[0],
            'crc32c': data_file_manifest_entry['crc32c'],
            'sha1': data_file_manifest_entry['sha1'],
            'sha256': data_file_manifest_entry['sha256'],
            's3_etag': data_file_manifest_entry['s3_etag'],
        }
        if self.validate_output:
            self.validator.validate_json(descriptor_json, descriptor_file_name)
        return descriptor_json

    def _format_file_version(self, old_version: str) -> str:
        """
        Convert the old file version syntax to the new syntax
        """
        azul.dss.validate_version(old_version)
        return datetime.strptime(old_version, azul.dss.version_format).strftime(self.file_version_format)

    def links_json_new_name(self) -> str:
        """
        Return the bucket layout compliant object name for a links.json file.
        """
        new_version = self._format_file_version(self.bundle_fqid.version)
        return f'{self.bundle_fqid.uuid}_{new_version}_{self.project_uuid}.json'

    def metadata_file_new_name(self, old_name: str) -> str:
        """
        Return the bucket layout compliant object name for a metadata file.
        """
        metadata_id = self.manifest_entries[old_name]['uuid']
        metadata_type = self.schema_types[metadata_id]
        new_version = self._format_file_version(self.manifest_entries[old_name]['version'])
        return f'{metadata_type}/{metadata_id}_{new_version}.json'

    def descriptor_file_new_name(self, metadata_file_name: str) -> str:
        """
        Return the bucket layout compliant object name for a file descriptor file.
        """
        return self.metadata_file_new_name(metadata_file_name)

    def data_file_new_name(self, old_name: str) -> str:
        """
        Return the bucket layout compliant object name for a data file
        """
        new_name = old_name.replace('!', '/')  # Undo DCP v1 swap of '/' with '!'
        return self.bundle_fqid.uuid + '/' + new_name


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
    @lru_cache(maxsize=None)
    def _download_schema(cls, schema_url: str) -> JSON:
        log.debug('Downloading schema %s', schema_url)
        response = requests.get(schema_url, allow_redirects=False)
        response.raise_for_status()
        return response.json()


if __name__ == '__main__':
    configure_script_logging(log)
    adapter = DSSv2Adapter(sys.argv[1:])
    sys.exit(adapter.main())
