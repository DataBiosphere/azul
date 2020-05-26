"""
Copies data and metadata files from a source DSS to a DCP/2 staging bucket.
"""

import argparse
import logging
import json
import re
import requests
import sys
from threading import RLock
import time
from typing import (
    Mapping,
    Tuple,
)
from urllib import parse

from botocore.config import Config
from google.cloud import storage
from google.cloud.storage import (
    Blob,
    Bucket,
)

from azul import config
import azul.dss
from azul.logging import configure_script_logging
from azul.threads import DeferredTaskExecutor
from azul.types import JSON

log = logging.getLogger(__name__)


class DSSv2Adapter(DeferredTaskExecutor):

    def main(self):
        try:
            self.run()
        except KeyboardInterrupt:
            log.info('Caught KeyboardInterrupt. Exiting ... ')
        log.info('Total warnings encountered: %i', len(self.warnings))
        for warning in self.warnings:
            log.warning(warning)
        log.info('Total errors encountered: %i', len(self.errors))
        for error in self.errors:
            log.error(error)
        if self.errors:
            sys.exit(1)

    @classmethod
    def _parse_args(cls, argv):
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument('--dss-endpoint', '-d',
                            default=config.dss_endpoint,
                            help='The URL of the source DSS REST API endpoint '
                                 '(default: %(default)s).')
        parser.add_argument('--destination-url', '-u',
                            required=True,
                            help='The GCS URL of the destination (syntax: '
                                 'gs://bucket/path)')
        parser.add_argument('--bundle-uuid-prefix', '-b',
                            default='',
                            help='Copy bundles only with given prefix.')
        parser.add_argument('--bundle-uuid-start-prefix', '-s',
                            default='',
                            help='Copy bundles including and after given '
                                 'start prefix. Max length of 8 characters.')
        parser.add_argument('--debug',
                            action='store_true',
                            help='Output debug benchmarking results.')
        args = parser.parse_args(argv)
        if args.debug:
            config.debug = 1
        configure_script_logging(log)
        return args

    num_workers = 32

    def __init__(self, argv) -> None:
        super().__init__(num_workers=self.num_workers)
        self.args = self._parse_args(argv)
        self.dss_endpoint = self.args.dss_endpoint
        self.dss_src_replica = 'aws'
        self._mini_dss = None
        self._mini_dss_expiration = None
        self._mini_dss_lock = RLock()
        self.storage_client = storage.Client()
        self.src_bucket = self._get_bucket('org-hca-dss-prod')
        dst_bucket_name, self.dst_path = self._parse_destination_url()
        self.dst_bucket = self._get_bucket(dst_bucket_name)
        self.warnings = []
        self.errors = []
        _ = self.mini_dss  # Avoid lazy loading to fail early if any issue allocating client

    @property
    def mini_dss(self):
        with self._mini_dss_lock:
            if self._mini_dss is None or self._mini_dss_expiration < time.time():
                dss_client_timeout = 30 * 60  # DSS session credentials timeout after 1 hour
                log.info('Allocating new DSS client for %s to expire in %d seconds',
                         self.dss_endpoint, dss_client_timeout)
                self._mini_dss = azul.dss.MiniDSS(dss_endpoint=self.dss_endpoint,
                                                  config=Config(max_pool_connections=self.num_workers))
                self._mini_dss_expiration = time.time() + dss_client_timeout
            return self._mini_dss

    def _parse_destination_url(self) -> Tuple[str, str]:
        """
        Validate and parse the given destination URL into bucket and path values
        """
        split_url = parse.urlsplit(self.args.destination_url)
        if not split_url.scheme == 'gs' or not split_url.netloc:
            raise ValueError('Destination URL must be in gs://bucket/path format')
        else:
            path = split_url.path.strip('/')
            return split_url.netloc, f'{path}/' if path else ''

    def _get_bucket(self, bucket_name: str) -> Bucket:
        """
        Returns a reference a GC bucket
        """
        log.info('Getting bucket: %s', bucket_name)
        bucket = self.storage_client.get_bucket(bucket_name)
        return bucket

    def _run(self):
        """
        Fetch list of all bundles in DSS and iterate through them.
        """
        prefixes = self.get_prefix_list(self.args.bundle_uuid_prefix, self.args.bundle_uuid_start_prefix)

        params = {
            'replica': self.dss_src_replica,
            'per_page': 500,  # limits how many bundles per request min=10 max=500
        }
        if prefixes:
            params['prefix'] = prefixes.pop(0)
        url_base = self.dss_endpoint + '/bundles/all'
        url = url_base + '?' + parse.urlencode(params)

        # TODO: map futures to bundle uuid and extract exceptions from futures
        while True:
            log.info('Requesting list of bundles: %s', url)
            response = requests.get(url)
            response.raise_for_status()
            response_json = response.json()
            for bundle in response_json['bundles']:
                self._defer(self._process_bundle, bundle['uuid'], bundle['version'])
            if response_json.get('has_more'):
                url = response_json.get('link')
            elif prefixes:
                params['prefix'] = prefixes.pop(0)
                url = url_base + '?' + parse.urlencode(params)
            else:
                break

    def _process_bundle(self, bundle_uuid: str, bundle_version: str):
        """
        Fetch all files from the given bundle, transform content based on file
        type, and stage in GCS bucket with bucket layout-compliant object name
        """
        if self.errors:
            return
        benchmarks = []
        bundle_fqid = f'{bundle_uuid}_{bundle_version}'
        log.info('Requesting Bundle: %s', bundle_fqid)
        t1 = time.perf_counter()
        bundle = self.mini_dss.get_bundle(uuid=bundle_uuid, version=bundle_version, replica=self.dss_src_replica)
        benchmarks.append((time.perf_counter() - t1, f'to request bundle {bundle_fqid}'))

        # First loop over the manifest entries to gather needed info to modify
        # links.json and metadata files. Also use this loop to validate the
        # files before starting the upload of this bundle.
        schema_types = {}  # Mapping of file uuid to schema type
        manifest_entries = {}  # Mapping of file names to manifest entry
        file_name_re = re.compile(r'^(\w+)_\d+\.json$')  # ex. 'cell_suspension_0.json'
        t1 = time.perf_counter()
        manifest_entry: JSON
        for manifest_entry in bundle['files']:
            manifest_entries[manifest_entry['name']] = manifest_entry
            if manifest_entry['name'] == 'project_0.json':
                schema_types[manifest_entry['uuid']] = 'project'
            elif manifest_entry['name'] == 'links.json':
                schema_types[manifest_entry['uuid']] = 'links'
            elif manifest_entry['indexed']:  # Metadata files
                match = file_name_re.match(manifest_entry['name'])
                if match:
                    schema_types[manifest_entry['uuid']] = match.group(1)
                else:
                    self.log_warning(bundle_fqid,
                                     manifest_entry['name'],
                                     f"Indexed file has unknown file name format. Bundle skipped.")
                    return
            else:  # Data files
                schema_types[manifest_entry['uuid']] = 'data'
                manifest_entries[manifest_entry['name']]['new_name'] = self.data_file_new_name(
                    manifest_entry['name'],
                    manifest_entry['uuid'],
                    manifest_entry['version']
                )
        missing_files = []
        if 'project_0.json' not in manifest_entries:
            missing_files.append('project_0.json')
        if 'links.json' not in manifest_entries:
            missing_files.append('links.json')
        if missing_files:
            self.log_warning(bundle_fqid, '', f"No {' or '.join(missing_files)} found in bundle. Bundle skipped.")
            return
        if self.args.debug:
            benchmarks.append((time.perf_counter() - t1, 'to build data file name mapping'))

        # Build the new links.json before we start uploading any parts of the bundle
        blob_key = self._build_blob_key(manifest_entries['links.json'])
        file_contents = json.loads(self._download_blob_as_string(blob_key))
        try:
            new_links_json = self._build_new_links_json(file_contents, schema_types)
        except TypeError as e:
            self.log_warning(bundle_fqid, 'links.json', str(e))
            return

        # Now loop over the manifest entries to upload or copy files to the
        # staging bucket. Any error encountered during this process should end
        # the script without processing any more bundles.
        for manifest_entry in bundle['files']:
            blob_key = self._build_blob_key(manifest_entry)
            # log.info('File: %s %s', manifest_entry['name'], manifest_entry['content-type'])
            t1 = time.perf_counter()

            # links.json: upload new links.json
            if manifest_entry['name'] == 'links.json':
                new_name = f'{self.dst_path}links/' + self.links_json_new_name(bundle_uuid, bundle_version)
                try:
                    self._upload_file_contents(new_links_json, new_name, manifest_entry['content-type'])
                except:
                    self.log_error(bundle_fqid, manifest_entry['name'], 'Failed to upload file contents')
                    break
                if self.args.debug:
                    benchmarks.append((time.perf_counter() - t1, f"to upload {manifest_entry['name']}"))

            # data files: perform a bucket to bucket copy
            elif not manifest_entry['indexed']:
                new_name = f'{self.dst_path}data/' + manifest_entries[manifest_entry['name']]['new_name']
                src_blob = self.src_bucket.get_blob(f'blobs/{blob_key}')
                dst_blob = self.dst_bucket.get_blob(new_name)
                if dst_blob:
                    if src_blob.md5_hash != dst_blob.md5_hash:
                        self.log_error(bundle_fqid, manifest_entry['name'],
                                       f'Existing file mismatch {src_blob.md5_hash} != {dst_blob.md5_hash}')
                        break
                    elif self.args.debug:
                        benchmarks.append((time.perf_counter() - t1, f"to skip {manifest_entry['name']}"))
                else:
                    self._copy_file(src_blob, new_name)
                    if self.args.debug:
                        benchmarks.append((time.perf_counter() - t1, f"to copy {manifest_entry['name']}"))

            # Metadata files: modify and upload
            else:
                entity_type = manifest_entry['name'].rsplit(sep='_', maxsplit=1)[0]  # remove '_0.json' suffix
                new_name = f'{self.dst_path}metadata/' + self.metadata_file_new_name(entity_type,
                                                                                     manifest_entry['uuid'],
                                                                                     manifest_entry['version'])
                file_contents = json.loads(self.src_bucket.blob(f'blobs/{blob_key}').download_as_string())
                if entity_type.endswith('_file'):
                    try:
                        data_file_old_name = file_contents['file_core']['file_name']
                    except KeyError:
                        self.log_error(bundle_fqid, manifest_entry['name'], f"'file_core.file_name' not found in file")
                        break
                    try:
                        data_file_manifest_entry = manifest_entries[data_file_old_name]
                    except KeyError:
                        self.log_error(bundle_fqid,
                                       manifest_entry['name'],
                                       f"Unknown 'file_core.file_name' value '{data_file_old_name}'")
                        break
                    updates = {
                        'file_name': data_file_manifest_entry['new_name'],
                        'content_type': data_file_manifest_entry['content-type'].partition(';')[0],
                        'file_size': data_file_manifest_entry['size'],
                        'file_crc32c': data_file_manifest_entry['crc32c'],
                        'file_sha1': data_file_manifest_entry['sha1'],
                        'file_sha256': data_file_manifest_entry['sha256'],
                    }
                    if 'checksum' not in file_contents['file_core']:
                        updates['checksum'] = None
                    file_contents['file_core'].update(updates)
                try:
                    self._upload_file_contents(file_contents, new_name, manifest_entry['content-type'])
                except:
                    self.log_error(bundle_fqid, manifest_entry['name'], 'Failed to upload file contents')
                    break
                if self.args.debug:
                    benchmarks.append((time.perf_counter() - t1, f"to upload {manifest_entry['name']}"))

        if self.args.debug:
            total_duration = 0
            for duration, message in benchmarks:
                total_duration += duration
                log.debug(f'{round(duration, 4)} {message}')
            log.debug(f'{round(total_duration, 4)} Total')

    def _build_blob_key(self, manifest_entry: JSON) -> str:
        return '.'.join(manifest_entry[key] for key in ['sha256', 'sha1', 's3_etag', 'crc32c'])

    def _build_new_links_json(self,
                              old_links_json: JSON,
                              schema_types: Mapping[str, str]) -> JSON:
        """
        Transform the old links.json data into version 2.0.0 syntax
        """
        new_links_json = {
            'describedBy': 'https://schema.humancellatlas.org/system/2.0.0/links',
            'schema_type': 'link_bundle',
            'schema_version': '2.0.0',
            'links': [],
        }
        link: JSON
        for link in old_links_json['links']:
            new_link = {
                'process_id': link['process'],
                'process_type': schema_types[link['process']],
                'inputs': [],
                'outputs': [],
                'protocols': [],
            }
            for prop in ('input', 'output', 'protocol'):
                for value in link[f'{prop}s']:
                    if isinstance(value, str):  # a uuid
                        new_link[f'{prop}s'].append(
                            {
                                f'{prop}_type': schema_types[value],
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
                        raise TypeError(f"Unknown value type in links.json {prop} field.")
            new_links_json['links'].append(new_link)
        return new_links_json

    def _download_blob_as_string(self, blob_key) -> JSON:
        return self.src_bucket.blob(f'blobs/{blob_key}').download_as_string()

    def _upload_file_contents(self, file_contents: JSON, new_name: str, content_type: str):
        """
        Perform an upload of a dict (json file content) to a bucket
        """
        # log.info('Uploading to: %s', new_name)
        self.dst_bucket.blob(new_name).upload_from_string(data=json.dumps(file_contents, indent=4),
                                                          content_type=content_type)

    def _copy_file(self, src_blob: Blob, new_name: str):
        """
        Perform a bucket to bucket copy
        """
        # log.info('Copying to: %s', new_name)
        dst_blob = self.src_bucket.copy_blob(blob=src_blob,
                                             destination_bucket=self.dst_bucket,
                                             new_name=new_name)
        if not isinstance(dst_blob, Blob):
            raise FileNotFoundError(f'Failed to copy blob {src_blob} to new name {new_name}')

    def log_warning(self, bundle_fqid: str, file_name: str, msg: str):
        """
        Log an error message and save a copy for re-reporting at end of script
        """
        self.warnings.append((bundle_fqid, file_name, msg))
        log.error(msg)

    def log_error(self, bundle_fqid: str, file_name: str, msg: str):
        """
        Log an error message and save a copy for re-reporting at end of script
        """
        self.errors.append((bundle_fqid, file_name, msg))
        log.error(msg)

    def links_json_new_name(self, bundle_uuid: str, bundle_version: str) -> str:
        """
        Return the bucket layout compliant object name for a links.json file
        """
        return f'{bundle_uuid}_{bundle_version}.json'

    def metadata_file_new_name(self, entity_type: str, entity_id: str, version: str) -> str:
        """
        Return the bucket layout compliant object name for a metadata file
        """
        return f'{entity_type}/{entity_id}_{version}.json'

    def data_file_new_name(self, old_name: str, file_uuid: str, file_version: str) -> str:
        """
        Return the bucket layout compliant object name for a data file
        """
        old_name = old_name.replace('!', '/')  # Undo DCP v1 substitution of '/' with '!'
        assert not old_name.endswith('/')
        pos = old_name.rfind('/')
        if pos == -1:
            dir_path = ''
        else:
            dir_path, old_name = old_name[:pos + 1], old_name[pos + 1:]
        return f'{dir_path}{file_uuid}_{file_version}_{old_name}'

    def get_prefix_list(self, prefix: str = None, start_prefix: str = None):
        """
        Generate ascending hex prefixes

        >>> self.get_prefix_list(prefix='aa', start_prefix=None)
        ['aa']

        >>> self.get_prefix_list(prefix='a', start_prefix='aa')
        ['aa', 'ab', 'ac', 'ad', 'ae', 'af']

        >>> self.get_prefix_list(prefix=None, start_prefix='aa')
        ['aa', 'ab', 'ac', 'ad', 'ae', 'af', 'b', 'c', 'd', 'e', 'f']

        >>> self.get_prefix_list(prefix=None, start_prefix=None)
        ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f']
        """
        if prefix and start_prefix and not start_prefix.startswith(prefix):
            raise ValueError('Start prefix must start with prefix')
        start = start_prefix or prefix or '0'
        if prefix:
            fill_count = len(start_prefix) - len(prefix) if start_prefix else 0
            end = prefix + 'f' * fill_count
        else:
            fill_count = len(start_prefix) if start_prefix else 1
            end = 'f' * fill_count
        trim = 0
        prefixes = []
        for prefix in [hex(i)[2:] for i in range(int(start, 16), int(end, 16) + 1)]:
            # If a start_prefix was used all the prefixes here will have the
            # same length as start_prefix however this list can be trimmed down
            # example: ['be', 'bf, 'c0, 'c1'...] to ['be', 'bf', 'c', 'd'...]
            next_prefix = prefix[:-trim] if trim > 0 else prefix
            if prefixes and next_prefix[-1] == '0':
                trim += 1
                prefixes.append(next_prefix[:-1])
            elif not prefixes or next_prefix != prefixes[-1]:
                prefixes.append(next_prefix)
        return prefixes


if __name__ == '__main__':
    DSSv2Adapter(sys.argv[1:]).main()
