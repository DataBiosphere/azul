"""
Copies data and metadata files from a source DSS to a v2 staging bucket.
"""

import argparse
import logging
import json
import re
import requests
import sys
import time
from typing import (
    Any,
    Mapping,
    Union,
)
from urllib.parse import urlencode

from google.cloud import storage
from google.cloud.storage import Blob

from azul import config
import azul.dss
from azul.logging import configure_script_logging

log = logging.getLogger(__name__)


class DSSv2Adapter:

    def main(self):
        try:
            self.process_bundles()
        except KeyboardInterrupt:
            log.info('Caught KeyboardInterrupt. Exiting ... ')
        log.info('Total errors encountered: %i', len(self.errors))
        for error in self.errors:
            log.error(error)

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
                            help='Copy all bundles including and after given '
                                 'start prefix. Max length of 8 characters.')
        parser.add_argument('--debug',
                            action='store_true',
                            help='Output benchmarking debug results.')
        args = parser.parse_args(argv)
        return args

    def __init__(self, argv) -> None:
        super().__init__()
        self.args = self._parse_args(argv)
        self.dss_endpoint = self.args.dss_endpoint
        self.dss_src_replica = 'aws'
        self._mini_dss = None
        self._mini_dss_expiration = None
        self.storage_client = storage.Client()
        self.src_bucket = self._get_bucket('org-hca-dss-prod')
        self.dst_bucket, self.dst_path = self._parse_destination_url()
        self.errors = []
        _ = self.mini_dss  # Avoid lazy loading to fail early if any issue allocating client

    @property
    def mini_dss(self):
        if self._mini_dss is None or self._mini_dss_expiration < time.time():
            dss_client_timeout = 30 * 60  # DSS session credentials timeout after 1 hour
            log.info('Allocating new DSS client for %s to expire in %d seconds', self.dss_endpoint, dss_client_timeout)
            self._mini_dss = azul.dss.MiniDSS(dss_endpoint=self.dss_endpoint)
            self._mini_dss_expiration = time.time() + dss_client_timeout
        return self._mini_dss

    def _parse_destination_url(self):
        """
        Validate and parse the given destination URL into bucket and path values
        """
        url = self.args.destination_url
        if not url.startswith('gs://') or url == 'gs://':
            raise ValueError('Destination URL did not match format gs://bucket/path')
        else:
            parts = url[5:].split('/', maxsplit=1)
            if len(parts) == 2 and parts[1]:
                return self._get_bucket(parts[0]), parts[1].rstrip('/') + '/'
            else:
                return self._get_bucket(parts[0]), ''

    def _get_bucket(self, bucket_name: str):
        """
        Returns a reference a GC bucket
        """
        log.info('Getting bucket: %s', bucket_name)
        bucket = self.storage_client.get_bucket(bucket_name)
        return bucket

    def process_bundles(self):
        """
        Fetch list of all bundles in DSS and iterate through them.
        """
        prefixes = None
        if self.args.bundle_uuid_prefix:
            prefixes = [self.args.bundle_uuid_prefix]
        elif self.args.bundle_uuid_start_prefix:
            prefixes = list(self.ascending_prefixes(self.args.bundle_uuid_start_prefix))

        params = {
            'replica': self.dss_src_replica,
            'per_page': 100,  # limits how many bundles per request min=10 max=500
        }
        if prefixes:
            params['prefix'] = prefixes.pop(0)
        url_base = self.dss_endpoint + '/bundles/all'
        url = url_base + '?' + urlencode(params)

        while True:
            log.info('Requesting Bundle List: %s', url)
            response = requests.get(url)
            response.raise_for_status()
            response_json = response.json()
            for bundle in response_json['bundles']:
                self.process_bundle(bundle['uuid'], bundle['version'])
            if response_json.get('has_more'):
                url = response_json.get('link')
            elif prefixes:
                params['prefix'] = prefixes.pop(0)
                url = url_base + '?' + urlencode(params)
            else:
                break

    def process_bundle(self, bundle_uuid: str, bundle_version: str):
        """
        Fetch all files from the given bundle, transform content based on file
        type, and stage in GCS bucket with bucket layout-compliant object name
        """
        benchmarks = []
        log.info('----')
        log.info('Requesting Bundle: %s', bundle_uuid)
        t1 = time.perf_counter()
        bundle = self.mini_dss.get_bundle(uuid=bundle_uuid, version=bundle_version, replica=self.dss_src_replica)
        benchmarks.append((time.perf_counter() - t1, 'to request bundle'))

        # Loop over the manifest entries to build a mapping of data file names
        # (eg. 'SRR5174704_1.fastq.gz') to the file's new name. This is needed
        # so we can insert the new name in the 'file_core.file_name' property
        # of the 'metadata/file' type file describing the data file. Also use
        # this loop to do validation on the files before this bundle is copied.
        found_project_json = False
        found_links_json = False
        file_name_re = re.compile(r'^\w+_\d+\.json$')
        data_file_new_names = {}
        t1 = time.perf_counter()
        manifest_entry: Mapping[str, Union[str, int, bool]]
        for manifest_entry in bundle['files']:
            if manifest_entry['name'] == 'project_0.json':
                found_project_json = True
            elif manifest_entry['name'] == 'links.json':
                found_links_json = True
            elif manifest_entry['indexed']:  # Metadata files
                if not file_name_re.match(manifest_entry['name']):
                    self.log_error(bundle_uuid,
                                   manifest_entry['name'],
                                   f"Indexed file has unknown file name format. Bundle skipped.")
                    return
            else:  # Data files
                data_file_new_names[manifest_entry['name']] = self.data_file_new_name(manifest_entry['name'],
                                                                                      manifest_entry['uuid'],
                                                                                      manifest_entry['version'])
        missing_files = []
        if not found_project_json:
            missing_files.append('project_0.json')
        if not found_links_json:
            missing_files.append('links.json')
        if missing_files:
            self.log_error(bundle_uuid, '', f"No {' or '.join(missing_files)} found in bundle. Bundle skipped.")
            return
        if self.args.debug:
            benchmarks.append((time.perf_counter() - t1, 'to build data file name mapping'))

        # Loop over the manifest entries to copy / upload files to staging bucket
        for manifest_entry in bundle['files']:
            result = False
            new_name = None
            blob_key = '.'.join(manifest_entry[key] for key in ['sha256', 'sha1', 's3_etag', 'crc32c'])
            log.info('File: %s %s', manifest_entry['name'], manifest_entry['content-type'])
            t1 = time.perf_counter()

            # links.json & data files get copied without modification
            if manifest_entry['name'] == 'links.json' or not manifest_entry['indexed']:
                if manifest_entry['name'] == 'links.json':
                    new_name = f'{self.dst_path}links/' + self.links_json_new_name(bundle_uuid, bundle_version)
                else:
                    new_name = f'{self.dst_path}data/' + data_file_new_names[manifest_entry['name']]
                if self.dst_bucket.blob(new_name).exists():
                    result = True
                    if self.args.debug:
                        benchmarks.append((time.perf_counter() - t1, f"to skip {manifest_entry['name']}"))
                else:
                    result = self.copy_file(blob_key, new_name)
                    if self.args.debug:
                        benchmarks.append((time.perf_counter() - t1, f"to copy {manifest_entry['name']}"))

            # Metadata files are either modified and uploaded or copied
            else:
                entity_type = manifest_entry['name'].rsplit(sep='_', maxsplit=1)[0]  # remove '_0.json' suffix
                new_name = f'{self.dst_path}metadata/' + self.metadata_file_new_name(entity_type,
                                                                                     manifest_entry['uuid'],
                                                                                     manifest_entry['version'])
                if self.dst_bucket.blob(new_name).exists():
                    result = True
                    if self.args.debug:
                        benchmarks.append((time.perf_counter() - t1, f"to skip {manifest_entry['name']}"))
                elif entity_type.endswith('_file'):
                    # Fetch file contents, modify, and upload to bucket
                    file_contents = json.loads(self.src_bucket.blob(f'blobs/{blob_key}').download_as_string())
                    try:
                        data_file_old_name = file_contents['file_core']['file_name']
                    except KeyError:
                        self.log_error(bundle_uuid, manifest_entry['name'], f"'file_core.file_name' not found in file")
                        break
                    try:
                        data_file_new_name = data_file_new_names[data_file_old_name]
                    except KeyError:
                        self.log_error(bundle_uuid, manifest_entry['name'], f"Unknown 'file_core.file_name' value")
                        break
                    file_contents['file_core']['file_name'] = data_file_new_name
                    result = self.upload_file_contents(file_contents, new_name, manifest_entry['content-type'])
                    if self.args.debug:
                        benchmarks.append((time.perf_counter() - t1, f"to upload {manifest_entry['name']}"))

                else:
                    result = self.copy_file(blob_key, new_name)
                    if self.args.debug:
                        benchmarks.append((time.perf_counter() - t1, f"to copy {manifest_entry['name']}"))

            if not result:
                self.log_error(bundle_uuid, manifest_entry['name'], f"Failed to copy file to {new_name}")

        if self.args.debug:
            total_duration = 0
            for duration, message in benchmarks:
                total_duration += duration
                log.debug(f'{round(duration, 4)} {message}')
            log.debug(f'{round(total_duration, 4)} Total')

    def upload_file_contents(self, file_contents: Mapping[str, Any], new_name: str, content_type: str) -> Blob:
        """
        Perform an upload of a dict (json file content) to a bucket
        """
        log.info('Uploading to: %s', new_name)
        self.dst_bucket.blob(new_name).upload_from_string(data=json.dumps(file_contents, indent=4),
                                                          content_type=content_type)
        return self.dst_bucket.blob(new_name).exists()

    def copy_file(self, blob_key: str, new_name: str) -> bool:
        """
        Perform a bucket to bucket copy
        """
        log.info('Copying to: %s', new_name)
        src_blob = self.src_bucket.blob(f'blobs/{blob_key}')
        dst_blob = self.src_bucket.copy_blob(blob=src_blob,
                                             destination_bucket=self.dst_bucket,
                                             new_name=new_name)
        return isinstance(dst_blob, Blob)

    def log_error(self, bundle_uuid: str, file_name: str, msg: str):
        """
        Log an error message and save a copy for re-reporting at end of script
        """
        self.errors.append((bundle_uuid, file_name, msg))
        log.error(msg)

    @classmethod
    def links_json_new_name(cls, bundle_uuid: str, bundle_version: str) -> str:
        """
        Return the bucket layout compliant object name for a links.json file
        """
        return f'{bundle_uuid}_{bundle_version}.json'

    @classmethod
    def metadata_file_new_name(cls, entity_type: str, entity_id: str, version: str) -> str:
        """
        Return the bucket layout compliant object name for a metadata file
        """
        return f'{entity_type}/{entity_id}_{version}.json'

    @classmethod
    def data_file_new_name(cls, old_name: str, file_uuid: str, file_version: str) -> str:
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

    @classmethod
    def ascending_prefixes(cls, prefix: str):
        """
        Generate ascending hex prefixes starting with given value.

        >>> list(cls.ascending_prefixes('7'))
        ['7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f']

        >>> list(cls.ascending_prefixes('aaa'))
        ['aaa', 'aab', 'aac', 'aad', 'aae', 'aaf', 'ab', 'ac', 'ad', 'ae', 'af', 'b', 'c', 'd', 'e', 'f']
        """
        chars = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f']
        while True:
            char = prefix[-1]
            loc = chars.index(char)
            while loc < 16:
                prefix = prefix[:-1] + chars[loc]
                yield prefix
                loc += 1
            while prefix and prefix[-1] == chars[-1]:
                prefix = prefix[:-1]
            if len(prefix) == 0:
                break
            loc = chars.index(prefix[-1])
            prefix = prefix[:-1] + chars[loc + 1]


if __name__ == '__main__':
    configure_script_logging(log)
    DSSv2Adapter(sys.argv[1:]).main()
