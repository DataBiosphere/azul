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
        parser.add_argument('--destination-path-prefix', '-p',
                            required=True,
                            help='Path prefix to use in the destination bucket')
        parser.add_argument('--bundle-uuid-prefix', '-b',
                            default='',
                            help='Copy bundles only with given prefix.')
        parser.add_argument('--bundle-uuid-start-prefix', '-s',
                            default='',
                            help='Copy all bundles including and after given '
                                 'starting prefix. Max length of 8 characters.')
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
        self.dst_bucket = self._get_bucket('tdr_test_staging_bucket')
        self.dst_path_prefix = self.args.destination_path_prefix.rstrip('/')
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
        type, and stage in GC bucket with bucket layout-compliant object name
        """
        log.info('----')
        log.info('Requesting Bundle: %s', bundle_uuid)
        bundle = self.mini_dss.get_bundle(uuid=bundle_uuid, version=bundle_version, replica=self.dss_src_replica)
        # Loop over the file summaries to build a mapping between data file
        # names (eg. 'SRR5174704_1.fastq.gz') and the constructed object name
        # that will later be inserted into the 'file_core.file_name' property
        # of the 'metadata/file' type file describing the data file. Also use
        # this loop to do validation on the files before this bundle is copied.
        found_links_json = False
        file_name_re = re.compile(r'^\w+_\d+\.json$')
        data_file_object_names = {}
        for manifest_entry in bundle['files']:
            # links.json
            if manifest_entry['name'] == 'links.json':
                found_links_json = True
            # Metadata files
            elif manifest_entry['indexed']:
                if not file_name_re.match(manifest_entry['name']):
                    self.log_error(bundle_uuid,
                                   manifest_entry['name'],
                                   f"Indexed file has unknown file name format. Bundle skipped.")
                    return
            # Data files
            else:
                data_file_object_names[manifest_entry['name']] = self.data_file_object_name(manifest_entry['name'],
                                                                                            manifest_entry['uuid'],
                                                                                            manifest_entry['version'])
        if not found_links_json:
            self.log_error(bundle_uuid, '', f"No links.json file found in bundle. Bundle skipped.")
            return

        # Now loop over all file summaries to fetch the file content, process,
        # and upload files to staging bucket
        manifest_entry: Mapping[str, Union[str, int, bool]]
        for manifest_entry in bundle['files']:
            log.info('File: %s %s', manifest_entry['name'], manifest_entry['content-type'])
            new_name = self.dst_path_prefix + '/' if self.dst_path_prefix else ''
            blob_key = '.'.join(manifest_entry[key] for key in ['sha256', 'sha1', 's3_etag', 'crc32c'])

            # links.json
            if manifest_entry['name'] == 'links.json':
                new_name += f'links/{bundle_uuid}_{bundle_version}.json'
                results = self.copy_file(blob_key, new_name)

            # Metadata files
            elif manifest_entry['indexed']:
                entity_type = manifest_entry['name'].rsplit(sep='_', maxsplit=1)[0]  # remove '_0.json' from end of name
                entity_id = manifest_entry['uuid']
                version = manifest_entry['version']
                new_name += f'metadata/{entity_type}/{entity_id}_{version}.json'
                if entity_type.endswith('_file'):
                    # Fetch file contents, modify, and upload to bucket
                    file_contents = self.mini_dss.get_file(uuid=entity_id, version=version, replica='aws')
                    try:
                        object_name = data_file_object_names.get(file_contents['file_core']['file_name'])
                    except KeyError:
                        self.log_error(bundle_uuid,
                                       manifest_entry['name'],
                                       f"Missing object name for {file_contents['file_core']['file_name']}")
                        break
                    log.info('Updating file_core.file_name to %s', object_name)
                    file_contents['file_core']['file_name'] = object_name
                    results = self.upload_file_contents(file_contents, new_name, manifest_entry['content-type'])
                else:
                    # Perform bucket to bucket copy
                    results = self.copy_file(blob_key, new_name)

            # Data files
            else:
                new_name += 'data/' + data_file_object_names[manifest_entry['name']]
                results = self.copy_file(blob_key, new_name)

            if not isinstance(results, Blob):
                self.log_error(bundle_uuid, manifest_entry['name'], f"Failed to copy file to {new_name}")

    def upload_file_contents(self, file_contents: Mapping[str, Any], new_name: str, content_type: str) -> Blob:
        """
        Perform an upload of a dict (json file content) to a bucket
        """
        log.info('Uploading to: %s', new_name)
        self.dst_bucket.blob(new_name).upload_from_string(data=json.dumps(file_contents, indent=4),
                                                          content_type=content_type)
        return self.dst_bucket.get_blob(new_name)

    def copy_file(self, blob_key: str, new_name: str) -> Blob:
        """
        Perform a bucket to bucket copy
        """
        log.info('Copying to: %s', new_name)
        src_blob = self.src_bucket.blob(f'blobs/{blob_key}')
        return self.src_bucket.copy_blob(blob=src_blob,
                                         destination_bucket=self.dst_bucket,
                                         new_name=new_name)

    def log_error(self, bundle_uuid: str, file_name: str, msg: str):
        """
        Log an error message and save a copy for re-reporting at end of script
        """
        self.errors.append((bundle_uuid, file_name, msg))
        log.error(msg)

    @classmethod
    def data_file_object_name(cls, file_name: str, file_uuid: str, file_version: str):
        """
        Generate the bucket layout compliant object name for a data file
        """
        file_name = file_name.replace('!', '/')  # Undo DCP v1 substitution of '/' with '!'
        assert not file_name.endswith('/')
        pos = file_name.rfind('/')
        if pos == -1:
            dir_path = ''
        else:
            dir_path, file_name = file_name[:pos + 1], file_name[pos + 1:]
        return f'{dir_path}{file_uuid}_{file_version}_{file_name}'

    @classmethod
    def ascending_prefixes(cls, prefix: str):
        """
        Generates ascending hex prefixes starting with given value.

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
