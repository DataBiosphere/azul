"""
Calculate the SHA-256 of Google Cloud Storage one or more blobs and write the
result as custom metadata to each blob.
"""
import argparse
import base64
import hashlib
import logging
import os
import sys
import tempfile
from typing import (
    List,
    Tuple,
)
from urllib import (
    parse,
)

# PyCharm doesn't seem to recognize PEP 420 namespace packages
# noinspection PyPackageRequirements
import google.cloud.storage as gcs

from azul import (
    reject,
    require,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)


class WriteCustomMetadata:

    def main(self):
        self._run()
        exit_code = 0
        return exit_code

    @classmethod
    def _parse_args(cls, argv):
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument('--source-area', '-s',
                            required=True,
                            help='The Google Cloud Storage URL of the source area. '
                                 'Syntax is gs://<bucket>[/<path>].')
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument('--blob-path', '-b',
                           action='append',
                           help='The path of a blob object relative to the source area. '
                                'Can be specified multiple times.')
        group.add_argument('--all-blobs', '-B',
                           action='store_true', default=False,
                           help='Process all blobs contained within the source area')
        parser.add_argument('--force', '-f',
                            action='store_true', default=False,
                            help='Force calculation of SHA256 if blob has an existing '
                                 'custom metadata value and overwrite if different.')
        args = parser.parse_args(argv)
        return args

    def __init__(self, argv: List[str]) -> None:
        super().__init__()
        self.args = self._parse_args(argv)
        self.gcs = gcs.Client()
        self.src_bucket, self.src_path = self._parse_gcs_url(self.args.source_area)

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
        Process each blob path given
        """
        for blob in self.iterate_blobs():
            logging.info(f'Processing {blob.name}')
            self.write_blob_sha256(blob, self.args.force)

    def iterate_blobs(self):
        if self.args.all_blobs:
            for blob in self.src_bucket.list_blobs(prefix=self.src_path):
                yield blob
        else:
            for blob_path in self.args.blob_path:
                yield self.get_blob(blob_path)

    def write_blob_sha256(self, blob: gcs.Blob, force: bool = False) -> None:
        """
        Calculates a blob's SHA256 and writes the value to the blob's custom
        metadata 'sha256' field.
        """
        current_value = None if blob.metadata is None else blob.metadata.get('sha256')
        logging.info(f'Current SHA256 value {current_value!r}')
        if current_value is None or force:
            file_sha256 = self.calculate_blob_sha256(blob)
            if current_value == file_sha256:
                logging.info('Calculated SHA256 matches current value, no change.')
            else:
                logging.info(f'Saving SHA256 value {file_sha256!r}')
                blob.metadata = {'sha256': file_sha256}
                blob.patch()
        else:
            logging.info('Blob SHA256 not calculated or changed.')

    def get_blob(self, blob_path: str) -> gcs.Blob:
        """
        Return the blob from the source bucket.
        """
        return self.src_bucket.get_blob(f'{self.src_path}{blob_path}')

    def calculate_blob_sha256(self,
                              blob: gcs.Blob) -> str:
        """
        Return the SHA256 for the given blob.
        To calculate the value the file is downloaded to a temporary file that
        is deleted after the hash is calculated.
        """
        file = tempfile.NamedTemporaryFile(mode='w+b', delete=False)
        file_name = file.name
        try:
            blob.download_to_file(file)
        finally:
            file.close()
        with open(file_name, 'rb') as file:
            file_md5 = hashlib.md5()
            file_sha256 = hashlib.sha256()
            while chunk := file.read(8192):
                file_md5.update(chunk)
                file_sha256.update(chunk)
        os.unlink(file_name)
        # The MD5 hash stored in blob object metadata is base64 encoded
        file_md5 = base64.b64encode(file_md5.digest()).decode()
        if blob.md5_hash != file_md5:
            raise Exception(f'Blob {blob.name} MD5 mismatch', blob.md5_hash, file_md5)
        # Return SHA256 as 64 character hex string
        return file_sha256.hexdigest()


if __name__ == '__main__':
    configure_script_logging(log)
    adapter = WriteCustomMetadata(sys.argv[1:])
    sys.exit(adapter.main())
