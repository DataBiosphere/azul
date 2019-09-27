import argparse
import csv
from datetime import datetime
import logging
import sys
import time
from typing import Set, Tuple
from urllib.parse import urlparse, urlunparse

from botocore.config import Config
from botocore.exceptions import ClientError
from hca.util import SwaggerAPIException

from azul import config, require
from azul.dss import MiniDSS, shared_dss_credentials
from azul.logging import configure_script_logging
from azul.threads import DeferredTaskExecutor
from azul.types import MutableJSON

logger = logging.getLogger(__name__)


class CopyBundle(DeferredTaskExecutor):

    def main(self):
        if self.args.shared:
            with shared_dss_credentials():
                errors = self.run()
        else:
            errors = self.run()
        if errors:
            for e in errors:
                # S3 errors often refer to the key they occurred for, providing useful context here
                if isinstance(e, ClientError):
                    key = getattr(e, 'response', None).get('Error', {}).get('Key', None)
                    if key is None:
                        continue
                    logger.error('Error in deferred task for key %s:\n%s', key, e)
                logger.error('Error in deferred task:\n%s', e)
            raise RuntimeError(f'Some bundles or files could not be copied. '
                               f'The total number of failed tasks is {len(errors)}.', )

    @classmethod
    def _parse_args(cls, argv):
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument('--source', '-s', metavar='URL', type=urlparse,
                            default=config.dss_endpoint,
                            help='The URL of the DSS REST API from which to copy the bundles (default: %(default)s).')
        parser.add_argument('--destination', '-d', metavar='URL', type=urlparse,
                            default=config.dss_endpoint,
                            help='The URL of the DSS REST API to which to copy the bundles (default: %(default)s).')
        parser.add_argument('--personal', '-P', dest='shared', action='store_false', default=True,
                            help="Do not use the shared credentials of the Google service account that represents the "
                                 "current deployment, but instead use personal credentials for authenticating to the "
                                 "DSS. When specifying this option you will need to a) run `hca dss login` prior to "
                                 "running this script or b) set GOOGLE_APPLICATION_CREDENTIALS to point to another "
                                 "service account's credentials.")
        version = parser.add_mutually_exclusive_group()
        version.add_argument('--keep-version', '-K', dest='version', action='store_const', const='keep',
                             default='keep',
                             help="This is the default. Use the original version string for each copy of a file or "
                                  "bundle. This mode is idempotent when used together with --keep-uuid or --map-uuid.")
        version.add_argument('--set-version', '-S', metavar='VERSION', dest='version', type=cls._validate_version,
                             help=f'Set the version of bundle and file copies to the given value. This mode is '
                                  f'idempotent but it will lead to conflicts if the input contains multiple versions '
                                  f'of the same bundle or file. The version must be a string like '
                                  f'{cls._new_version()}.')
        version.add_argument('--map-version', '-M', metavar='VERSION', dest='version', type=float,
                             help='Set the version of bundle and file copies to the version of the orginal plus/minus '
                                  'the specified duration in seconds. This mode is idempotent but has a low '
                                  'probability of introducing collisions.')
        version.add_argument('--new-version', '-N', dest='version', action='store_const', const='new',
                             help='Allocate a new version for copies of bundles and files. This is not idempotent '
                                  'because it creates new files and bundles everytime the program is run.')
        parser.add_argument('--fix-tags', '-f', action='store_true', default=False,
                            help="Add checksum tags to the blob objects in the source (!) DSS if necessary.")
        input_ = parser.add_mutually_exclusive_group(required=True)
        input_.add_argument('--bundle', '-b', metavar='UUID.VERSION', nargs='+', dest='bundles',
                            help='One or more fully qualified identifiers (FQID) of bundles to be copied')
        input_.add_argument('--manifest', '-m', metavar='PATH')

        parser.add_argument('--prefix', '-p', type=str, metavar='HEX', default='',
                            help='Only copy input bundles whose UUID begins with the given string. Applied to both '
                                 '--bundles and --manifest but really only makes sense with the latter where it can '
                                 'be used copy only a deterministic subset of the bundles in the manifest.')
        parser.add_argument('--suffix', '-x', metavar='HEX', type=str, default='',
                            help='Only copy input bundles whose UUID ends in the given string. Applied to both '
                                 '--bundles and --manifest but really only makes sense with the latter where it can '
                                 'be used copy only a deterministic subset of the bundles in the manifest.')
        args = parser.parse_args(argv)
        return args

    num_workers = 32

    def __init__(self, argv) -> None:
        super().__init__(num_workers=self.num_workers)
        self.args = self._parse_args(argv)
        self.source = MiniDSS(dss_endpoint=urlunparse(self.args.source),
                              config=Config(max_pool_connections=self.num_workers))
        self.destination = self._new_dss_client()

    def _new_dss_client(self):
        return config.dss_client(dss_endpoint=urlunparse(self.args.destination),
                                 adapter_args=dict(pool_maxsize=self.num_workers))

    def _run(self):
        if self.args.bundles:
            bundle_fqids = {(uuid, version)
                            for uuid, _, version in (fqid.partition('.')
                                                     for fqid in self.args.bundles)}
        else:
            with open(self.args.manifest) as f:
                manifest = csv.DictReader(f, delimiter='\t')
                columns = {'bundle_uuid', 'file_uuid'}
                require(columns.issubset(manifest.fieldnames),
                        f'Expecting TSV with at least these columns: {columns}')
                bundle_fqids = {(row['bundle_uuid'], row['bundle_version']) for row in manifest}
        self._copy_bundles(bundle_fqids)

    def _copy_bundles(self, bundle_fqids: Set[Tuple[str, str]]):
        for bundle_fqid in bundle_fqids:
            bundle_uuid, bundle_version = bundle_fqid
            if bundle_uuid.endswith(self.args.suffix) and bundle_uuid.startswith(self.args.prefix):
                self._defer(self._copy_files, bundle_uuid, bundle_version)

    def _copy_files(self, bundle_uuid, bundle_version):
        logger.info('Getting bundle %s, version %s', bundle_uuid, bundle_version)
        manifest = self.source.get_bundle(uuid=bundle_uuid,
                                          version=bundle_version,
                                          replica='aws')
        files = manifest['files']
        logger.info('Copying %i file(s) from bundle %s, version %s',
                    len(files), bundle_uuid, bundle_version)
        file: MutableJSON
        futures = [self._defer(self._copy_file, bundle_uuid, bundle_version, file) for file in files]
        self._defer(self._copy_bundle, bundle_uuid, bundle_version, manifest, run_after=futures)

    def _copy_file(self, bundle_uuid, bundle_version, file, attempt=0):
        attempt += 1
        logger.info('Copying file %r from bundle %s, version %s', file, bundle_uuid, bundle_version)
        source_url = self.source.get_native_file_url(uuid=(file['uuid']),
                                                     version=(file['version']),
                                                     replica='aws')
        new_file = dict(uuid=file['uuid'],
                        version=(self._copy_version(file['version'])),
                        creator_uid=0,
                        source_url=source_url)
        logger.info('Creating file %r', new_file)
        try:
            # noinspection PyProtectedMember
            self.destination.put_file._request(new_file)
        except SwaggerAPIException as e:
            if e.code == 422 and e.reason == 'missing_checksum' and self.args.fix_tags and attempt < 10:
                logger.warning('Target DSS complains that source blob for file %s, version %s lacks checksum tags, '
                               'retagging in %is.', file['uuid'], file['version'], attempt)
                self.source.retag_blob(uuid=(file['uuid']),
                                       version=(file['version']),
                                       replica='aws')
                # Object tag updates are eventually consistent so the DSS might not see the tag update
                # immediately. Keep trying until it does
                self._defer(self._copy_file, bundle_uuid, bundle_version, file, attempt=attempt, delay=attempt)
            else:
                raise
        else:
            # Update the source manifest to refer to the new bundle
            file['version'] = new_file['version']

    def _copy_bundle(self, bundle_uuid, bundle_version, manifest, attempt=0):
        attempt += 1
        new_bundle_version = self._copy_version(bundle_version)
        try:
            logger.info('Creating bundle %s, version %s', bundle_uuid, new_bundle_version)
            self.destination.put_bundle(uuid=bundle_uuid,
                                        version=new_bundle_version,
                                        replica='aws',
                                        creator_uid=0,
                                        files=manifest['files'])
        except SwaggerAPIException as e:
            if e.code == 400 and e.reason == 'file_missing' and attempt < 10:
                logger.warning('Target DSS complains that a source file in bundle %s, version %s is missing, '
                               'retrying in %is.', bundle_uuid, bundle_version, attempt)
                self._defer(self._copy_bundle, bundle_uuid, bundle_version, manifest, attempt=attempt, delay=attempt)
            else:
                raise

    def _copy_version(self, version: str):
        mode = self.args.version
        if mode == 'keep':
            return version
        elif mode == 'new':
            return self._new_version()
        else:
            if isinstance(mode, float):
                version = datetime.strptime(version, self.version_format)
                version = datetime.fromtimestamp(version.timestamp() + mode)
                return version.strftime(self.version_format)
            else:
                return mode

    version_format = '%Y-%m-%dT%H%M%S.%fZ'

    @classmethod
    def _new_version(cls):
        return datetime.utcfromtimestamp(time.time()).strftime(cls.version_format)

    @classmethod
    def _validate_version(cls, version: str):
        """
        >>> # noinspection PyProtectedMember
        >>> CopyBundle._validate_version('2018-10-18T150431.370880Z')
        '2018-10-18T150431.370880Z'

        >>> # noinspection PyProtectedMember
        >>> CopyBundle._validate_version('2018-10-18T150431.0Z')
        Traceback (most recent call last):
        ...
        ValueError: ('2018-10-18T150431.0Z', '2018-10-18T150431.000000Z')

        >>> # noinspection PyProtectedMember
        >>> CopyBundle._validate_version(' 2018-10-18T150431.370880Z')
        Traceback (most recent call last):
        ...
        ValueError: time data ' 2018-10-18T150431.370880Z' does not match format '%Y-%m-%dT%H%M%S.%fZ'

        >>> # noinspection PyProtectedMember
        >>> CopyBundle._validate_version('2018-10-18T150431.370880')
        Traceback (most recent call last):
        ...
        ValueError: time data '2018-10-18T150431.370880' does not match format '%Y-%m-%dT%H%M%S.%fZ'

        >>> # noinspection PyProtectedMember
        >>> CopyBundle._validate_version('2018-10-187150431.370880Z')
        Traceback (most recent call last):
        ...
        ValueError: time data '2018-10-187150431.370880Z' does not match format '%Y-%m-%dT%H%M%S.%fZ'
        """
        reparsed_version = datetime.strptime(version, cls.version_format).strftime(cls.version_format)
        if version != reparsed_version:
            raise ValueError(version, reparsed_version)
        return version


if __name__ == '__main__':
    configure_script_logging(logger)
    CopyBundle(sys.argv[1:]).main()
