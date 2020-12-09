#! /usr/bin/env python3

"""
Download manifest and metadata for a given bundle from TDR and store them as
$UUID.manifest.json and $UUID.metadata.json. Note: silently overwrites the
destination file.
"""

import argparse
import json
import logging
import os
import sys

from azul import (
    config,
)
from azul.files import (
    write_file_atomically,
)
from azul.indexer import (
    Bundle,
    BundleFQID,
)
from azul.logging import (
    configure_script_logging,
)
from azul.plugins import (
    RepositoryPlugin,
)

logger = logging.getLogger(__name__)


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--catalog', '-c',
                        default='aws',
                        help="The catalog containing the bundle")
    parser.add_argument('--uuid', '-b',
                        required=True,
                        help='The UUID of the bundle to can.')
    parser.add_argument('--version', '-v',
                        help='The version of the bundle to can  (default: the latest version).')
    parser.add_argument('--output-dir', '-O',
                        default=os.path.join(config.project_root, 'test', 'indexer', 'data'),
                        help='The path to the output directory (default: %(default)s).')
    args = parser.parse_args(argv)

    fqid = BundleFQID(args.uuid, args.version)
    bundle = download_bundle(args.catalog, fqid)
    save_bundle(bundle, args.output_dir)


def download_bundle(catalog: str, fqid: BundleFQID) -> Bundle:
    repository_plugin = RepositoryPlugin.load(catalog).create(catalog)
    bundle = repository_plugin.fetch_bundle(fqid)
    logger.info('Downloaded bundle %s version %s from catalog %s.', fqid.uuid, fqid.version, catalog)
    return bundle


def save_bundle(bundle: Bundle, output_dir):
    for obj, suffix in [(bundle.manifest, ".manifest.json"),
                        (bundle.metadata_files, '.metadata.json')]:
        path = os.path.join(output_dir, bundle.uuid + suffix)
        with write_file_atomically(path) as f:
            json.dump(obj, f, indent=4)
        logger.info("Successfully wrote %s", path)


if __name__ == '__main__':
    configure_script_logging(logger)
    main(sys.argv[1:])
