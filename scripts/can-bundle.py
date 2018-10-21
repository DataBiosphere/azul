#! /usr/bin/env python3

"""
Download manifest and metadata for a given bundle from DSS and store them as $UUID.manifest.json and
$UUID.metadata.json. Note: silently overwrites the destination file.
"""

import json
import logging
import os
import sys

import argparse
import tempfile

from humancellatlas.data.metadata.api import Bundle
from humancellatlas.data.metadata.helpers.dss import download_bundle_metadata
from humancellatlas.data.metadata.helpers.json import as_json

from azul import config
from azul.types import JSON

logger = logging.getLogger(__name__)


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dss-url', '-u',
                        default=config.dss_endpoint,
                        help='The URL of the DSS REST API endpoint from which to download the bundle to be canned '
                             '(default: %(default)s).')
    parser.add_argument('--replica', '-r',
                        default='aws',
                        help="The replica from which to donwload the bundle to be canned (default: %(default)s).")
    parser.add_argument('--uuid', '-b',
                        required=True,
                        help='The UUID of the bundle to can.')
    parser.add_argument('--version', '-v',
                        help='The version of the bundle to can  (default: the latest version).')
    parser.add_argument('--output-dir', '-O',
                        default=os.path.join(config.project_root, 'test', 'indexer', 'data'),
                        help='The path to the output directory (default: %(default)s).')
    parser.add_argument('--api-json', '-A',
                        default=False,
                        action='store_true',
                        help="Dump the return value of metadata-api's as_json function (default off).")
    args = parser.parse_args(argv)

    dss_client = config.dss_client(dss_endpoint=args.dss_url)
    version, manifest, metadata_files = download_bundle_metadata(client=dss_client,
                                                                 replica=args.replica,
                                                                 uuid=args.uuid,
                                                                 version=args.version)
    logger.info('Downloaded bundle %s version %s from replica %s.', args.uuid, version, args.replica)

    api_json = as_json(Bundle(args.uuid, version, manifest, metadata_files)) if args.api_json else None

    for obj, suffix in [(manifest, ".manifest.json"),
                        (metadata_files, '.metadata.json'),
                        *([(api_json, ".api.json")] if api_json else [])]:
        json_dump_atomically(os.path.join(args.output_dir, args.uuid + suffix), obj)


def json_dump_atomically(path, obj: JSON):
    dir_path, file_name = os.path.split(path)
    fd, temp_path = tempfile.mkstemp(dir=dir_path)
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(obj, f, indent=4)
        os.rename(temp_path, path)
        logger.info("Successfully wrote %s", path)
    except:
        os.unlink(temp_path)
        raise


if __name__ == '__main__':
    logging.basicConfig(format="%(asctime)s %(levelname)-7s %(threadName)-7s: %(message)s", level=logging.INFO)
    main(sys.argv[1:])
