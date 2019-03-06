import sys
import argparse

import logging
import requests
from azul import config

logger = logging.getLogger(__name__)


def main(argv):
    parser = argparse.ArgumentParser(description='Delete bundles from Azul index.')
    parser.add_argument('bundles',
                        metavar='UUID.VERSION',
                        type=parse_fqid,
                        nargs='+',
                        help='One or more references of the bundles to be deleted.')
    args = parser.parse_args(argv)
    bundles = args.bundles
    for bundle in bundles:
        try:
            bundle_uuid, bundle_version = bundle
        except ValueError:
            parser.parse_args(['--help'])
        else:
            delete_bundle(bundle_uuid, bundle_version)


def delete_bundle(bundle_uuid, bundle_version):
    logging.info('Deleting bundle %s.%s', bundle_uuid, bundle_version)
    base_url = config.indexer_endpoint()
    deletion_endpoint = f'{base_url}/delete'
    notification = {
        'match': {
            'bundle_uuid': bundle_uuid,
            'bundle_version': bundle_version
        }
    }
    response = requests.post(url=deletion_endpoint,
                             json=notification)
    response.raise_for_status()


def parse_fqid(s: str):
    uuid, _, version = s.partition('.')
    return uuid, version


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)-7s %(threadName)-7s: %(message)s',
                        level=logging.INFO)
    main(sys.argv[1:])
