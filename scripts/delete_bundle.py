import sys
import argparse

import logging
import requests
from azul import config

logger = logging.getLogger(__name__)


def main(argv):
    parser = argparse.ArgumentParser(description='Delete bundles from Azul index')
    parser.add_argument('bundles',
                        metavar='bundle_uuid.bundle_version',
                        type=str,
                        nargs='+',
                        help='bundles uuid and version to be deleted')
    args = parser.parse_args(argv)
    bundles = args.bundles
    for bundle in bundles:
        try:
            bundle_uuid, bundle_version = bundle.split('.', 1)
        except ValueError:
            raise parser.parse_args(['--help'])
        else:
            delete_bundle(bundle_uuid, bundle_version)


def delete_bundle(bundle_uuid, bundle_version):
    logging.info('Deleting bundle %s.%s', bundle_uuid, bundle_version
                 )
    base_url = config.indexer_endpoint()
    delete_endpoint = f'{base_url}/delete'
    notification = {
        'match': {
            'bundle_uuid': bundle_uuid,
            'bundle_version': bundle_version
        }
    }
    response = requests.post(url=delete_endpoint,
                             json=notification)
    response.raise_for_status()


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)-7s %(threadName)-7s: %(message)s',
                        level=logging.INFO)
    main(sys.argv[1:])
