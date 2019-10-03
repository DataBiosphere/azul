"""
Command line utility to trigger indexing of bundles from DSS into Azul
"""

import argparse
import json
import logging
import shutil
import sys
from typing import List

from azul import config
from azul.azulclient import AzulClient
from azul.logging import configure_script_logging
from azul.queues import Queues

logger = logging.getLogger(__name__)

defaults = AzulClient()


class MyFormatter(argparse.ArgumentDefaultsHelpFormatter):

    def __init__(self, prog) -> None:
        super().__init__(prog,
                         max_help_position=50,
                         width=min(shutil.get_terminal_size((80, 25)).columns, 120))


parser = argparse.ArgumentParser(description=__doc__, formatter_class=MyFormatter)
parser.add_argument('--dss-url',
                    metavar='URL',
                    default=defaults.dss_url,
                    help='The URL of the DSS aka Blue Box REST API endpoint')
parser.add_argument('--indexer-url',
                    metavar='URL',
                    default=defaults.indexer_url,
                    help="The URL of the indexer's notification endpoint to send bundles to")
group1 = parser.add_mutually_exclusive_group()
group1.add_argument('--query',
                    metavar='JSON',
                    type=json.loads,
                    help=f'The Elasticsearch query to use against DSS to enumerate the bundles to be indexed. '
                    f'The default is {defaults.query()}')
group1.add_argument('--prefix',
                    metavar='HEX',
                    default=defaults.prefix,
                    help='A bundle UUID prefix. This must be a sequence of hexadecimal characters. Only bundles whose '
                         'UUID starts with the given prefix will be indexed. If --partition-prefix-length is given, '
                         'the prefix of a partition will be appended to the prefix specified with --prefix.')
parser.add_argument('--workers',
                    metavar='NUM',
                    dest='num_workers',
                    default=defaults.num_workers,
                    type=int,
                    help='The number of workers that will be sending bundles to the indexer concurrently')
group2 = parser.add_mutually_exclusive_group()
group2.add_argument('--partition-prefix-length',
                    metavar='NUM',
                    default=0,
                    type=int,
                    help='The length of the bundle UUID prefix by which to partition the set of bundles matching the '
                         'query. Each query partition is processed independently and remotely by the indexer lambda. '
                         'The lambda queries the DSS and queues a notification for each matching bundle. If 0 (the '
                         'default) no partitioning occurs, the DSS is queried locally and the indexer notification '
                         'endpoint is invoked for each bundle individually and concurrently using worker threads. '
                         'This is magnitudes slower that partitioned indexing.')
parser.add_argument('--delete',
                    default=False,
                    action='store_true',
                    help='Delete all Azul indices in the current deployment before doing anything else.')
parser.add_argument('--index',
                    default=False,
                    action='store_true',
                    help='Index all matching bundles in the configured DSS instance.')
parser.add_argument('--purge',
                    default=False,
                    action='store_true',
                    help='Purge the queues before taking any action on the index.')
parser.add_argument('--dryrun', '--dry-run',
                    default=False,
                    action='store_true',
                    help='Just print what would be done, do not actually do it.')
parser.add_argument('--verbose',
                    default=False,
                    action='store_true',
                    help='Enable verbose logging')


def main(argv: List[str]):
    args = parser.parse_args(argv)

    if args.verbose:
        config.debug = 1

    configure_script_logging(logger)

    azul_client = AzulClient(indexer_url=args.indexer_url,
                             dss_url=args.dss_url,
                             query=args.query,
                             prefix=args.prefix,
                             num_workers=args.num_workers,
                             dryrun=args.dryrun)
    if args.purge:
        queue_manager = Queues()
        queues = dict(queue_manager.azul_queues())
        logger.info('Disabling lambdas ...')
        queue_manager.manage_lambdas(queues, enable=False)
        logger.info('Purging queues ...')
        queue_manager.purge_queues_unsafely(queues)
    else:
        queue_manager, queues = None, None
    if args.delete:
        logger.info('Deleting indices ...')
        azul_client.delete_all_indices()
    if args.purge:
        logger.info('Re-enabling lambdas ...')
        queue_manager.manage_lambdas(queues, enable=True)
    if args.index:
        logger.info('Queuing notifications for reindexing ...')
        if args.partition_prefix_length:
            azul_client.remote_reindex(args.partition_prefix_length)


if __name__ == "__main__":
    main(sys.argv[1:])
