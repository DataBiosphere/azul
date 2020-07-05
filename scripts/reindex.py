"""
Command line utility to trigger indexing of bundles from DSS into Azul
"""

import argparse
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


def my_formatter(prog: str):
    # This should be a subclass of ArgumentDefaultsHelpFormatter instead of a
    # factory function but doing so causes a false type check warning in PyCharm
    # because it uses a typeshed stub for argparse which maybe buggy or
    # something PyCharm doesn't understand.
    return argparse.ArgumentDefaultsHelpFormatter(prog,
                                                  max_help_position=50,
                                                  width=min(shutil.get_terminal_size((80, 25)).columns, 120))


parser = argparse.ArgumentParser(description=__doc__, formatter_class=my_formatter)
parser.add_argument('--prefix',
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
parser.add_argument('--partition-prefix-length',
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
                    help='Delete all Elasticsearch indices in the current deployment. '
                         'Implies --create when combined with --index.')
parser.add_argument('--index',
                    default=False,
                    action='store_true',
                    help='Index all matching metadata in the configured repository. '
                         'Implies --create when combined with --delete.')
parser.add_argument('--create',
                    default=False,
                    action='store_true',
                    help='Create all Elasticsearch indices in the current deployment. '
                         'Implied when --delete and --index are given.')
parser.add_argument('--purge',
                    default=False,
                    action='store_true',
                    help='Purge the queues before taking any action on the indices.')
parser.add_argument('--nowait', '--no-wait',
                    dest='wait',
                    default=True,
                    action='store_false',
                    help="Don't wait for queues to empty before exiting script.")
parser.add_argument('--verbose',
                    default=False,
                    action='store_true',
                    help='Enable verbose logging')


def main(argv: List[str]):
    args = parser.parse_args(argv)

    if args.verbose:
        config.debug = 1

    configure_script_logging(logger)

    azul_client = AzulClient(prefix=args.prefix,
                             num_workers=args.num_workers)
    queues = Queues()
    work_queues = queues.get_queues(config.work_queue_names)

    if args.purge:
        logger.info('Disabling lambdas ...')
        queues.manage_lambdas(work_queues, enable=False)
        logger.info('Purging queues: %s', ', '.join(work_queues.keys()))
        queues.purge_queues_unsafely(work_queues)

    if args.delete:
        logger.info('Deleting indices ...')
        azul_client.delete_all_indices()

    if args.purge:
        logger.info('Re-enabling lambdas ...')
        queues.manage_lambdas(work_queues, enable=True)

    if args.create or args.index and args.delete:
        logger.info('Creating indices ...')
        azul_client.create_all_indices()

    if args.index:
        logger.info('Queuing notifications for reindexing ...')
        if args.partition_prefix_length:
            azul_client.remote_reindex(args.partition_prefix_length)
        else:
            azul_client.reindex()
        if args.wait:
            # Total wait time for queues must be less than timeout in `.gitlab-ci.yml`
            queues.wait_for_queue_level(empty=False)
            queues.wait_for_queue_level(empty=True,
                                        min_timeout=10 * 60 if config.dss_query_prefix else None)


if __name__ == "__main__":
    main(sys.argv[1:])
