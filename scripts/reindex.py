#! /usr/bin/env python3

"""
Command line utility to trigger indexing of bundles from DSS into Azul
"""

import argparse
import json
import logging
import shutil
import sys
from typing import List

from azul.reindexer import Reindexer

logger = logging.getLogger(__name__)

defaults = Reindexer()


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
parser.add_argument('--es-query',
                    default=defaults.es_query,
                    metavar='JSON',
                    type=json.loads,
                    help='The Elasticsearch query to use against DSS to enumerate the bundles to be indexed')
parser.add_argument('--workers',
                    metavar='NUM',
                    dest='num_workers',
                    default=defaults.num_workers,
                    type=int,
                    help='The number of workers that will be sending bundles to the indexer concurrently')
group = parser.add_mutually_exclusive_group()
group.add_argument('--sync',
                   dest='sync',
                   default=False,
                   action='store_true',
                   help='Have the indexer lambda process the notification synchronously instead of queueing it for '
                        'asynchronous processing by a worker lambda.')
group.add_argument('--prefix',
                    metavar='NUM',
                   default=0,
                   type=int,
                   help='The length of the bundle UUID prefix by which to partition the set of bundles that match the '
                        'ES query. Each query partition is processed independently in a local worker thread. The '
                        'worker invokes the reindex() lambda, passing the query partition. The lambda queries the '
                        'DSS and queues a notification for each matching bundle. If 0 (the default) no partitioning '
                        'occurs, the DSS is queried locally and the indexer notification endpoint is invoked for each '
                        'bundle individually and concurrently using worker threads.')
parser.add_argument('--delete',
                    default=False,
                    action='store_true',
                    help='Delete all entity indices before reindexing.')
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

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(format="%(asctime)s %(levelname)-7s %(threadName)-7s: %(message)s", level=level)
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger('azul').setLevel(level)

    reindexer = Reindexer(indexer_url=args.indexer_url,
                          dss_url=args.dss_url,
                          es_query=args.es_query,
                          num_workers=args.num_workers,
                          dryrun=args.dryrun)
    if args.delete:
        reindexer.delete_all_indices()
    if args.prefix:
        reindexer.remote_reindex(args.prefix)
    else:
        reindexer.reindex(args.sync)


if __name__ == "__main__":
    main(sys.argv[1:])
