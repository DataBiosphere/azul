"""
Command line utility to trigger indexing of bundles from DSS into Azul
"""

import argparse
from collections import (
    defaultdict,
)
import fnmatch
import logging
import sys

from azul import (
    config,
    require,
)
from azul.args import (
    AzulArgumentHelpFormatter,
)
from azul.azulclient import (
    AzulClient,
)
from azul.bigquery_reservation import (
    BigQueryReservation,
)
from azul.logging import (
    configure_script_logging,
)
from azul.plugins.repository.tdr import (
    TDRPlugin,
)

logger = logging.getLogger(__name__)

defaults = AzulClient()

parser = argparse.ArgumentParser(description=__doc__, formatter_class=AzulArgumentHelpFormatter)
parser.add_argument('--prefix',
                    metavar='HEX',
                    default='',
                    help='A bundle UUID prefix. This must be a sequence of hexadecimal characters. This prefix '
                         'argument will be appended to the prefix specified by the source. Only bundles whose '
                         'UUID starts with the concatenated prefix will be indexed.')
parser.add_argument('--workers',
                    metavar='NUM',
                    dest='num_workers',
                    default=defaults.num_workers,
                    type=int,
                    help='The number of workers that will be sending bundles to the indexer concurrently')
parser.add_argument('--local',
                    default=False,
                    action='store_true',
                    help='Do not offload the listing of subgraphs to the indexer Lambda function. When this option is '
                         'used, this script queries the repository without partitioning, and the indexer notification '
                         'endpoint is invoked for each subgraph individually and concurrently using worker threads. '
                         'This is magnitudes slower than remote (i.e. partitioned) indexing. If this option is not '
                         'used (the default), the set of subgraphs matching the query is partitioned using the '
                         'partition prefix length configured for each of the catalog sources being reindexed. Each '
                         'query partition is processed independently and remotely by the indexer lambda. The index '
                         'Lambda function queries the repository for each partition and queues a notification for each '
                         'matching subgraph in the partition.')
parser.add_argument('--catalogs',
                    nargs='+',
                    metavar='NAME',
                    default=[
                        c for c in config.catalogs
                        if c not in config.integration_test_catalogs
                    ],
                    choices=config.catalogs,
                    help='The names of the catalogs to reindex.')
parser.add_argument('--sources',
                    default=config.reindex_sources,
                    nargs='+',
                    help='Limit reindexing to a subset of the configured sources. '
                         'Supports shell-style wildcards to match multiple sources per argument. '
                         'Must be * for local reindexing i.e., if --local is given.')
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
parser.add_argument('--deindex',
                    default=False,
                    action='store_true',
                    help='Delete all documents in the current deployment that match '
                         'the specified sources. '
                         'Incompatible with --index, --create, and --delete. '
                         'Do not run while indexing is ongoing.')
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
parser.add_argument('--no-slots',
                    dest='manage_slots',
                    default=True,
                    action='store_false',
                    help='Suppress management of BigQuery slot commitments.')


def main(argv: list[str]):
    args = parser.parse_args(argv)

    if args.verbose:
        config.debug = 1

    configure_script_logging(logger)

    azul = AzulClient(num_workers=args.num_workers)

    source_globs = set(args.sources)
    if not args.local or args.deindex:
        sources_by_catalog = defaultdict(set)
        globs_matched = set()
        for catalog in args.catalogs:
            sources = azul.catalog_sources(catalog)
            for source_glob in source_globs:
                matches = fnmatch.filter(sources, source_glob)
                if matches:
                    globs_matched.add(source_glob)
                logger.debug('Source glob %r matched sources %r in catalog %r',
                             source_glob, matches, catalog)
                sources_by_catalog[catalog].update(matches)
        unmatched = source_globs - globs_matched
        if unmatched:
            logger.warning('Source(s) not found in any catalog: %r', unmatched)
        require(any(sources_by_catalog.values()),
                'No valid sources specified for any catalog')
    else:
        if source_globs == {'*'}:
            sources_by_catalog = {
                catalog: azul.catalog_sources(catalog)
                for catalog in args.catalogs
            }
        else:
            parser.error('Cannot specify sources when performing a local reindex')
            assert False

    if args.deindex:
        require(not any((args.index, args.delete, args.create)),
                '--deindex is incompatible with --index, --create, and --delete.')
        require('*' not in source_globs, '--deindex is incompatible with source `*`. '
                                         'Use --delete instead.')

        for catalog, sources in sources_by_catalog.items():
            if sources:
                azul.deindex(catalog, sources)

    azul.reset_indexer(args.catalogs,
                       purge_queues=args.purge,
                       delete_indices=args.delete,
                       create_indices=args.create or args.index and args.delete)

    if args.index:
        logger.info('Queuing notifications for reindexing ...')
        reservation = None
        num_notifications = 0
        for catalog, sources in sources_by_catalog.items():
            if sources:
                if (
                    args.manage_slots
                    and reservation is None
                    and isinstance(azul.repository_plugin(catalog), TDRPlugin)
                ):
                    reservation = BigQueryReservation()
                    reservation.activate()
                if not args.local:
                    azul.remote_reindex(catalog, sources)
                    num_notifications = None
                else:
                    num_notifications += azul.reindex(catalog, args.prefix)
            else:
                logger.info('Skipping catalog %r (no matching sources)', catalog)
        if args.wait:
            if num_notifications == 0:
                logger.warning('No notifications for prefix %r and catalogs %r were sent',
                               args.prefix, args.catalogs)
            else:
                azul.wait_for_indexer()


if __name__ == '__main__':
    main(sys.argv[1:])
