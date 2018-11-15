#! /usr/bin/env python3

"""
Command line utility to trigger indexing of bundles from DSS into Azul
"""

import argparse
import json
import logging
import sys
from typing import List

from azul import config
from azul.es import ESClientFactory
from azul.reindexer import Reindexer

logger = logging.getLogger(__name__)

plugin = config.plugin()


class Defaults:
    dss_url = config.dss_endpoint
    indexer_url = "https://" + config.api_lambda_domain('indexer') + "/"
    es_query = plugin.dss_subscription_query
    num_workers = 16


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--dss-url',
                    default=Defaults.dss_url,
                    help='The URL of the DSS aka Blue Box REST API endpoint')
parser.add_argument('--indexer-url',
                    default=Defaults.indexer_url,
                    help="The URL of the indexer's notification endpoint to send bundles to")
parser.add_argument('--es-query',
                    default=Defaults.es_query,
                    type=json.loads,
                    help='The Elasticsearch query to use against DSS to enumerate the bundles to be indexed')
parser.add_argument('--workers',
                    dest='num_workers',
                    default=Defaults.num_workers,
                    type=int,
                    help='The number of workers that will be sending bundles to the indexer concurrently')
parser.add_argument('--sync',
                    dest='sync',
                    default=None,
                    action='store_true',
                    help='Have the indexer lambda process the notification synchronously instead of queueing it for '
                         'asynchronous processing by a worker lambda.')
parser.add_argument('--async',
                    dest='sync',
                    default=None,
                    action='store_false',
                    help='Have the indexer lambda queue the notification for asynchronous processing by a worker '
                         'lambda instead of processing it directly.')
parser.add_argument('--delete',
                    default=False,
                    action='store_true',
                    help='Delete all entity indices before reindexing.')
parser.add_argument('--dryrun', '--dry-run',
                    default=False,
                    action='store_true',
                    help='Just print what would be done, do not actually do it.')


def main(argv: List[str]):
    args = parser.parse_args(argv)

    if args.delete:
        plugin = config.plugin()
        es_client = ESClientFactory.get()
        properties = plugin.IndexProperties(dss_url=config.dss_endpoint,
                                            es_endpoint=config.es_endpoint)
        for entity_type in properties.entities:
            for aggregate in False, True:
                index_name = config.es_index_name(entity_type, aggregate=aggregate)
                if es_client.indices.exists(index_name):
                    if args.dryrun:
                        logger.info("Would delete index '%s'", index_name)
                    else:
                        es_client.indices.delete(index=index_name)

    Reindexer(indexer_url=args.indexer_url, dss_url=args.dss_url, es_query=args.es_query,
              sync=args.sync, num_workers=args.num_workers,
              dryrun=args.dryrun).reindex()


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s %(levelname)-7s %(threadName)-7s: %(message)s", level=logging.INFO)
    main(sys.argv[1:])
