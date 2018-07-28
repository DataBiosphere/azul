# -*- coding: utf-8 -*-

"""
Command line utility to trigger indexing of bundles from DSS into Azul
"""

import argparse
from collections import defaultdict
from concurrent.futures import Future, ThreadPoolExecutor
from functools import partial
import json
import logging
from pprint import PrettyPrinter
import sys
from typing import List
from urllib.error import HTTPError
from urllib.parse import urlparse, urlencode, parse_qs
from urllib.request import Request, urlopen
from uuid import uuid4


from azul import config


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


def post_bundle(bundle_fqid, es_query, indexer_url):
    """
    Send a mock DSS notification to the indexer
    """
    bundle_uuid, _, bundle_version = bundle_fqid.partition('.')
    simulated_event = {
        "query": es_query,
        "subscription_id": str(uuid4()),
        "transaction_id": str(uuid4()),
        "match": {
            "bundle_uuid": bundle_uuid,
            "bundle_version": bundle_version
        }
    }
    body = json.dumps(simulated_event).encode('utf-8')
    request = Request(indexer_url, body)
    request.add_header("content-type", "application/json")
    with urlopen(request) as f:
        return f.read()


def main(argv: List[str]):
    args = parser.parse_args(argv)
    dss_client = config.dss_client(dss_endpoint=args.dss_url)
    # noinspection PyUnresolvedReferences
    response = dss_client.post_search.iterate(es_query=args.es_query, replica="aws")
    bundle_fqids = [r['bundle_fqid'] for r in response]
    logger.info("Bundle FQIDs to index: %i", len(bundle_fqids))

    errors = defaultdict(int)
    missing = {}
    indexed = 0
    total = 0

    with ThreadPoolExecutor(max_workers=args.num_workers, thread_name_prefix='pool') as tpe:

        def attempt(bundle_fqid, i):
            try:
                logger.info("Bundle %s, attempt %i: Sending notification", bundle_fqid, i)
                url = urlparse(args.indexer_url)
                if args.sync is not None:
                    # noinspection PyProtectedMember
                    url = url._replace(query=urlencode({**parse_qs(url.query), 'sync': args.sync}, doseq=True))
                post_bundle(bundle_fqid=bundle_fqid,
                            es_query=args.es_query,
                            indexer_url=url.geturl())
            except HTTPError as e:
                if i < 3:
                    logger.warning("Bundle %s, attempt %i: scheduling retry after error %s", bundle_fqid, i, e)
                    return bundle_fqid, tpe.submit(partial(attempt, bundle_fqid, i + 1))
                else:
                    logger.warning("Bundle %s, attempt %i: giving up after error %s", bundle_fqid, i, e)
                    return bundle_fqid, e
            else:
                logger.info("Bundle %s, attempt %i: success", bundle_fqid, i)
                return bundle_fqid, None

        def handle_future(future):
            nonlocal indexed
            # Block until future raises or succeeds
            exception = future.exception()
            if exception is None:
                bundle_fqid, result = future.result()
                if result is None:
                    indexed += 1
                elif isinstance(result, HTTPError):
                    errors[result.code] += 1
                    missing[bundle_fqid] = result.code
                elif isinstance(result, Future):
                    # The task scheduled a follow-on task, presumably a retry. Follow that new task.
                    handle_future(result)
                else:
                    assert False
            else:
                logger.warning("Unhandled exception in worker:", exc_info=exception)

        futures = []
        for bundle_fqid in bundle_fqids:
            total += 1
            futures.append(tpe.submit(partial(attempt, bundle_fqid, 0)))
        for future in futures:
            handle_future(future)

    printer = PrettyPrinter(stream=None, indent=1, width=80, depth=None, compact=False)
    logger.info("Total of bundle FQIDs read: %i", total)
    logger.info("Total of bundle FQIDs indexed: %i", indexed)
    logger.warning("Total number of errors by code:\n%s", printer.pformat(dict(errors)))
    logger.warning("Missing bundle_fqids and their error code:\n%s", printer.pformat(missing))


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s %(levelname)-7s %(threadName)-7s: %(message)s", level=logging.INFO)
    main(sys.argv[1:])
