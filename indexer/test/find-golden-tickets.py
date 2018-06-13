# -*- coding: utf-8 -*-

"""
Command line utility to trigger indexing of bundles based on a query
"""

import argparse
from collections import defaultdict
import json
import os
from pprint import pprint
import sys
from time import sleep
from typing import List
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

from hca.dss import DSSClient

from utils.deployment import aws


class Defaults:
    dss_url = os.environ['AZUL_DSS_ENDPOINT']
    indexer_url = aws.api_getway_endpoint(function_name=os.environ['AZUL_INDEXER_NAME'],
                                          api_gateway_stage=os.environ['AZUL_DEPLOYMENT_STAGE'])
    es_query = {"query": {"match_all": {}}}


parser = argparse.ArgumentParser(description='Process options the finder of golden bundles.')
parser.add_argument('--dss-url',
                    dest='dss_url',
                    action='store',
                    default=Defaults.dss_url,
                    help='The url for the storage system.')
parser.add_argument('--indexer-url',
                    dest='indexer_url',
                    action='store',
                    default=Defaults.indexer_url,
                    help='The indexer URL')
parser.add_argument('--es-query',
                    dest='es_query',
                    action='store',
                    default=Defaults.es_query,
                    type=json.loads,
                    help='The ElasticSearch query to use')


def post_bundle(bundle_fqid, es_query, indexer_url):
    """
    Send a fake BlueBox notification to the indexer
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
    """
    Entrypoint method for the script
    """
    args = parser.parse_args(argv)
    dss_client = DSSClient()
    dss_client.host = args.dss_url
    # noinspection PyUnresolvedReferences
    response = dss_client.post_search.iterate(es_query=args.es_query, replica="aws")
    bundle_fqids = [r['bundle_fqid'] for r in response]
    errors = defaultdict(int)
    missing = {}
    indexed = 0
    total = 0
    for bundle_fqid in bundle_fqids:
        total += 1
        print(f"Bundle: {bundle_fqid}")
        retries = 3
        while True:
            try:
                post_bundle(bundle_fqid=bundle_fqid,
                            es_query=args.es_query,
                            indexer_url=args.indexer_url)
                indexed += 1
            except HTTPError as er:
                # Current retry didn't work. Try again
                print(f"Error sending bundle to indexer:\n{er}")
                print(f"{retries} retries left")
                if retries > 0:
                    retries -= 1
                    sleep(retries)
                else:
                    print("Out of retries, there might be a problem.")
                    print(f"Error:\n{er}")
                    errors[er.code] += 1
                    missing[bundle_fqid] = er.code
                    break
            else:
                break
    print(f"Total of bundle_fqids read: {total}")
    print(f"Total of {indexed} bundle_fqids indexed")
    print("Total number of errors by code:")
    pprint(dict(errors))
    print("Missing bundle_fqids and their error code:")
    pprint(missing)


if __name__ == "__main__":
    main(sys.argv[1:])
