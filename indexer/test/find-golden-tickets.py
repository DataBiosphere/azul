# -*- coding: utf-8 -*-

"""
Command line utility to trigger indexing of bundles based on a query
"""

import argparse
from collections import defaultdict
import os

from hca.dss import DSSClient
import json
from pprint import pprint
from time import sleep
from urllib.request import urlopen, Request
from urllib.error import HTTPError
from uuid import uuid4

from utils.deployment import aws


class DefaultProperties:
    """
    Default properties for this script
    """

    def __init__(self):
        """Initialize the default values."""
        self._dss_url = os.environ['AZUL_DSS_ENDPOINT']
        self._indexer_url = aws.api_getway_endpoint(function_name=os.environ['AZUL_INDEXER_NAME'],
                                                    api_gateway_stage=os.environ['AZUL_DEPLOYMENT_STAGE'])
        self._es_query = {"query": {"match_all": {}}}

    @property
    def dss_url(self):
        """
        Return the URL of the dss
        """
        return self._dss_url

    @property
    def indexer_url(self):
        """
        Return the url of the indexer
        """
        return self._indexer_url

    @property
    def es_query(self):
        """
        Return the ElasticSearch query
        """
        return self._es_query


default = DefaultProperties()
dss_client = DSSClient()

parser = argparse.ArgumentParser(
    description='Process options the finder of golden bundles.')
parser.add_argument('--dss-url',
                    dest='dss_url',
                    action='store',
                    default=default.dss_url,
                    help='The url for the storage system.')
parser.add_argument('--indexer-url',
                    dest='indexer_url',
                    action='store',
                    default=default.indexer_url,
                    help='The indexer URL')
parser.add_argument('--es-query',
                    dest='es_query',
                    action='store',
                    default=default.es_query,
                    type=json.loads,
                    help='The ElasticSearch query to use')

args = parser.parse_args()


def post_bundle(bundle_fqid):
    """
    Send a fake BlueBox notification to the indexer
    """
    bundle_uuid, _, bundle_version = bundle_fqid.partition('.')
    simulated_event = {
        "query": args.es_query,
        "subscription_id": str(uuid4()),
        "transaction_id": str(uuid4()),
        "match": {
            "bundle_uuid": bundle_uuid,
            "bundle_version": bundle_version
        }
    }
    body = json.dumps(simulated_event).encode('utf-8')
    request = Request(args.indexer_url, body)
    request.add_header("content-type", "application/json")
    with urlopen(request) as f:
        return f.read()


def main():
    """
    Entrypoint method for the script
    """
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
                post_bundle(bundle_fqid)
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
    main()
