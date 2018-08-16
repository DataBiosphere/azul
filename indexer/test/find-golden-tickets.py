# -*- coding: utf-8 -*-
"""Command line utility to trigger indexing of bundles based on a query."""
import argparse
from collections import defaultdict
from hca import HCAConfig
from hca.dss import DSSClient
import json
from pprint import pprint
from time import sleep
from urllib.request import urlopen, Request
from urllib.error import HTTPError
from uuid import uuid4


class DefaultProperties:
    """Default Properties for this script."""

    def __init__(self):
        """Initialize the default values."""
        self._dss_url = "https://dss.staging.data.humancellatlas.org/v1"
        self._indexer_url = "https://9b92wjnlgh.execute-api.us-west-2.\
        amazonaws.com/dev/"
        self._es_query = {"query": {"match_all": {}}}

    @property
    def dss_url(self):
        """Return the url of the dss."""
        return self._dss_url

    @property
    def indexer_url(self):
        """Return the url of the indexer."""
        return self._indexer_url

    @property
    def es_query(self):
        """Return the ElasticSearch query."""
        return self._es_query


default = DefaultProperties()

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


def request_constructor(url, headers, data):
    """Create a request using urlopen."""
    req = Request(url, data.encode('utf-8'))
    for key, value in list(headers.items()):
        req.add_header(key, value)
    return req


def execute_request(req):
    """Execute a request."""
    f = urlopen(req)
    response = f.read()
    f.close()
    return response


def uuid_and_version(result_entry):
    """Parse the search result entry into bundle uuid and bundle version."""
    bundle_fqid = result_entry['bundle_fqid']
    bundle_uuid = bundle_fqid.partition('.')[0]
    bundle_version = bundle_fqid.partition('.')[2]
    return (bundle_uuid, bundle_version)


def post_bundle(bundle_uuid, bundle_version):
    """Send a fake BlueBox notification to the indexer."""
    simulated_event = {
        "query": args.es_query,
        "subscription_id": str(uuid4()),
        "transaction_id": str(uuid4()),
        "match": {
            "bundle_uuid": bundle_uuid,
            "bundle_version": bundle_version
        }
    }
    request = request_constructor(args.indexer_url,
                                  {"content-type": "application/json"},
                                  json.dumps(simulated_event))
    execute_request(request)


def main():
    """Entrypoint method for the script."""

    # Workaround known issues with setting-up HCAConfig/DSSClient
    # See: https://github.com/HumanCellAtlas/dcp-cli/issues/170
    HCAConfig._user_config_home = '/tmp/'
    config = HCAConfig(save_on_exit=False, autosave=False)
    config['DSSClient'].swagger_url = args.dss_url + '/swagger.json'
    dss_client = DSSClient(config=config)

    parameters = {
        "es_query": args.es_query,
        "replica": "aws"
    }
    # TODO: Need to write this so it scales nicely
    bundle_list = [uuid_and_version(r)
                   for r in dss_client.post_search.iterate(**parameters)]
    errors = defaultdict(int)
    missing = {}
    indexed = 0
    total = 0
    for bundle_uuid, bundle_version in bundle_list:
        total += 1
        print("Bundle: {}, Version: {}".format(bundle_uuid, bundle_version))
        retries = 3
        while True:
            try:
                post_bundle(bundle_uuid, bundle_version)
                indexed += 1
            except HTTPError as er:
                # Current retry didn't work. Try again
                print("Error sending bundle to indexer:\n{}".format(er))
                print("{} retries left".format(retries))
                if retries > 0:
                    retries -= 1
                    sleep(retries)
                else:
                    print("Out of retries, there might be a problem.")
                    print("Error:\n{}".format(er))
                    errors[er.code] += 1
                    bundle_fqid = "{}.{}".format(bundle_uuid, bundle_version)
                    missing[bundle_fqid] = er.code
                    break
            else:
                break
    print("Total of bundles read: {}".format(total))
    print("Total of {} bundles indexed".format(indexed))
    print("Total number of errors by code:")
    pprint(errors)
    print("Missing bundles and their error code:")
    pprint(missing)


if __name__ == "__main__":
    main()
