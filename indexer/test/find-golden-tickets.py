import os
import json, ssl, argparse
from urllib2 import urlopen, Request

#es_service = os.environ.get("ES_SERVICE", "localhost")

parser = argparse.ArgumentParser(description='Process options the finder of golden bundles.')
parser.add_argument('--assay-id', dest='assay_id', action='store',
                    default='Q3_DEMO-assay1', help='assay id')
parser.add_argument('--dss-url', dest='dss_url', action='store',
                    default='https://dss.staging.data.humancellatlas.org/v1/search?replica=aws', help='The url for the storage system.')
parser.add_argument('--indexer-url', dest='repoCode', action='store',
                    default='Carlos????', help='The indexer URL')

#Get the arguments into args
args = parser.parse_args()

# headers = {}
#json_str = urlopen(requestConstructor(str("https://metadata."+redwood_host+"/entities?page=0"), headers), context=ctx).read()

def requestConstructor(url, headers):
    '''
    Helper function to make requests to use on with urlopen()
    '''
    req = Request(url)
    for key, value in headers.items():
         req.add_header(key, value)

    return req
