import os
import json, ssl, argparse
import threading
from urllib2 import urlopen, Request

#es_service = os.environ.get("ES_SERVICE", "localhost")

parser = argparse.ArgumentParser(description='Process options the finder of golden bundles.')
parser.add_argument('--assay-id', dest='assay_id', action='store',
                    default='Q3_DEMO-assay1', help='assay id')
parser.add_argument('--dss-url', dest='dss_url', action='store',
                    default='https://dss.staging.data.humancellatlas.org/v1/search?replica=aws', help='The url for the storage system.')
parser.add_argument('--indexer-url', dest='repoCode', action='store',
                    default='https://9b92wjnlgh.execute-api.us-west-2.amazonaws.com/dev/', help='The indexer URL')

#Get the arguments into args
args = parser.parse_args()

# headers = {}
#json_str = urlopen(requestConstructor(str("https://metadata."+redwood_host+"/entities?page=0"), headers), context=ctx).read()

def requestConstructor(url, headers, data):
    '''
    Helper function to make requests to use on with urlopen()
    '''
    req = Request(url, data)
    for key, value in headers.items():
         req.add_header(key, value)

    return req

def postToIndexer(bundle_list, url, headers):
    '''
    Helper function to make the post request to the indexer
    '''
    pass

def parseResultEntry(result_entry):
    '''
    Helper function to parse the results from a single results entry
    '''
    bundle_id = result_entry['bundle_id']
    bundle_uuid = bundle_id[:36]
    bundle_version = bundle_id[37:]
    return (bundle_uuid, bundle_version)

def main():
    '''
    Main function which will carry out the execution of the program
    '''
    headers = {"accept": "application/json", "content-type": "application/json"}
    data = json.dumps({"es_query": {"query": { "bool": {"must": [{"match":{"files.assay_json.id": args.assay_id}}]}}}})
    req = requestConstructor(args.dss_url, headers, data)
    f = urlopen(req)
    response = f.read()
    f.close()
    response = json.loads(response)
    bundle_list = [parseResultEntry(x) for x in response['results']]
    # Post to the indexer endpoint 
    headers = {"content-type": "application/json"}
    # post_result = postToIndexer(bundle_list, args.repoCode, headers)
    for bundle, version in bundle_list:
        data = json.dumps({ "query": { "query": { "match_all":{}} }, "subscription_id": "ba50df7b-5a97-4e87-b9ce-c0935a817f0b", "transaction_id": "ff6b7fa3-dc79-4a79-a313-296801de76b9", "match": { "bundle_version": version, "bundle_uuid": bundle } })
        req = requestConstructor(args.repoCode, headers, data)
        print req
        f = urlopen(req)
        response = f.read()
        f.close()
        print response

if __name__ == "__main__":
    main()
