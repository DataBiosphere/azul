from future import standard_library
standard_library.install_aliases()
import os
import json, ssl, argparse
import threading
import time
from urllib.request import urlopen, Request

#es_service = os.environ.get("ES_SERVICE", "localhost")

parser = argparse.ArgumentParser(description='Process options the finder of golden bundles.')
#parser.add_argument('--assay-id', dest='assay_id', action='store',
#                    default='Q3_DEMO-assay1', help='assay id')
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
    req = Request(url, data.encode('utf-8'))
    for key, value in list(headers.items()):
         req.add_header(key, value)

    return req

def executeRequest(req):
    '''
    Helper function to make the post request to the indexer
    '''
    f = urlopen(req)
    response = f.read()
    f.close()
    return response

def parseResultEntry(result_entry):
    '''
    Helper function to parse the results from a single results entry
    '''
    bundle_id = result_entry['bundle_fqid']
    bundle_uuid = bundle_id[:36]
    bundle_version = bundle_id[37:]
    return (bundle_uuid, bundle_version)

def main():
    '''
    Main function which will carry out the execution of the program
    '''
    headers = {"accept": "application/json", "content-type": "application/json"}
    data = json.dumps({"es_query": {"query": {"match": {"files.project_json.content.core.schema_version": "4.6.1"}}}})
    req = requestConstructor(args.dss_url, headers, data)
    response = executeRequest(req)
    response = json.loads(response)
    bundle_list = [parseResultEntry(x) for x in response['results']]
    # Post to the indexer endpoint
    headers = {"content-type": "application/json"}
    # post_result = postToIndexer(bundle_list, args.repoCode, headers)
    threads = []
    for bundle, version in bundle_list:
        data = json.dumps({ "query": { "query": { "match_all":{}} }, "subscription_id": "ba50df7b-5a97-4e87-b9ce-c0935a817f0b", "transaction_id": "ff6b7fa3-dc79-4a79-a313-296801de76b9", "match": { "bundle_version": version, "bundle_uuid": bundle } })
        req = requestConstructor(args.repoCode, headers, data)
        threads.append(threading.Thread(target=executeRequest, args=(req,)))

        print("Bundle: {}, Version: {}".format(bundle, version))
        try:
            response = executeRequest(req)
        except Exception as e:
            print (e)
    print("Total of {} bundles".format(len(bundle_list)))
    start = time.time()
#    for thread in threads:
#        thread.start()
#    for thread in threads:
#        thread.join()
#    print "Elapsed Time: %s" % (time.time() - start)


if __name__ == "__main__":
    main()
