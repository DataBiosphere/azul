import logging
from chalice import Chalice
from urllib.request import urlopen
import json
from flatten_json import flatten
import requests
from subprocess import Popen, PIPE
from elasticsearch import Elasticsearch, RequestsHttpConnection
import boto3
from aws_requests_auth.aws_auth import AWSRequestsAuth
import os
from aws_requests_auth import boto_utils

app = Chalice(app_name='notifications_test')
app.log.setLevel(logging.DEBUG)

#set env on lambda, chalice config and profile
es_host = os.environ['ES_ENDPOINT']
bb_host = "https://"+os.environ['BLUE_BOX_ENDPOINT']

awsauth = AWSRequestsAuth(
    aws_host=es_host,
    aws_region='us-west-2',
    aws_service='es',
    **boto_utils.get_credentials()
)

# use the requests connection_class and pass in our custom auth class
es = Elasticsearch(
    hosts=[{'host': es_host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)


es.indices.create(index='test-index', ignore=400)
@app.route('/')
def es_check():
    return es.info()

@app.route('/index/{file_uuid}')
def get_index(file_uuid):
    file = get_files(file_uuid)
    res = es.index(index="test-index", doc_type='tweet', id=file_uuid, body=file)
    return(res['created'])

#note: Support CORS by adding app.route('/', cors=True)

@app.route('/bundle/{bundle_uuid}', methods=['GET'])
def get_bundles(bundle_uuid):
    app.log.info("get_bundle %s", bundle_uuid)
    json_str = urlopen(str(bb_host+('v1/bundles/')+bundle_uuid)).read()
    bundle = json.loads(json_str)
    json_files = []
    data_files = []
    for file in bundle['bundle']['files']:
        if file["name"].endswith(".json"):
            json_files.append({file["name"]:file["uuid"]})
        else:
            data_files.append({file["name"]: file["uuid"]})
    return {'json_files': json_files, 'data_files': data_files}


@app.route('/file/{file_uuid}', methods=['GET'])
def get_files(file_uuid):
    app.log.info("get_file %s", file_uuid)
    aws_url = bb_host+"v1/files/"+file_uuid+"?replica=aws"
    header = {'accept': 'application/json'}
    aws_response = requests.get(aws_url, headers=header)
    #not flattened
    file = json.loads(aws_response.content)
    return file

@app.route('/write/{bundle_uuid}')
def write_index(bundle_uuid):
    bundle = json.loads(get_bundles(bundle_uuid))
    app.log.info("0")
    fcount = len(bundle['data_files'])
    app.log.info("1")
    json_files = bundle['json_files']
    app.log.info("2")
    with open('config.json') as f:
        config = json.loads(f.read())
    app.log.info("config is %s", config)
    file_uuid = ""
    for i in range (fcount):
        for d_key, d_value in bundle['data_files'][i].items():
            file_uuid = d_key
        es_json = []
        for c_key, c_value in config.items():
            for j in range (len(json_files)):
                if c_key in json_files[j]:
                    file = json.loads(get_file(json_files[j][c_key]))
                    for c_item in c_value:
                        if c_item in file:
                            file_value = file[c_item]
                            if not isinstance(file_value, list):
                                es_json.append({c_item:file_value})
        write_es(es_json, file_uuid)
    return bundle['data_files']

def write_es(es_json, file_uuid):
    app.log.info("write_es %s", file_uuid)
    res = es.index(index="test-index", doc_type='tweet', id=file_uuid, body=es_json)
    return(res['created'])
