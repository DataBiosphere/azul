import logging
from chalice import Chalice, NotFoundError
from urllib.request import urlopen
import json
import requests
from elasticsearch import Elasticsearch, RequestsHttpConnection
from aws_requests_auth.aws_auth import AWSRequestsAuth
import os
from aws_requests_auth import boto_utils
import collections

app = Chalice(app_name='azul_indexer')
app.debug = True
app.log.setLevel(logging.DEBUG)

# set env on lambda, chalice config and profile
es_host = os.environ['ES_ENDPOINT']
bb_host = "https://"+os.environ['BLUE_BOX_ENDPOINT']
in_host = "https://"+os.environ['INDEXER_ENDPOINT']

# need to have the AWS CLI and $aws configure
awsauth = AWSRequestsAuth(
    aws_host=es_host,
    aws_region='us-west-2',
    aws_service='es',
    **boto_utils.get_credentials()
)

if es_host.endswith('.es.amazonaws.com'):
    # use the requests connection_class and pass in our custom auth class
    es = Elasticsearch(
        hosts=[{'host': es_host, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )
else:
    # default auth for testing purposes
    es = Elasticsearch(
        [{'host': 'localhost', 'port': 9200}],
        http_auth=('elastic', 'changeme'),
        use_ssl=False
    )

es.indices.create(index='test-index', ignore=[400])


# for blue box notification
@app.route('/', methods=['GET', 'POST'])
def post_notification():
    request = app.current_request.json_body
    app.log.info("Received notification %s", request)
    bundle_uuid = request['match']['bundle_uuid']
    urlopen(str(in_host + '/write/' + bundle_uuid))
    return {"bundle_uuid": bundle_uuid}


@app.route('/escheck')
def es_check():
    return json.dumps(es.info())

# note: Support CORS by adding app.route('/', cors=True)


# returns the name and file uuids sorted by data and json files
@app.route('/bundle/{bundle_uuid}', methods=['GET'])
def get_bundles(bundle_uuid):
    app.log.info("get_bundle %s", bundle_uuid)
    try:
        json_str = urlopen(str(bb_host+('v1/bundles/')+bundle_uuid)).read()
        bundle = json.loads(json_str)
    except Exception as e:
        app.log.info(e)
        raise NotFoundError("Bundle '%s' does not exist" % bundle_uuid)
    json_files = []
    data_files = []
    for file in bundle['bundle']['files']:
        if file["name"].endswith(".json"):
            json_files.append({file["name"]: file["uuid"]})
        else:
            data_files.append({file["name"]: file["uuid"]})
    return json.dumps({'json_files': json_files, 'data_files': data_files})


# returns the file
@app.route('/file/{file_uuid}', methods=['GET'])
def get_file(file_uuid):
    app.log.info("get_file %s", file_uuid)
    aws_url = bb_host+"v1/files/"+file_uuid+"?replica=aws"
    header = {'accept': 'application/json'}
    try:
        aws_response = requests.get(aws_url, headers=header)
        # not flattened
        file = json.loads(aws_response.content)
    except Exception as e:
        app.log.info(e)
        raise NotFoundError("File '%s' does not exist" % file_uuid)
    return json.dumps(file)


# indexes the files in the bundle
@app.route('/write/{bundle_uuid}')
def write_index(bundle_uuid):
    app.log.info("write_index %s", bundle_uuid)
    try:
        # bundle_url = urlopen(str(in_host+'/bundle/'+ bundle_uuid)).read()
        bundle_url = get_bundles(bundle_uuid)
    except Exception as e:
        app.log.info(e)
        raise NotFoundError("Bundle '%s' does not exist" % bundle_uuid)
    bundle = json.loads(bundle_url)
    # bundle = json.loads(get_bundles(bundle_uuid))
    fcount = len(bundle['data_files'])
    json_files = bundle['json_files']
    try:
        with open('chalicelib/config.json') as f:
            config = json.loads(f.read())
    except Exception as e:
        app.log.info(e)
        raise NotFoundError("chalicelib/config.json file does not exist")
    app.log.info("config is %s", config)
    file_uuid = ""
    for i in range(fcount):  # in the case of no data_files, this doesn't run
        for d_key, d_value in bundle['data_files'][i].items():
            file_uuid = d_key
        es_json = []
        for c_key, c_value in config.items():
            for j in range(len(json_files)):
                if c_key in json_files[j]:
                    try:
                        file_url = get_file(json_files[j][c_key])
                        file = json.loads(file_url)
                    except Exception as e:
                        app.log.info(e)
                        raise NotFoundError("File '%s' does not exist"
                                            % file_uuid)
                    for c_item in c_value:
                        to_append = look_file(c_item, file, "")
                        if to_append is not None:
                            if isinstance(to_append, list):
                                to_append = flatten(to_append)
                                for item in to_append:
                                    es_json.append(item)
                            else:
                                es_json.append(to_append)
                    app.log.info("write_index es_json %s", str(es_json))
        es_json.append({'bundle_uuid': bundle_uuid})
        write_es(es_json, file_uuid)
    return json.dumps(bundle['data_files'])


# used by write_index to recursively return values of items in config file
def look_file(c_item, file, name):
    app.log.info("look_file %s", c_item)
    if isinstance(c_item, dict):
        es_array = []
        for key, value in c_item.items():
            if key in file:
                name = str(name)+str(key)+"|"
                for item in value:
                    es_array.append(look_file(item, file[key], name))
                return es_array
    elif c_item in file:
        file_value = file[c_item]
        if not isinstance(file_value, list):
            name = str(name)+str(c_item)
            return ({name: file_value})


# used by write_index to flatten nested arrays
# from https://stackoverflow.com/a/2158532
def flatten(l):
    for el in l:
        if isinstance(el, collections.Sequence) and not isinstance(el, (str, bytes)):
            yield from flatten(el)
        else:
            yield el


# used by write_index to add to ES
def write_es(es_json, file_uuid):
    app.log.info("write_es %s", file_uuid)
    es_keys = []
    es_values = []
    app.log.info("write_es es_json %s", str(es_json))
    for item in es_json:
        for key, value in item.items():
            es_keys.append(key)
            es_values.append(value)
    es_file = dict(zip(es_keys, es_values))
    app.log.info("write_es es_file %s", str(es_file))
    res = es.index(index="test-index", doc_type='document',
                   id=file_uuid, body=es_file)
    return(res['created'])


# daily scan of the blue box
@app.schedule('rate(1 day)')
def every_day(event, context):
    pass


@app.route('/cron')
def cron_look():
    headers = {"content-type": "application/json",
               "accept": "application/json"}
    data = {"query": {"match_all": {}}}
    bb_url = str(bb_host)+"/v1/search"
    r = requests.post(bb_url, data=json.dumps(data), headers=headers)
    r_response = json.loads(r.text)
    bundle_ids = []
    for item in r_response['results']:
        bundle_mod = item['bundle_id'][:-26]
        app.log.info(bundle_mod)
        bundle_ids.append(bundle_mod)
        write_index(bundle_mod)
    app.log.info({"bundle_id": bundle_ids})
    return {"bundle_id": bundle_ids}

