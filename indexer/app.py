import logging
from chalice import Chalice, NotFoundError
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
import json
import requests
from elasticsearch import Elasticsearch, RequestsHttpConnection
from aws_requests_auth.aws_auth import AWSRequestsAuth
import os
from aws_requests_auth import boto_utils
import collections
import re
import random
app = Chalice(app_name='test-indexer')
app.debug = True
app.log.setLevel(logging.DEBUG)
# set env on lambda, chalice config and profile
es_host = os.environ['ES_ENDPOINT']
bb_host = "https://"+os.environ['BLUE_BOX_ENDPOINT']
in_host = "https://"+os.environ['INDEXER_ENDPOINT']
try:
    es_index = os.environ['ES_INDEX']
except KeyError:
    es_index = 'test-chalice'
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
#es.indices.delete(index=es_index, ignore=[400])
es.indices.create(index=es_index, ignore=[400])
# used by write_index to flatten nested arrays, also used for mapping
# from https://stackoverflow.com/a/2158532


def flatten(l):
    for el in l:
        if isinstance(el, collections.Sequence) and not isinstance(el, (str, bytes)):
            yield from flatten(el)
        else:
            yield el


def es_config(c_item, name):
    if isinstance(c_item, dict):
        es_array = []
        for key, value in c_item.items():
            name = str(name) + str(key) + "|"
            for item in value:
                es_array.append(es_config(item, name))
            return es_array
    else:
        name = str(name) + str(c_item)
        return (name)
# ES mapping
try:
    with open('chalicelib/config.json') as f:
        config = json.loads(f.read())
except Exception as e:
    print(e)
    raise NotFoundError("chalicelib/config.json file does not exist")
# key_names = ['bundle_uuid', 'dirpath', 'file_name']
key_names = ['bundle_uuid', 'file_name', 'file_name', 'file_uuid', 'file_version', 'file_format','bundle_type']
for c_key, c_value in config.items():
    for c_item in c_value:
        key_names.append(es_config(c_item, ""))
key_names = flatten(key_names)
es_mappings = []
for item in key_names:
    es_mappings.append({item : {"type":"keyword"}})
# file size is different than other key names
es_mappings.append({"file_size" : {"type":"long"}})
es_keys = []
es_values = []
for item in es_mappings:
    if item is not None:
        for key, value in item.items():
            es_keys.append(key)
            es_values.append(value)
    es_file = dict(zip(es_keys, es_values))
final_mapping = '{"properties":'+json.dumps(es_file)+'}'
print(final_mapping)
es.indices.put_mapping(index=es_index,
    doc_type="document", body = final_mapping)
# for blue box notification
@app.route('/', methods=['GET', 'POST'])
def post_notification():
    request = app.current_request.json_body
    app.log.info("Received notification %s", request)
    bundle_uuid = request['match']['bundle_uuid']
    #urlopen(str(in_host + '/write/' + bundle_uuid))
    write_index(bundle_uuid)
    return {"bundle_uuid":bundle_uuid}


@app.route('/escheck')
def es_check():
    return json.dumps(es.info())
# note: Support CORS by adding app.route('/', cors=True)
# returns the name and file uuids sorted by data and json files
@app.route('/bundle/{bundle_uuid}', methods=['GET'])
def get_bundles(bundle_uuid):
    app.log.info("get_bundle %s", bundle_uuid)
#    try:
#        json_str = urlopen(str(bb_host+('v1/bundles/')+bundle_uuid)).read()
#        bundle = json.loads(json_str)
#    except Exception as e:
#        app.log.info(e)
#        raise NotFoundError("Bundle '%s' does not exist" % bundle_uuid)
    retries = 0
    while retries < 3:
        try:
            json_str = urlopen(str(bb_host + ('v1/bundles/') + bundle_uuid)).read()
            bundle = json.loads(json_str)
            break
        except HTTPError as er:
            app.log.info("Error on try {}\n:{}".format(retries, er))
            # if er.code == 504:
            #    retries += 1
            #    continue
            #else:
            #   raise
            retries += 1
            continue
        except URLError as er:
            app.log.info("Error on try {}\n:{}".format(retries, er))
            retries += 1
            continue
    else:
        app.log.error("Maximum number of retries reached: {}".format(retries))
        raise Exception("Unable to access bundle '%s'" % bundle_uuid)


    json_files = []
    data_files = []
    for file in bundle['bundle']['files']:
        if file["name"].endswith(".json"):
            json_files.append({file["name"]: file["uuid"]})
        else:
            # data_files.append({file["name"]: file["uuid"]}) CARLOS REMOVED THIS
            data_files.append({file["name"]: file})
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
        app.log.info("get_bundle1")
        # bundle_url = urlopen(str(in_host+'/bundle/'+ bundle_uuid)).read()
        bundle_url = get_bundles(bundle_uuid)
        app.log.info("get_bundle2")
    except Exception as e:
        app.log.info(e)
        raise NotFoundError("Bundle '%s' does not exist" % bundle_uuid)
    app.log.info("-1")
    bundle = json.loads(bundle_url)
    # bundle = json.loads(get_bundles(bundle_uuid))
    app.log.info("0")
    fcount = len(bundle['data_files'])
    app.log.info("1")
    json_files = bundle['json_files']
    app.log.info("2")
    try:
        with open('chalicelib/config.json') as f:
            config = json.loads(f.read())
    except Exception as e:
        app.log.info(e)
        raise NotFoundError("chalicelib/config.json file does not exist")
    app.log.info("config is %s", config)
    file_uuid = ""
    #for i in range(2):  # in the case of no data_files, this doesn't run
    #    file_uuid = random.randint(100, 999)
    #    app.log.info("3")
        # for d_key, d_value in bundle['data_files'][i].items():
        #     file_uuid = d_key
        #     app.log.info("4")
    file_extensions = set()
    for data_file in bundle['data_files']:
        file_name, values = list(data_file.items())[0]
        file_format = '.'.join(file_name.split('.')[1:])
        file_format = file_format if file_format != '' else 'None'
        file_extensions.add(file_format)
    # Carlos's addition
    for _file in bundle['data_files']:
        file_name, values = list(_file.items())[0]
        file_uuid = values['uuid']
        file_version = values['version']
        # Make the ES uuid be a concatenation of:
        # bundle_uuid:file_uuid:file_version
        es_uuid = "{}:{}:{}".format(bundle_uuid, file_uuid, file_version)
        es_json = []
        app.log.info("4.5")
        for c_key, c_value in config.items():
            app.log.info("5")
            for j in range (len(json_files)):
                app.log.info("6")
                if c_key in json_files[j]:
                    app.log.info("7")
                    try:
                        app.log.info("get_file1")
                        # file_url = urlopen(str(in_host+'/file/' + json_files[j][c_key])).read()
                        file_url = get_file(json_files[j][c_key])
                        app.log.info("get_file2")
                        file = json.loads(file_url)
                    except Exception as e:
                        app.log.info(e)
                        raise NotFoundError("File '%s' does not exist"
                                            % file_uuid)
                    app.log.info("8")
                    for c_item in c_value:
                        app.log.info("9")
                        to_append = look_file(c_item, file, "")
                        app.log.info("10")
                        if to_append is not None:
                            app.log.info("11")
                            if isinstance(to_append, list):
                                app.log.info("11.1")
                                to_append = flatten(to_append)
                                app.log.info("11.2")
                                for item in to_append:
                                    app.log.info("11.3")
                                    es_json.append(item)
                                app.log.info("11.4")
                            else:
                                app.log.info("12")
                                es_json.append(to_append)
                    app.log.info("es12.15 %s", str(es_json))
        app.log.info("12")
        es_json.append({'bundle_uuid': bundle_uuid})
        # Carlos adding extra fields
        es_json.append({'file_name': file_name})
        es_json.append({'file_uuid': file_uuid})
        es_json.append({'file_version': file_version})
        # Add the file format
        file_format = '.'.join(file_name.split('.')[1:])
        file_format = file_format if file_format != '' else 'None'
        es_json.append({'file_format': file_format})
        # Emily adding bundle_type
        if 'analysis.json' in json_files:
            es_json.append({'bundle_type': 'Analysis'})
        elif re.search(r'(tiff)', str(file_extensions)):
            es_json.append({'bundle_type': 'Imaging'})
        elif re.search(r'(fastq.gz)', str(file_extensions)):
            es_json.append({'bundle_type': 'scRNA-Seq Upload'})
        else:
            es_json.append({'bundle_type': 'Unknown'})
        # fake file size
        es_json.append({'file_size': 500000})
        # Carlos using set theory to handle non-present keys
        all_keys = es_file.keys()
        present_keys = [list(x.keys())[0] for x in es_json]
        missing_keys = all_keys - present_keys
        for missing_key in missing_keys:
            es_json.append({missing_key: "None"})
        # write_es(es_json, file_uuid)
        write_es(es_json, es_uuid)
    app.log.info("13")
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
    # Carlos's test
    elif c_item not in file:
        file_value = "None" # Putting an empty string. I think if i put None it could break things downstream
        name = str(name)+str(c_item)
        return ({name: file_value})

# used by write_index to add to ES
def write_es(es_json, file_uuid):
    app.log.info("write_es %s", file_uuid)
    es_keys = []
    es_values = []
    app.log.info("es12.1")
    app.log.info("write_es es_json %s", str(es_json))
    for item in es_json:
        if item is not None:
            app.log.info("es12.2")
            for key, value in item.items():
                app.log.info("es12.3")
                es_keys.append(key)
                es_values.append(value)
    app.log.info("es12.4")
    es_file = dict(zip(es_keys, es_values))
    app.log.info("write_es es_file %s", str(es_file))
    res = es.index(index=es_index, doc_type='document',
                   id=file_uuid, body=es_file)
    app.log.info("es12.6")
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
