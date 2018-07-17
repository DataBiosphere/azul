import json

from azul import config
from elasticsearch import Elasticsearch, RequestsHttpConnection
from aws_requests_auth.aws_auth import AWSRequestsAuth
import os
from aws_requests_auth import boto_utils
import collections
import uuid

es_host = config.es_endpoint
bundle_path = os.environ['BUNDLE_PATH']
try:
    es_index = config.es_index
except KeyError:
    es_index = 'test-import'


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

es.indices.delete(index=es_index, ignore=[400])
es.indices.create(index=es_index, ignore=[400])

print(es.indices.get_mapping(index=es_index,
                                     doc_type="document"))

# used by write_index to flatten nested arrays, and config
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
key_names = ['bundle_uuid', 'dirpath', 'file_name']
for c_key, c_value in config.items():
    for c_item in c_value:
        key_names.append(es_config(c_item, ""))
key_names = flatten(key_names)
es_mappings = []
for item in key_names:
    print ({item: {"type":"keyword"}})
    es_mappings.append({item : {"type":"keyword"}})
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

#main function to call to load bundles
def load_bundles():
    try:
        with open('chalicelib/config.json') as f:
            config = json.loads(f.read())
    except Exception as e:
        print(e)
        raise NotFoundError("chalicelib/config.json file does not exist")
    for (dirpath, dirnames, filenames) in os.walk(bundle_path):
        if 'manifest.json' in filenames:
            manifest = open(str(dirpath)+'/manifest.json')
            rmanifest = json.loads(manifest.read())
            fcount = len(rmanifest['files'])
            for i in range(fcount):  # in the case of no data_files, this doesn't run
                for item in rmanifest['files']:
                    name = str(item.get('name') + dirpath)
                    namespace = uuid.NAMESPACE_URL
                    file_uuid = str(uuid.uuid5(namespace, name))
                    file_name = item.get('name')
                    es_json = []
                    for c_key, c_value in config.items():
                        #for j in range (len(filenames)):
                            if c_key in filenames:
                                try:
                                    file_url = open(str(dirpath)+'/'+c_key)
                                    file = json.loads(file_url.read())
                                except Exception as e:
                                    print(e)
                                    raise NotFoundError("File '%s' does not exist"
                                                        % file_uuid)
                                es_json.append({"file_name": file_name})
                                for c_item in c_value:
                                    to_append = look_file(c_item, file, "", file_name)
                                    if to_append is not None:
                                        if isinstance(to_append, list):
                                            to_append = flatten(to_append)
                                            for item in to_append:
                                                es_json.append(item)
                                        else:
                                            es_json.append(to_append)
                                print("es12.15 %s", str(es_json))
                            else:
                                for c_item in c_value:
                                    to_append = es_config(c_item, "")
                                    if to_append is not None:
                                        if isinstance(to_append, list):
                                            to_append = flatten(to_append)
                                            for item in to_append:
                                                es_json.append({item:"no"+item})
                                        else:
                                            es_json.append({to_append:"no "+to_append})
                    es_json.append({'dirpath': dirpath})
                    write_es(es_json, file_uuid)
                print("13")



# used by write_index to recursively return values of items in config file
def look_file(c_item, file, name, file_name):
    print("look_file %s", c_item)
    if isinstance(c_item, dict):
        es_array = []
        for key, value in c_item.items():
            if key in file:
                name = str(name)+str(key)+"|"
                for item in value:
                    es_array.append(look_file(item, file[key], name, file_name))
                return es_array
    elif isinstance(file, list):
        for item in file:
            if item.get('name') == file_name:
                name = str(name) + str(c_item)
                return ({name: item.get(c_item)})
    elif c_item in file:
        file_value = file[c_item]
        if not isinstance(file_value, list):
            name = str(name)+str(c_item)
            return ({name: file_value})
    else:
        return ({c_item:"no "+c_item})


# used by write_index to add to ES
def write_es(es_json, file_uuid):
    print("write_es %s", file_uuid)
    es_keys = []
    es_values = []
    for item in es_json:
        if item is not None:
            for key, value in item.items():
                es_keys.append(key)
                es_values.append(value)
        es_file = dict(zip(es_keys, es_values))
    print("write_es es_file %s", str(es_file))
    res = es.index(index=es_index, doc_type='document',
                       id=file_uuid, body=es_file)
    return(res['created'])


load_bundles()

