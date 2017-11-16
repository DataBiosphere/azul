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
from multiprocessing import Process
import threading

# import time
# import random


app = Chalice(app_name=os.getenv('INDEXER_NAME', 'dss-indigo'))
app.debug = True
app.log.setLevel(logging.DEBUG)
# set env on lambda, chalice config and profile
es_host = os.environ['ES_ENDPOINT']
bb_host = "http://" + os.environ['BLUE_BOX_ENDPOINT']
# in_host = "https://"+os.environ['INDEXER_ENDPOINT']

try:
    config_analyzer = os.environ['CONFIG_ANALYZER']
except KeyError:
    config_analyzer = 'autocomplete'
try:
    es_index = os.environ['ES_INDEX']
except KeyError:
    es_index = 'test-chalice'
try:
    replica = os.environ['REPLICA']
except KeyError:
    replica = 'aws'

# get settings for elasticsearch
try:
    with open('chalicelib/settings.json') as f:
        es_settings = json.loads(f.read())
except Exception as e:
    print(e)
    raise NotFoundError("chalicelib/settings.json file does not exist")

# choose which Elasticsearch to use
if es_host.endswith('.es.amazonaws.com'):
    # need to have the AWS CLI and $aws configure
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
else:
    # default auth for testing purposes
    es = Elasticsearch(
        [{'host': 'localhost', 'port': 9200}],
        http_auth=('elastic', 'changeme'),
        use_ssl=False
    )
# es.indices.delete(index=es_index, ignore=[400])
es.indices.create(index=es_index, body=es_settings, ignore=[400])


# used by write_index to flatten nested arrays, also used for mapping
# from https://stackoverflow.com/a/2158532
def flatten(l):
    for el in l:
        if isinstance(el, collections.Sequence) and not isinstance(el, (
                str, bytes)):
            yield from flatten(el)
        else:
            yield el


def es_config(c_item, name):
    """
    This function is a simpler version of look_file
    The name is recursively found by going through
    the nested levels of the config file
    :param c_item: config item
    :param name: used for key in the key, value pair
    :return: name
    """
    if isinstance(c_item, dict):
        es_array = []
        # name concatenated with config key
        for key, value in c_item.items():
            if len(name) > 0:
                name = str(name) + "|" + str(key)
            else:
                name = str(key)
            # recursively call function on each item in this level of config values
            for item in value:
                es_array.append(es_config(item, name))
            return es_array
    else:
        # return name concatenated with config key
        name = str(name) + "|" + str(c_item)
        return (name)


# ES mapping
try:
    with open('chalicelib/config.json') as f:
        config = json.loads(f.read())
except Exception as e:
    print(e)
    raise NotFoundError("chalicelib/config.json file does not exist")
# key_names = ['bundle_uuid', 'dirpath', 'file_name']
key_names = ['bundle_uuid', 'file_name', 'file_uuid',
             'file_version', 'file_format', 'bundle_type', "file_size*long"]
for c_key, c_value in config.items():
    for c_item in c_value:
        # get the names for each of the items in the config
        # the key_names array still has the mapping attached to each name
        key_names.append(es_config(c_item, c_key))
key_names = flatten(key_names)
es_mappings = []

# i_split splits at "*" (i for item)
# u_split splits i_split[1] at "_" (u for underscore)
# banana_split splits i_split[2] at "_"
for item in key_names:
    # this takes in names with mappings still attached, separates it
    # name and mappings separated by *
    # analyzer is separated by mapping by _
    # ex: name*mapping1*mapping2_analyzer
    i_replace = item.replace(".", ",")
    i_split = i_replace.split("*")
    if len(i_split) == 1:
        # ex: name
        # default behavior: main field: keyword, raw field: text with analyzer
        es_mappings.append(
            {i_split[0]: {"type": "keyword",
                          "fields": {"raw": {"type": "text",
                                             "analyzer": config_analyzer,
                                             "search_analyzer": "standard"}}}})
    elif len(i_split) == 2:
        u_split = i_split[1].split("_")
        if len(u_split) == 1:
            # ex: name*mapping1
            es_mappings.append({i_split[0]: {"type": i_split[1]}})
        else:
            # ex: name*mapping1_analyzer
            es_mappings.append(
                {i_split[0]: {"type": u_split[0], "analyzer": u_split[1],
                              "search_analyzer": "standard"}})
    else:
        u_split = i_split[1].split("_")
        banana_split = i_split[2].split("_")
        if len(u_split) == 1 and len(banana_split) == 1:
            # ex: name*mapping1*mapping2
            es_mappings.append({i_split[0]: {"type": i_split[1], "fields": {
                "raw": {"type": i_split[2]}}}})
        elif len(u_split) == 2 and len(banana_split) == 1:
            # ex: name*mapping1_analyzer*mapping2
            es_mappings.append(
                {i_split[0]: {"type": u_split[0], "analyzer": u_split[1],
                              "search_analyzer": "standard",
                              "fields": {"raw": {"type": i_split[2]}}}})
        elif len(u_split) == 1 and len(banana_split) == 2:
            # ex: name*mapping1*mapping2_analyzer
            es_mappings.append(
                {i_split[0]: {"type": i_split[1],
                              "fields": {"raw": {"type": banana_split[0],
                                                 "analyzer": banana_split[1],
                                                 "search_analyzer": "standard"}}}})
        elif len(u_split) == 2 and len(banana_split) == 2:
            # ex: name*mapping1_analyzer*mapping2_analyzer
            es_mappings.append(
                {i_split[0]: {"type": u_split[0], "analyzer": u_split[1],
                              "search_analyzer": "standard",
                              "fields": {"raw": {"type": banana_split[0],
                                                 "analyzer": banana_split[1],
                                                 "search_analyzer": "standard"}}}})
        else:
            app.log.info("mapping formatting problem %s", i_split)

es_keys = []
es_values = []
# format mappings
for item in es_mappings:
    if item is not None:
        for key, value in item.items():
            es_keys.append(key)
            es_values.append(value)
    es_file = dict(zip(es_keys, es_values))
final_mapping = '{"properties":' + json.dumps(es_file) + '}'

# mapping added to elasticsearch
es.indices.put_mapping(index=es_index,
                       doc_type="document", body=final_mapping)


# for blue box notification
@app.route('/', methods=['GET', 'POST'])
def post_notification():
    """
    This function takes in post notifications from the blue box and
    sends a request to write_index to start creating the ES index
    :return: bundle_uuid
    """
    # blue box sends json with post request
    request = app.current_request.json_body
    app.log.info("Received notification %s", request)
    # look within request for the bundle_uuid
    bundle_uuid = request['match']['bundle_uuid']
    # send bundle_uuid to write_index
    write_index(bundle_uuid)
    return {"bundle_uuid": bundle_uuid}


@app.route('/escheck')
def es_check():
    """
    Returns the general Elasticsearch info.
    Tests to see if indexer is hooked up to Elasticsearch
    """
    return json.dumps(es.info())


# note: Support CORS by adding app.route('/', cors=True)
# returns the name and file uuids sorted by data and json files
@app.route('/bundle/{bundle_uuid}', methods=['GET'])
def get_bundles(bundle_uuid):
    """
    This function gets a bundle from the blue box
    and sorts returned items by json and data (not json) files
    :param bundle_uuid: tell blue box which bundle to get
    :return: json file with items separated by json files and data files
    """
    app.log.info("get_bundle %s", bundle_uuid)
    #    try:
    #        json_str = urlopen(str(bb_host+('v1/bundles/')+bundle_uuid)).read()
    #        bundle = json.loads(json_str)
    #    except Exception as e:
    #        app.log.info(e)
    #        raise NotFoundError("Bundle '%s' does not exist" % bundle_uuid)

    # the blue box may have trouble returning bundle, so retry 3 times
    retries = 0
    while retries < 3:
        try:
            # call the blue box
            json_str = urlopen(str(bb_host + (
                'v1/bundles/') + bundle_uuid + "?replica=" + replica)).read()
            # json load string for processing later
            bundle = json.loads(json_str)
            break
        except HTTPError as er:
            app.log.info("Error on try {}\n:{}".format(retries, er))
            # if er.code == 504:
            #    retries += 1
            #    continue
            # else:
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
    # separate files in bundle by json files and data files
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
    """
    This function gets a file from the blue box
    :param file_uuid: tell blue box which file to get
    :return: file
    """
    app.log.info("get_file %s", file_uuid)
    # '?replica=aws' needed to choose cloud location
    aws_url = bb_host + "v1/files/" + file_uuid + "?replica=" + replica
    # only accept json files
    header = {'accept': 'application/json'}
    try:
        aws_response = requests.get(aws_url, headers=header)
        # not flattened
        file = json.loads(aws_response.content)
    except Exception as e:
        app.log.info(e)
        raise NotFoundError("File '%s' does not exist" % file_uuid)
    # queue.put(json.dumps(file))
    return json.dumps(file)


# indexes the files in the bundle
@app.route('/write/{bundle_uuid}')
def write_index(bundle_uuid):
    """
    This function indexes the bundle,
    the bulk of the work is going through all of the items on the config.
    :param bundle_uuid: tell indexer which bundle to index
    :return: data files (since these are the files being indexed)
    """
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
    # open config file
    try:
        with open('chalicelib/config.json') as f:
            config = json.loads(f.read())
    except Exception as e:
        app.log.info(e)
        raise NotFoundError("chalicelib/config.json file does not exist")
    app.log.info("config is %s", config)
    file_uuid = ""
    # for i in range(2):  # in the case of no data_files, this doesn't run
    #    file_uuid = random.randint(100, 999)
    #    app.log.info("3")
    # for d_key, d_value in bundle['data_files'][i].items():
    #     file_uuid = d_key
    #     app.log.info("4")

    # get file_format from file_names
    file_extensions = set()
    for data_file in bundle['data_files']:
        file_name, values = list(data_file.items())[0]
        file_format = '.'.join(file_name.split('.')[1:])
        file_format = file_format if file_format != '' else 'None'
        file_extensions.add(file_format)
    # Carlos's addition
    # Since file-based index, every data file in bundle will need to be indexed
    for _file in bundle['data_files']:
        file_name, values = list(_file.items())[0]
        file_uuid = values['uuid']
        file_version = values['version']
        file_size = values['size']
        # Make the ES uuid be a concatenation of:
        # bundle_uuid:file_uuid:file_version
        es_uuid = "{}:{}:{}".format(bundle_uuid, file_uuid, file_version)
        es_json = []

        # every item in config must be looked at
        # top level of config is file name
        threads = []
        for c_key, c_value in config.items():
            # config (file name) must be looked for in all the json_files
            thread = threading.Thread(target=config_thread,
                                      args=(
                                          c_key, c_value, json_files,
                                          file_uuid,
                                          es_json))
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()

        # add special fields (ones that aren't in config)
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
        if 'analysis.json' in [list(x.keys())[0] for x in json_files]:
            es_json.append({'bundle_type': 'Analysis'})
        elif re.search(r'(tiff)', str(file_extensions)):
            es_json.append({'bundle_type': 'Imaging'})
        elif re.search(r'(fastq.gz)', str(file_extensions)):
            es_json.append({'bundle_type': 'scRNA-Seq Upload'})
        else:
            es_json.append({'bundle_type': 'Unknown'})
        # adding size of the file
        es_json.append({'file_size': file_size})
        # Carlos using set theory to handle non-present keys
        all_keys = es_file.keys()
        present_keys = [list(x.keys())[0] for x in es_json]
        missing_keys = all_keys - present_keys
        for missing_key in missing_keys:
            es_json.append({missing_key: "None"})
        # write_es(es_json, file_uuid)
        # tell elasticsearch to index
        write_es(es_json, es_uuid)
    return json.dumps(bundle['data_files'])


def config_thread(c_key, c_value, json_files, file_uuid, es_json):
    app.log.info("config_thread %s", str(c_key))
    for j in range(len(json_files)):
        # if the config (file name) is in the given json file
        if c_key in json_files[j]:
            try:
                # file_url = urlopen(str(in_host+'/file/' + json_files[j][c_key])).read()
                file_url = get_file(json_files[j][c_key])
                file = json.loads(file_url)
            except Exception as e:
                app.log.info(e)
                raise NotFoundError("File '%s' does not exist"
                                    % file_uuid)
            # for every item under this file name in config
            for c_item in c_value:
                # look for config item in file
                to_append = look_file(c_item, file, c_key)
                # if config item is in the file
                if to_append is not None:
                    if isinstance(to_append, list):
                        # makes lists of lists into a single list
                        to_append = flatten(to_append)
                        for item in to_append:
                            # add file item to list of items to append to ES
                            es_json.append(item)
                    else:
                        # add file item to list of items to append to ES
                        es_json.append(to_append)
            app.log.info("config_thread es_json %s", str(es_json))


# used by write_index to recursively return values of items in config file
def look_file(c_item, file, name):
    """
    This function recursively iterates through the config file and
    file to find values for each key in the config
    The returning key value pairs in the array have keys that show the
    path taken to get to the value.
    Example File: {"happy":{"hello":"world"}}
    Example Return: [{"happy|hello":"world"}]
    :param c_item: config item
    :param file: complete file to look through
    :param name: used for key in the key, value pair
    :return: array of items found in file given config
    """
    app.log.info("look_file %s", c_item)
    if isinstance(c_item, dict):
        # if the config is a dictionary,
        # then need to look deeper into the file and config for the key
        es_array = []
        for key, value in c_item.items():
            # removing mapping param
            key_split = key.split("*")
            if key in file:
                # making the name that shows path taken to get to value
                if len(name) > 0:
                    name = str(name) + "|" + str(key_split[0])
                else:
                    name = str(key)
                for item in value:
                    # resursive call, one nested item deeper
                    es_array.append(look_file(item, file[key_split[0]], name))
                return es_array
    elif c_item.split("*")[0] in file:
        # if config item is in the file
        c_item_split = c_item.split("*")
        file_value = file[c_item_split[0]]
        # need to be able to handle lists
        if not isinstance(file_value, list):
            if len(name) > 0:
                name = str(name) + "|" + str(c_item_split[0])
            else:
                name = str(c_item_split[0])
            # ES does not like periods(.) use commas(,) instead
            n_replace = name.replace(".", ",")
            # return the value of key (given by config)
            return ({n_replace: file_value})
    # Carlos's test
    elif c_item.split("*")[0] not in file:
        # all config items that cannot be found in file are given value "None"
        c_item_split = c_item.split("*")
        file_value = "None"  # Putting an empty string. I think if I put None (instead of "None") it could break things downstream
        name = str(name) + "|" + str(c_item_split[0])
        # ES does not like periods(.) use commas(,) instead
        n_replace = name.replace(".", ",")
        return ({n_replace: file_value})


# used by write_index to add to ES
def write_es(es_json, file_uuid):
    """
    This function adds json to Elasticsearch with id of file_uuid
    :param es_json: json to index into Elasticsearch
    :param file_uuid: used for Elasticsearch id
    :return: Elasticsearch response of creation
    """
    app.log.info("write_es %s", file_uuid)
    es_keys = []
    es_values = []
    app.log.info("write_es es_json %s", str(es_json))
    # es_json is a list of dictionaries,
    # instead want dictionary of key value pairs
    for item in es_json:
        if item is not None:
            for key, value in item.items():
                es_keys.append(key)
                es_values.append(value)
    es_file = dict(zip(es_keys, es_values))
    app.log.info("write_es es_file %s", str(es_file))
    # this is the actual call to ES
    res = es.index(index=es_index, doc_type='document',
                   id=file_uuid, body=es_file)
    return (res['created'])


# daily scan of the blue box
@app.schedule('rate(1 day)')
def every_day(event, context):
    pass


@app.route('/cron')
def cron_look():
    app.log.info("cron job running")
    headers = {"content-type": "application/json",
               "accept": "application/json"}
    data = {"es_query": {"query": {"match_all": {}}}}
    bb_url = str(bb_host) + "/v1/search?replica=" + replica
    r = requests.post(bb_url, data=json.dumps(data), headers=headers)
    r_response = json.loads(r.text)
    app.log.info(r_response)
    bundle_ids = []
    count = 0
    for item in r_response['results']:
        count += 1
    # create a list to keep all processes
    processes = []
    for item in r_response['results']:
        bundle_mod = item['bundle_id'][:-26]
        app.log.info(bundle_mod)
        bundle_ids.append(bundle_mod)
        # Comma after bundle_mod is important for input as string
        process = Process(target=make_thread, args=(bundle_mod,))
        # thread = threading.Thread(target=write_index, args=(bundle_mod,))
        # threads.append(thread)
        # process = Process(target=write_index, args=(bundle_mod,))

        processes.append(process)
        process.start()
    # make sure that all processes have finished
    for process in processes:
        process.join()
    # for thread in threads:
    #     thread.start()
    # for thread in threads:
    #     thread.join()
    app.log.info({"bundle_id": bundle_ids})
    # return {"bundle_id": bundle_ids}
    return {"cron_bundle_ids": bundle_ids}


def make_thread(bundle_mod):
    app.log.info("make_thread %s", bundle_mod)
    thread = threading.Thread(target=write_index, args=(bundle_mod,))
    thread.start()
    thread.join()
    return thread

