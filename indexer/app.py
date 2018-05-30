# -*- coding: utf-8 -*-
"""App module to receive event notifications.

This chalice web services receives BlueBox event notifications
and triggers indexing of the bundle within the POST notification..

This module makes use of the indexer module and its components
to drive the indexing operation.

"""
from aws_requests_auth import boto_utils
from aws_requests_auth.aws_auth import AWSRequestsAuth
from chalice import Chalice
from elasticsearch import Elasticsearch, RequestsHttpConnection
from chalicelib.indexer import FileIndexerV5
# BundleOrientedIndexer as BundleIndexer, \
# AssayOrientedIndexer as AssayIndexer, \
# SampleOrientedIndexer as SampleIndexer, \
# ProjectOrientedIndexer as ProjectIndexer
from chalicelib.utils import DataExtractor
from utils.indexer import BaseIndexer
import json
import logging
import imp
import os

# Set up the chalice application
app = Chalice(app_name=os.getenv('INDEXER_NAME', 'dss-indigo'))
app.debug = True
app.log.setLevel(logging.DEBUG)
# Set env on lambda, chalice config and profile
# Get the ElasticSearch and BlueBox host
es_host = os.environ.get('ES_ENDPOINT', "localhost")
es_port = os.environ.get("ES_PORT", 9200)
bb_host = "https://" + os.environ.get('BLUE_BOX_ENDPOINT',
                                      "dss.staging.data.humancellatlas.org/v1")
# es_index = os.environ.get('ES_INDEX', "azul-test-indexer")
es_doc_type = os.environ.get('ES_DOC_TYPE', "doc")
replica = os.environ.get("REPLICA", "aws")

# get which indexer project definition to use
#https://lkubuntu.wordpress.com/2012/10/02/writing-a-python-plugin-api/
IndexerPluginDirectory = "./project"
IndexerModule = "indexer"
indexer_project = os.environ.get('INDEXER_PROJECT', 'hca')

def importProjects():
    projects = []
    possible_projects = os.listdir(IndexerPluginDirectory)
    for project in possible_projects:
        location = os.path.join(IndexerPluginDirectory, project)
        if not os.path.isdir(location) or not IndexerModule + ".py" in os.listdir(location):
            continue
        info = imp.find_module(IndexerModule, [location])
        projects.append({"name": project, "info": info})
    return projects

def loadProject(project):
    return imp.load_module(IndexerModule, *project["info"])

def load_indexer_class():
    for i in importProjects():
        if i['name'] == indexer_project:
            IndexerLoaded = getattr(loadProject(i), "Indexer")
            #setup default constructor - similar to V5Indexer constructor
            my_indexer = IndexerLoaded()
            if isinstance(my_indexer, BaseIndexer):
                #return the class
                return IndexerLoaded
            else:
                raise NotImplementedError("Project supplied does not have an indexer derived from the BaseIndexer")

# Get settings for elasticsearch
with open('chalicelib/settings.json') as f:
    es_settings = json.loads(f.read())

with open('chalicelib/config.json') as f:
    index_mapping_config = json.loads(f.read())

# Choose which Elasticsearch to use
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
    es = Elasticsearch([{'host': es_host, 'port': es_port}])


# for blue box notification
@app.route('/', methods=['GET', 'POST'])
def post_notification():
    """
    Receive the notification event.

    This function takes in a POST event from the blue box and
    triggers the whole indexing process.

    :return: bundle_uuid
    """
    # blue box sends json with post request; get payload
    payload = app.current_request.json_body
    app.log.info("Received notification %s", payload)
    # look within request for the bundle_uuid and version
    bundle_uuid = payload['match']['bundle_uuid']
    bundle_version = payload['match']['bundle_version']
    # Create a DataExtractor instance
    extractor = DataExtractor(bb_host)
    # Extract the relevant files and metadata to the bundle
    metadata_files, data_files = extractor.extract_bundle(payload, replica)
    # Create an instance of the Indexers and run it
    IndexerToLoad = load_indexer_class()
    my_indexer = IndexerToLoad()

    # TODO: Replace with a call to the constructor of the indexer loaded in the previous step
    file_indexer = FileIndexerV5(metadata_files,
                                 data_files,
                                 es,
                                 'file_index_v5',
                                 es_doc_type,
                                 index_settings=es_settings,
                                 index_mapping_config=index_mapping_config)
    # bundle_indexer = BundleIndexer(metadata_files,
    #                                data_files,
    #                                es,
    #                                "bundle_index_v4",
    #                                "doc",
    #                                index_settings=es_settings,
    #                                index_mapping_config=index_mapping_config)
    # assay_indexer = AssayIndexer(metadata_files,
    #                              data_files,
    #                              es,
    #                              "assay_index_v4",
    #                              "doc",
    #                              index_settings=es_settings,
    #                              index_mapping_config=index_mapping_config)
    # sample_indexer = SampleIndexer(metadata_files,
    #                                data_files,
    #                                es,
    #                                "sample_index_v4",
    #                                "doc",
    #                                index_settings=es_settings,
    #                                index_mapping_config=index_mapping_config)
    #
    # project_indexer = ProjectIndexer(metadata_files,
    #                                  data_files,
    #                                  es,
    #                                  "project_index_v4",
    #                                  "doc",
    #                                  index_settings=es_settings,
    #                                  index_mapping_config=index_mapping_config)

    file_indexer.index(bundle_uuid, bundle_version)
    # bundle_indexer.index(bundle_uuid, bundle_version)
    # assay_indexer.index(bundle_uuid, bundle_version)
    # sample_indexer.index(bundle_uuid, bundle_version)
    # project_indexer.index(bundle_uuid, bundle_version)
    return {"status": "done"}


@app.route('/escheck')
def es_check():
    """Check the status of ElasticSearch by returning its info."""
    return json.dumps(es.info())
