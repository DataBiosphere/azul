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
from chalicelib.indexer import FileIndexer
from chalicelib.dcc.dcc_indexer import DCCIndexer
from chalicelib.utils import DataExtractor
import json
import logging
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
es_index = os.environ.get('ES_INDEX', "azul-test-indexer")
es_doc_type = os.environ.get('ES_DOC_TYPE', "doc")
replica = os.environ.get("REPLICA", "aws")

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
    metadata_files, data_files = extractor.extract_bundle(payload, replica, will_include_urls=False)
    # Create an instance of the Indexers and run it
    file_indexer = FileIndexer(metadata_files,
                               data_files,
                               es,
                               '{}_file_v4'.format(es_index),
                               es_doc_type,
                               index_settings=es_settings,
                               index_mapping_config=index_mapping_config)
    file_indexer.index(bundle_uuid, bundle_version)
    dcc_indexer = DCCIndexer(metadata_files, data_files, es, index_settings=es_settings,
                             index_mapping_config=index_mapping_config)
    dcc_indexer.index(bundle_uuid, bundle_version)
    return {"status": "done"}


@app.route('/escheck')
def es_check():
    """Check the status of ElasticSearch by returning its info."""
    return json.dumps(es.info())
