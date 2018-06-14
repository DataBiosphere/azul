# -*- coding: utf-8 -*-
"""App module to receive event notifications.

This chalice web services receives BlueBox event notifications
and triggers indexing of the bundle within the POST notification..

This module makes use of the indexer module and its components
to drive the indexing operation.

"""
from chalice import Chalice
import imp
import json
import logging
import os
import sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'chalicelib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

# from utils.indexer import BaseIndexer
# from utils.base_config import BaseIndexProperties

# Set up the chalice application
app = Chalice(app_name=os.environ['AZUL_INDEXER_NAME'])
app.debug = True
app.log.setLevel(logging.DEBUG)
dss_url = os.environ['AZUL_DSS_ENDPOINT']

# get which indexer project definition to use
# https://lkubuntu.wordpress.com/2012/10/02/writing-a-python-plugin-api/
IndexerPluginDirectory = "./project" if os.path.exists("./project") else "chalicelib/project"
IndexerModule = "indexer"
ConfigModule = "config"
indexer_project = os.environ.get('INDEXER_PROJECT', 'hca')


def import_projects():
    projects = []
    possible_projects = os.listdir(IndexerPluginDirectory)
    for project in possible_projects:
        location = os.path.join(IndexerPluginDirectory, project)
        if not os.path.isdir(location) or not IndexerModule + ".py" in os.listdir(location):
            continue
        info = imp.find_module(IndexerModule, [location])
        projects.append({"name": project, "info": info})
    return projects


def import_projects_configs():
    projects = []
    possible_projects = os.listdir(IndexerPluginDirectory)
    for project in possible_projects:
        location = os.path.join(IndexerPluginDirectory, project)
        if not os.path.isdir(location) or not ConfigModule + ".py" in os.listdir(location):
            continue
        info = imp.find_module(ConfigModule, [location])
        projects.append({"name": project, "info": info})
    return projects


def load_project(project):
    return imp.load_module(IndexerModule, *project["info"])


def load_project_config(project):
    return imp.load_module("config", *project["info"])


def load_config(project):
    return imp.load_module(ConfigModule, *project["info"])


def load_indexer_class():
    from utils.indexer import BaseIndexer
    for i in import_projects():
        if i['name'] == indexer_project:
            indexer_loaded = getattr(load_project(i), "Indexer")
            # setup default constructor - similar to V5Indexer constructor
            if issubclass(indexer_loaded, BaseIndexer):
                # return the class
                return indexer_loaded
            else:
                raise NotImplementedError("Project supplied does not have an indexer derived from the BaseIndexer")


def load_config_class():
    from utils.base_config import BaseIndexProperties
    for i in import_projects_configs():
        if i['name'] == indexer_project:
            config_loaded = getattr(load_project_config(i), "IndexProperties")
            # setup default constructor - similar to V5Indexer constructor
            if issubclass(config_loaded, BaseIndexProperties):
                # return the class
                return config_loaded
            else:
                raise NotImplementedError("Project supplied does not have an config derived from BaseIndexProperties")


indexer_to_load = load_indexer_class()
indexer_properties = load_config_class()
try:
    es_endpoint = os.environ['AZUL_ES_ENDPOINT']
except KeyError:
    kwargs = dict(es_domain=os.environ['AZUL_ES_DOMAIN'])
else:
    host, _, port = es_endpoint.partition(':')
    kwargs = dict(es_endpoint=(host, int(port)))

loaded_properties = indexer_properties(dss_url, **kwargs)
loaded_indexer = indexer_to_load(loaded_properties)


@app.route('/', methods=['POST'])
def post_notification():
    """
    Receive the notification event.

    This function takes in a POST event from the blue box and
    triggers the whole indexing process.

    """
    # blue box sends json with post request; get payload
    payload = app.current_request.json_body
    app.log.info("Received notification %s", payload)
    # Index based on the payload
    loaded_indexer.index(payload)
    return {"status": "done"}


@app.route('/escheck')
def es_check():
    """Check the status of ElasticSearch by returning its info."""
    return json.dumps(loaded_properties.elastic_search_client.info())
