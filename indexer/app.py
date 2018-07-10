# -*- coding: utf-8 -*-
"""App module to receive event notifications.

This chalice web services receives BlueBox event notifications
and triggers indexing of the bundle within the POST notification..

This module makes use of the indexer module and its components
to drive the indexing operation.

"""
from concurrent.futures import ThreadPoolExecutor, as_completed
import imp
import json
import logging
import math
import os
import sys
import time

import boto3
import chalice
from chalice import Chalice
from chalice.app import CloudWatchEvent
import requests.adapters

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'chalicelib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from utils.time import RemainingLambdaContextTime, RemainingTime

logging.basicConfig(level=logging.WARNING)
# FIXME: this should just be one top-level package called `azul`
log = logging.getLogger(__name__)
for top_level_pkg in (__name__, 'project', 'utils'):
    logging.getLogger(top_level_pkg).setLevel(logging.DEBUG)

app = Chalice(app_name=os.environ['AZUL_INDEXER_NAME'])
app.debug = True
app.log.setLevel(logging.DEBUG)  # please use module logger instead

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

dss_url = os.environ['AZUL_DSS_ENDPOINT']
loaded_properties = indexer_properties(dss_url, **kwargs)
loaded_indexer = indexer_to_load(loaded_properties)

num_workers = int(os.environ['AZUL_INDEX_WORKERS'])
num_dss_workers = int(os.environ['AZUL_DSS_WORKERS'])
requests.adapters.DEFAULT_POOLSIZE = num_workers * num_dss_workers


@app.route('/', methods=['POST'])
def post_notification():
    """
    Receive a notification event and either queue it for asynchronous indexing or process it synchronously.
    """
    notification = app.current_request.json_body
    log.info("Received notification %r", notification)
    params = app.current_request.query_params
    if params and params.get('sync', 'False').lower() == 'true':
        loaded_indexer.index(notification)
    else:
        queue().send_message(MessageBody=json.dumps(notification))
        log.info("Queued notification %r", notification)
    return {"status": "done"}


@app.route('/escheck')
def es_check():
    """Check the status of ElasticSearch by returning its info."""
    return json.dumps(loaded_properties.elastic_search_client.info())


# Work around https://github.com/aws/chalice/issues/856

def new_handler(self, event, context):
    app.lambda_context = context
    return old_handler(self, event, context)


old_handler = chalice.app.ScheduledEventHandler.__call__
chalice.app.ScheduledEventHandler.__call__ = new_handler


@app.schedule("rate(1 minute)", name='worker')
def index(event: CloudWatchEvent):
    log.info(f'Starting worker threads')
    remaining_time = RemainingLambdaContextTime(app.lambda_context)
    with ThreadPoolExecutor(num_workers) as tpe:
        futures = [tpe.submit(_index, i, remaining_time) for i in range(num_workers)]
        for future in as_completed(futures):
            e = future.exception()
            if e:
                log.error("Exception in worker thread", exc_info=e)
    log.info(f'Shutting down')


def queue():
    session = boto3.session.Session()  # See https://github.com/boto/boto3/issues/801
    queue_name = "azul-notify-" + os.environ['AZUL_DEPLOYMENT_STAGE']
    queue = session.resource("sqs").get_queue_by_name(QueueName=queue_name)
    return queue


def _index(worker: int, remaining_time: RemainingTime) -> None:
    _queue = queue()
    # Min. time to wait after this lambda execution finishes before another attempt should be made to process a
    # notification that failed to be processed in the current lambda execution. This is to make sure that 1) the next
    # attempt is not made in the same lambda execution in case there is something wrong with the current execution
    # and 2) to dissipate the worker's attention away from a potentially problematic notification.
    backoff_time = 10
    polling_time = 20  # SQS long-polling time, max. is 20
    indexing_time = 60  # estimated time for indexing one bundle, if less time is left we won't attempt the indexing
    shutdown_time = 5  # max. time it takes the lambda to shut down
    while polling_time + shutdown_time + indexing_time < remaining_time.get():
        visibility_timeout = remaining_time.get() + backoff_time
        messages = _queue.receive_messages(MaxNumberOfMessages=1,
                                           WaitTimeSeconds=polling_time,
                                           AttributeNames=['All'],
                                           MessageAttributeNames=['*'],
                                           VisibilityTimeout=int(math.ceil(visibility_timeout)))
        if messages and shutdown_time + indexing_time < remaining_time.get():
            assert len(messages) == 1
            message = messages[0]
            attempts = int(message.attributes['ApproximateReceiveCount'])
            new_visibility_timeout = remaining_time.get() + min(1000.0, backoff_time ** (attempts / 2))
            if new_visibility_timeout > visibility_timeout:
                message.change_visibility(VisibilityTimeout=int(new_visibility_timeout))
            notification = json.loads(message.body)
            log.info(f'Worker {worker} handling notification {notification}')
            start = time.time()
            loaded_indexer.index(notification)
            duration = time.time() - start
            log.info(f'Worker {worker} successfully handled notification {notification} in {duration:.3f}s.')
            message.delete()
    else:
        log.info(f"Exiting worker.")
