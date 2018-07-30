# -*- coding: utf-8 -*-
"""App module to receive event notifications.

This chalice web services receives BlueBox event notifications
and triggers indexing of the bundle within the POST notification..

This module makes use of the indexer module and its components
to drive the indexing operation.

"""
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import logging
import math
import time

import boto3
import chalice
from chalice import Chalice
from chalice.app import CloudWatchEvent
import requests.adapters

from azul import config
from azul.time import RemainingLambdaContextTime, RemainingTime

logging.basicConfig(level=logging.WARNING)
log = logging.getLogger(__name__)
for top_level_pkg in (__name__, 'azul'):
    logging.getLogger(top_level_pkg).setLevel(logging.DEBUG)

app = Chalice(app_name=config.indexer_name)
app.debug = True
app.log.setLevel(logging.DEBUG)  # please use module logger instead

# Initialize the project-specific plugin
#
plugin = config.plugin()
properties = plugin.IndexProperties(dss_url=config.dss_endpoint,
                                    es_endpoint=config.es_endpoint)
indexer = plugin.Indexer(properties)

requests.adapters.DEFAULT_POOLSIZE = config.num_workers * config.num_workers


@app.route('/version', methods=['GET'], cors=True)
def version():
    return {
        'git': config.git_status
    }


@app.route('/health', methods=['GET'], cors=True)
def health():
    from azul.health import get_elasticsearch_health, get_queue_health

    return {
        'status': 'UP',
        'elasticsearch': get_elasticsearch_health(),
        'queues': get_queue_health()
    }


@app.route('/', methods=['POST'])
def post_notification():
    """
    Receive a notification event and either queue it for asynchronous indexing or process it synchronously.
    """
    notification = app.current_request.json_body
    log.info("Received notification %r", notification)
    params = app.current_request.query_params
    if params and params.get('sync', 'False').lower() == 'true':
        indexer.index(notification)
    else:
        queue().send_message(MessageBody=json.dumps(notification))
        log.info("Queued notification %r", notification)
    return {"status": "done"}


@app.route('/escheck')
def es_check():
    """Check the status of ElasticSearch by returning its info."""
    return json.dumps(properties.elastic_search_client.info())


# Work around https://github.com/aws/chalice/issues/856

def new_handler(self, event, context):
    app.lambda_context = context
    return old_handler(self, event, context)


old_handler = chalice.app.ScheduledEventHandler.__call__
chalice.app.ScheduledEventHandler.__call__ = new_handler


@app.schedule("rate(4 minutes)", name='worker')
def index(event: CloudWatchEvent):
    log.info(f'Starting worker threads')
    remaining_time = RemainingLambdaContextTime(app.lambda_context)
    with ThreadPoolExecutor(config.num_workers) as tpe:
        futures = [tpe.submit(_index, i, remaining_time) for i in range(config.num_workers)]
        for future in as_completed(futures):
            e = future.exception()
            if e:
                log.error("Exception in worker thread", exc_info=e)
    log.info(f'Shutting down')


def queue():
    session = boto3.session.Session()  # See https://github.com/boto/boto3/issues/801
    queue_name = "azul-notify-" + config.deployment_stage
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
            log.info(f'Worker {worker} handling notification {notification}, attempt #{attempts} (approx).')
            start = time.time()
            try:
                indexer.index(notification)
            except:
                log.warning(f"Worker {worker} failed to handle notification {notification}.", exc_info=True)
            else:
                duration = time.time() - start
                log.info(f'Worker {worker} successfully handled notification {notification} in {duration:.3f}s.')
                message.delete()
    else:
        log.info(f"Exiting worker.")
