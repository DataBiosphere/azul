"""
Chalice application module to receive and process DSS event notifications.
"""
import json
import logging
import time

import boto3
import chalice
from chalice import Chalice
from chalice.app import SQSEvent

from azul import config

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
        queue('notify').send_message(MessageBody=json.dumps(notification))
        log.info("Queued notification %r", notification)
    return {"status": "done"}


# Work around https://github.com/aws/chalice/issues/856

def new_handler(self, event, context):
    app.lambda_context = context
    return old_handler(self, event, context)


old_handler = chalice.app.EventSourceHandler.__call__
chalice.app.EventSourceHandler.__call__ = new_handler


def queue(queue_name):
    session = boto3.session.Session()  # See https://github.com/boto/boto3/issues/801
    queue_name = config.qualified_resource_name(queue_name)
    queue = session.resource('sqs').get_queue_by_name(QueueName=queue_name)
    return queue


@app.on_sqs_message(queue=config.qualified_resource_name('notify'), batch_size=1)
def index(event: SQSEvent):
    for record in event:
        notification = json.loads(record.body)
        attempts = record.to_dict()['attributes']['ApproximateReceiveCount']
        log.info(f'Worker handling notification {notification}, attempt #{attempts} (approx).')
        start = time.time()
        try:
            indexer.index(notification)
        except:
            log.warning(f"Worker failed to handle notification {notification}.", exc_info=True)
            raise
        else:
            duration = time.time() - start
            log.info(f'Worker successfully handled notification {notification} in {duration:.3f}s.')
