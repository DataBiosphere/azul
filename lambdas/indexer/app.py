"""
Chalice application module to receive and process DSS event notifications.
"""
from collections import Counter
import http
import json
import logging
import time
import uuid

import boto3
# noinspection PyPackageRequirements
import chalice
from elasticsearch import JSONSerializer
from more_itertools import chunked

from azul import config
from azul.indexer import DocumentsById
from azul.time import RemainingLambdaContextTime
from azul.transformer import ElasticSearchDocument

logging.basicConfig(level=logging.WARNING)
log = logging.getLogger(__name__)
for top_level_pkg in (__name__, 'azul'):
    logging.getLogger(top_level_pkg).setLevel(logging.DEBUG)

app = chalice.Chalice(app_name=config.indexer_name)
app.debug = True
app.log.setLevel(logging.DEBUG)  # please use module logger instead

# Initialize the project-specific plugin
#
plugin = config.plugin()
properties = plugin.IndexProperties(dss_url=config.dss_endpoint,
                                    es_endpoint=config.es_endpoint)


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
        indexer = plugin.Indexer(properties)
        indexer.index(notification)
    else:
        message = _make_message(action='add', notification=notification)
        notify_queue = queue(config.notify_queue_name)
        notify_queue.send_message(MessageBody=message)
        log.info("Queued notification %r", notification)
    return {"status": "done"}


@app.route('/delete', methods=['POST'])
def delete_notification():
    """
    Receive a deletion event and process it asynchronously
    """
    notification = app.current_request.json_body
    log.info("Received deletion notification %r", notification)
    message = _make_message(action='delete', notification=notification)
    notify_queue = queue(config.notify_queue_name)
    notify_queue.send_message(MessageBody=message)
    log.info("Queued notification %r", notification)

    return chalice.app.Response(body='', status_code=http.HTTPStatus.ACCEPTED)


def _make_message(action, notification):
    if action not in ['add', 'delete']:
        raise ValueError(action)
    return json.dumps({'action': action, 'notification': notification})


# Work around https://github.com/aws/chalice/issues/856

def new_handler(self, event, context):
    app.lambda_context = context
    return old_handler(self, event, context)


old_handler = chalice.app.EventSourceHandler.__call__
chalice.app.EventSourceHandler.__call__ = new_handler


def queue(queue_name):
    session = boto3.session.Session()  # See https://github.com/boto/boto3/issues/801
    queue = session.resource('sqs').get_queue_by_name(QueueName=queue_name)
    return queue


@app.on_sqs_message(queue=config.notify_queue_name, batch_size=1)
def index(event: chalice.app.SQSEvent):
    for record in event:
        message = json.loads(record.body)
        attempts = record.to_dict()['attributes']['ApproximateReceiveCount']
        log.info(f'Worker handling message {message}, attempt #{attempts} (approx).')
        start = time.time()
        try:
            indexer = plugin.Indexer(properties, handle_documents)
            action = message['action']
            notification = message['notification']
            if action == 'add':
                indexer.index(notification)
            if action == 'delete':
                indexer.delete(notification)
        except:
            log.warning(f"Worker failed to handle message {message}.", exc_info=True)
            raise
        else:
            duration = time.time() - start
            log.info(f'Worker successfully handled message {message} in {duration:.3f}s.')


def handle_documents(documents_by_id: DocumentsById) -> None:
    log.info("Queueing %i document(s) for indexing.", len(documents_by_id))
    json_serializer = JSONSerializer()  # Elasticsearch's serializer translates UUIDs
    for batch in chunked(documents_by_id.values(), document_batch_size):
        value = len(batch)
        document_queue = queue(config.document_queue_name)
        token_queue = queue(config.token_queue_name)
        document_queue.send_messages(Entries=[dict(MessageBody=json_serializer.dumps(doc.to_json()),
                                                   MessageGroupId=doc.document_id,
                                                   MessageDeduplicationId=str(uuid.uuid4()),
                                                   Id=str(i))
                                              for i, doc in enumerate(batch)])
        # One might think that we'd want to avoid token debt by queueing tokens *before* documents. However,
        # this is not a good idea: the queueing of documents is more likely to fail due to batch size constraints
        # etc. The resulting surplus in tokens would cause those excess tokens to circulate indefinitely,
        # eating up SQS and Lambda fees. Tokens aren't returned via visibility timeout (or raising an exception) but
        # rather by requeueing new tokens of equivalent value.
        token_queue.send_message(MessageBody=json_serializer.dumps(dict(token=uuid.uuid4(), value=value)))


# The number of documents to be queued in a single SQS `send_messages`. Theoretically, larger batches are better but
# SQS currently limits the batch size to 10.
#
document_batch_size = 10

# The maximum number of tokens to be processed by a single Lambda invocation. This should be at least 2 to allow for
# token reconciliation to occur (two smaller tokens being merged into one). It must be at most 10 because of a limit
# imposed by SQS and Lambda. The higher this value, the more token reconcilation will occur at the expense of
# increased token churn (unused token value being returned to the queue). One token can be at most
# document_batch_size in value, and one Lambda invocation consumes at most document_batch_size in token value so
# retrieving ten tokens may cause nine tokens to be returned.
#
token_batch_size = 2


@app.on_sqs_message(queue=config.token_queue_name, batch_size=token_batch_size)
def write(event: chalice.app.SQSEvent):
    remaining_time = RemainingLambdaContextTime(app.lambda_context)
    tokens = [json.loads(token.body) for token in event]
    total = sum(token['value'] for token in tokens)
    assert 0 < total <= document_batch_size * token_batch_size
    document_queue = queue(config.document_queue_name)
    token_queue = queue(config.token_queue_name)
    messages = document_queue.receive_messages(WaitTimeSeconds=20,
                                               AttributeNames=['ApproximateReceiveCount', 'MessageGroupId'],
                                               VisibilityTimeout=round(remaining_time.get()) + 10,
                                               MaxNumberOfMessages=min(document_batch_size, total))
    log.info('Received %i messages for %i token(s) with a total value of %i',
             len(messages), len(tokens), total)
    assert len(messages) <= document_batch_size

    if messages:
        _log_document_grouping(messages)
        documents = []
        for message in messages:
            document = ElasticSearchDocument.from_json(json.loads(message.body))
            attempts = int(message.attributes['ApproximateReceiveCount'])
            assert len(document.bundles) == 1
            bundle = document.bundles[0]
            log.info('Attempt %i of writing document %s/%s from bundle %s, version %s',
                     attempts, document.entity_type, document.document_id, bundle.uuid, bundle.version)
            documents.append(document)

        indexer = plugin.Indexer(properties)
        documents_by_id = indexer.consolidate(documents)
        # Merge documents into index, without retries (let SQS take care of that)
        indexer.write(documents_by_id, conflict_retry_limit=0, error_retry_limit=0)

        document_queue.delete_messages(Entries=[dict(Id=str(i), ReceiptHandle=message.receipt_handle)
                                                for i, message in enumerate(messages)])
        total -= len(messages)

    assert total >= 0

    if total:
        tokens = [dict(token=str(uuid.uuid4()), value=value)
                  for value in _dispense_tokens(document_batch_size, total)]
        log.info('Returning %i token(s) for a total value of %i', len(tokens), total)
        token_queue.send_messages(Entries=[dict(Id=str(i), MessageBody=json.dumps(token))
                                           for i, token in enumerate(tokens)])


def _log_document_grouping(messages):
    message_group_sizes = Counter()
    for message in messages:
        message_group_sizes[message.attributes['MessageGroupId']] += 1
    log.info('Document grouping for received messages: %r', dict(message_group_sizes))


def _dispense_tokens(size, total):
    """
    >>> _dispense_tokens(3, 0)
    []
    >>> _dispense_tokens(3, 1)
    [1]
    >>> _dispense_tokens(3, 3)
    [3]
    >>> _dispense_tokens(3, 4)
    [3, 1]
    """
    return [min(i, size) for i in range(total, 0, -size)]
