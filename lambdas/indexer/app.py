"""
Chalice application module to receive and process DSS event notifications.
"""
from collections import Counter
from datetime import datetime
import http
import json
import logging
import random
import time
from typing import List
import uuid

import boto3
# noinspection PyPackageRequirements
import chalice
from dataclasses import asdict, dataclass
from elasticsearch import JSONSerializer
from more_itertools import chunked, partition

from azul import config
from azul.indexer import DocumentsById
from azul.plugin import Plugin
from azul.time import RemainingLambdaContextTime
from azul.transformer import ElasticSearchDocument
from azul.types import JSON

logging.basicConfig(level=logging.WARNING)
log = logging.getLogger(__name__)
for top_level_pkg in (__name__, 'azul'):
    logging.getLogger(top_level_pkg).setLevel(logging.DEBUG)

app = chalice.Chalice(app_name=config.indexer_name)
app.debug = True
app.log.setLevel(logging.DEBUG)  # please use module logger instead

plugin = Plugin.load()


@app.route('/version', methods=['GET'], cors=True)
def version():
    from azul.changelog import compact_changes
    return {
        'git': config.git_status,
        'changes': compact_changes(limit=10)
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
        indexer_cls = plugin.indexer_class()
        indexer = indexer_cls()
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
    return boto3.resource('sqs').get_queue_by_name(QueueName=queue_name)


@app.on_sqs_message(queue=config.notify_queue_name, batch_size=1)
def index(event: chalice.app.SQSEvent):
    for record in event:
        message = json.loads(record.body)
        attempts = record.to_dict()['attributes']['ApproximateReceiveCount']
        log.info(f'Worker handling message {message}, attempt #{attempts} (approx).')
        start = time.time()
        try:
            indexer_cls = plugin.indexer_class()
            indexer = indexer_cls(handle_documents)
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
    sqs = boto3.client('sqs')
    token_queue = queue(config.token_queue_name)
    document_queue = queue(config.document_queue_name)

    def message(doc):
        return dict(MessageBody=json_serializer.dumps(doc.to_json()),
                    MessageGroupId=doc.document_id,
                    MessageDeduplicationId=str(uuid.uuid4()))

    for batch in chunked(documents_by_id.values(), document_batch_size):
        value = len(batch)
        token_queue.send_message(MessageBody=json_serializer.dumps(Token.mint(value).to_json()))
        try:
            document_queue.send_messages(Entries=[dict(message(doc), Id=str(i)) for i, doc in enumerate(batch)])
        except sqs.exceptions.BatchRequestTooLong:
            log.info('Message batch was too big. Sending messages individually.', exc_info=True)
            for doc in batch:
                document_queue.send_message(**message(doc))


# The number of documents to be queued in a single SQS `send_messages`. Theoretically, larger batches are better but
# SQS currently limits the batch size to 10.
#
document_batch_size = 10

# The maximum number of tokens to be processed by a single Lambda invocation. This should be at least 2 to allow for
# token recombination to occur (two smaller tokens being merged into one) and to increase the chance that the
# combined token value is 10. It must be at most 10 because of a limit imposed by SQS and Lambda. The higher this
# value, the more token reconcilation will occur at the expense of increased token churn (unused token value being
# returned to the queue). One token can be at most document_batch_size in value, and one Lambda invocation consumes
# at most document_batch_size in token value so retrieving ten tokens may cause nine tokens to be returned.
#
token_batch_size = 2

token_lifetime: float = 10 * 60 * 60


@app.on_sqs_message(queue=config.token_queue_name, batch_size=token_batch_size)
def write(event: chalice.app.SQSEvent):
    remaining_time = RemainingLambdaContextTime(app.lambda_context)
    tokens = [Token.from_json(json.loads(token.body)) for token in event]
    total = sum(token.value for token in tokens)
    assert 0 < total <= document_batch_size * token_batch_size
    document_queue = queue(config.document_queue_name)
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

        indexer_cls = plugin.indexer_class()
        indexer = indexer_cls()
        documents_by_id = indexer.collate(documents)
        # Merge documents into index, without retries (let SQS take care of that)
        indexer.write(documents_by_id, conflict_retry_limit=0, error_retry_limit=0)

        document_queue.delete_messages(Entries=[dict(Id=str(i), ReceiptHandle=message.receipt_handle)
                                                for i, message in enumerate(messages)])
        total -= len(messages)
    else:
        tokens, expired_tokens = map(list, partition(Token.expired, tokens))
        if expired_tokens:
            expired_total = sum(token.value for token in expired_tokens)
            log.info('Expiring %i token(s) with a total value of %i', len(expired_tokens), expired_total)
            total -= expired_total

    assert total >= 0

    if total:
        expiration = max(token.expiration for token in tokens)
        tokens = Token.mint_many(total, expiration=expiration)
        delay = 0 if messages else round(random.uniform(30, 90))
        log.info('Recirculating %i token(s), after a delay of %is, for a total value of %i, expiring at %s',
                 len(tokens), delay, total, datetime.utcfromtimestamp(expiration).isoformat(timespec='seconds') + 'Z')
        _send_tokens(tokens, delay=delay)


@dataclass(frozen=True)
class Token:
    """
    >>> Token.mint(0)
    Traceback (most recent call last):
    ...
    ValueError: 0

    >>> Token.mint(11)
    Traceback (most recent call last):
    ...
    ValueError: 11

    >>> [t.value for t in Token.mint_many(0)]
    []

    >>> [t.value for t in Token.mint_many(10)]
    [10]

    >>> [t.value for t in Token.mint_many(11)]
    [10, 1]

    >>> Token.mint(10, expiration=time.time()).expired()
    True

    >>> Token.mint(10, expiration=time.time()+100).expired()
    False

    >>> t = Token.mint(5)
    >>> c = Token.from_json(t.to_json())
    >>> c == t, c is t
    (True, False)
    """
    uuid: str
    value: int
    expiration: float

    @classmethod
    def mint(cls, value, expiration=None) -> 'Token':
        if value < 1 or value > document_batch_size:
            raise ValueError(value)
        if expiration is None:
            expiration = time.time() + token_lifetime
        return cls(uuid=str(uuid.uuid4()), value=value, expiration=expiration)

    @classmethod
    def from_json(cls, json: JSON) -> 'Token':
        return cls(**json)

    def to_json(self) -> JSON:
        return asdict(self)

    @classmethod
    def mint_many(cls, total, expiration=None) -> List['Token']:
        return [cls.mint(value, expiration=expiration)
                for value in _dispense_tokens(document_batch_size, total)]

    def expired(self):
        return self.expiration <= time.time()


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


def _send_tokens(tokens, delay=0):
    queue(config.token_queue_name).send_messages(Entries=[dict(Id=str(i),
                                                               DelaySeconds=delay,
                                                               MessageBody=json.dumps(token.to_json()))
                                                          for i, token in enumerate(tokens)])


def _log_document_grouping(messages):
    message_group_sizes = Counter()
    for message in messages:
        message_group_sizes[message.attributes['MessageGroupId']] += 1
    log.info('Document grouping for received messages: %r', dict(message_group_sizes))


@app.schedule('rate(10 minutes)')
def nudge(event: chalice.app.CloudWatchEvent):
    """
    Work around token deficit (https://github.com/DataBiosphere/azul/issues/390). Current hypothesis is that SQS
    trigger is dropping messages because I cannot see how the current code could result in a token deficit. There are
    two places where tokens are sent: handle_documents() and write(). In handle_documents() they are sent before the
    documents, so a crash or exception would result in a token surplus. In write(), they are sent after the document
    messages have been deleted, to return unused tokens. But if an exception or crash prevents the tokens to be
    returned, SQS trigger should return all tokens (used and unused) which should also result in a token surplus.

    So what we do here is periodically check if more tokens are needed and mint them if necessary.
    """
    num_tokens, num_documents = (sum(int(queue(queue_name).attributes['ApproximateNumberOfMessages' + k])
                                     for k in ('', 'Delayed', 'NotVisible'))
                                 for queue_name in (config.token_queue_name, config.document_queue_name))
    if num_documents and not num_tokens:
        tokens = Token.mint_many(num_documents)
        log.info('Nudging queue with %i token(s) for a total value of %i', len(tokens), num_documents)
        for batch in chunked(tokens, 10):
            _send_tokens(batch)
