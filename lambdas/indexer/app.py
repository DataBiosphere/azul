"""
Chalice application module to receive and process DSS event notifications.
"""
from collections import (
    Counter,
    defaultdict,
)
from datetime import datetime
import http
from itertools import chain
import json
import logging
import random
import time
from typing import (
    List,
    MutableMapping,
    Optional,
)
import uuid

import boto3
# noinspection PyPackageRequirements
import chalice
from dataclasses import (
    asdict,
    dataclass,
    replace,
)
from more_itertools import (
    chunked,
    partition,
)

from azul import (
    config,
    hmac,
)
from azul.azulclient import AzulClient
from azul.chalice import AzulChaliceApp
from azul.health import HealthController
from azul.logging import configure_app_logging
from azul.plugin import Plugin
from azul.time import RemainingLambdaContextTime
from azul.transformer import EntityReference
from azul.types import JSON

log = logging.getLogger(__name__)

app = AzulChaliceApp(app_name=config.indexer_name)

configure_app_logging(app, log)

plugin = Plugin.load()


@app.route('/version', methods=['GET'], cors=True)
def version():
    from azul.changelog import compact_changes
    return {
        'git': config.lambda_git_status,
        'changes': compact_changes(limit=10)
    }


@app.route('/health', methods=['GET'], cors=True)
@app.route('/health/{keys}', methods=['GET'], cors=True)
def health(keys: Optional[str] = None):
    controller = HealthController(lambda_name='indexer',
                                  keys=keys,
                                  request_path=app.current_request.context['path'])
    return controller.response()


@app.schedule('rate(1 minute)', name=config.indexer_cache_health_lambda_basename)
def generate_health_object(_event: chalice.app.CloudWatchEvent):
    controller = HealthController(lambda_name='indexer')
    controller.generate_cache()


@app.route('/', cors=True)
def hello():
    return {'Hello': 'World!'}


@app.route('/delete', methods=['POST'])
@app.route('/', methods=['POST'])
def post_notification():
    """
    Receive a notification event and queue it for indexing or deletion.
    """
    hmac.verify(current_request=app.current_request)
    notification = app.current_request.json_body
    log.info("Received notification %r", notification)
    validate_request_syntax(notification)
    if app.current_request.context['path'] == '/':
        return process_notification('add', notification)
    elif app.current_request.context['path'] in ('/delete', '/delete/'):
        return process_notification('delete', notification)
    else:
        assert False


def process_notification(action: str, notification: JSON):
    if config.test_mode:
        if 'test_name' not in notification:
            log.error('Rejecting non-test notification in test mode: %r.', notification)
            raise chalice.ChaliceViewError('The indexer is currently in test mode where it only accepts specially '
                                           'instrumented notifications. Please try again later')
    else:
        if 'test_name' in notification:
            log.error('Rejecting test notification in production mode: %r.', notification)
            raise chalice.BadRequestError('Cannot process test notifications outside of test mode')

    message = dict(action=action, notification=notification)
    notify_queue = queue(config.notify_queue_name)
    notify_queue.send_message(MessageBody=json.dumps(message))
    log.info("Queued notification %r", notification)
    return chalice.app.Response(body='', status_code=http.HTTPStatus.ACCEPTED)


def validate_request_syntax(notification):
    try:
        match = notification['match']
    except KeyError:
        raise chalice.BadRequestError('Missing notification entry: match')

    try:
        bundle_uuid = match['bundle_uuid']
    except KeyError:
        raise chalice.BadRequestError('Missing notification entry: bundle_uuid')

    try:
        bundle_version = match['bundle_version']
    except KeyError:
        raise chalice.BadRequestError('Missing notification entry: bundle_version')

    if not isinstance(bundle_uuid, str):
        raise chalice.BadRequestError(f'Invalid type: bundle_uuid: {type(bundle_uuid)} (should be str)')

    if not isinstance(bundle_version, str):
        raise chalice.BadRequestError(f'Invalid type: bundle_version: {type(bundle_version)} (should be str)')

    if bundle_uuid.lower() != str(uuid.UUID(bundle_uuid)).lower():
        raise chalice.BadRequestError(f'Invalid syntax: {bundle_uuid} (should be a UUID)')

    if not bundle_version:
        raise chalice.BadRequestError('Invalid syntax: bundle_version can not be empty')


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
            action = message['action']
            if action == 'reindex':
                AzulClient.do_remote_reindex(message)
            else:
                notification = message['notification']
                indexer_cls = plugin.indexer_class()
                indexer = indexer_cls()
                if action == 'add':
                    contributions = indexer.transform(notification, delete=False)
                elif action == 'delete':
                    contributions = indexer.transform(notification, delete=True)
                else:
                    assert False

                log.info("Writing %i contributions to index.", len(contributions))
                tallies = indexer.contribute(contributions)
                tallies = [DocumentTally.for_entity(entity, num_contributions)
                           for entity, num_contributions in tallies.items()]

                log.info("Queueing %i entities for aggregating a total of %i contributions.",
                         len(tallies), sum(tally.num_contributions for tally in tallies))
                token_queue = queue(config.token_queue_name)
                document_queue = queue(config.document_queue_name)
                for batch in chunked(tallies, document_batch_size):
                    token_queue.send_message(MessageBody=json.dumps(Token.mint(len(batch)).to_json()))
                    document_queue.send_messages(Entries=[dict(tally.to_message(), Id=str(i))
                                                          for i, tally in enumerate(batch)])
        except BaseException:
            log.warning(f"Worker failed to handle message {message}.", exc_info=True)
            raise
        else:
            duration = time.time() - start
            log.info(f'Worker successfully handled message {message} in {duration:.3f}s.')


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

token_lifetime: float = 60 * 60


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
                                               MaxNumberOfMessages=document_batch_size)
    log.info('Received %i messages for %i token(s) with a total value of %i',
             len(messages), len(tokens), total)
    assert len(messages) <= document_batch_size

    if messages:
        _log_document_grouping(messages)

        # Consolidate multiple tallies for the same entity and process entities with only one message. Because SQS FIFO
        # queues try to put as many messages from the same message group in a reception batch, a single message per
        # group may indicate that that message is the last one in the group. Inversely, multiple messages per group
        # in a batch are a likely indidicator for the presence of even more queued messages in that group. The more
        # bundle contributions we defer, the higher the amortized savings on aggregation become. Aggregating bundle
        # contributions is a costly operation for any entity with many contributions e.g., a large project.
        #
        tallies_by_id: MutableMapping[EntityReference, List[DocumentTally]] = defaultdict(list)
        for message in messages:
            tally = DocumentTally.from_message(message)
            log.info('Attempt %i of handling %i contribution(s) for entity %s/%s',
                     tally.attempts, tally.num_contributions, tally.entity.entity_type, tally.entity.entity_id)
            tallies_by_id[tally.entity].append(tally)
        deferrals, referrals = [], []
        for tallies in tallies_by_id.values():
            if len(tallies) == 1:
                referrals.append(tallies[0])
            elif len(tallies) > 1:
                deferrals.append(tallies[0].consolidate(tallies[1:]))
            else:
                assert False

        if referrals:
            for tally in referrals:
                log.info('Aggregating %i contribution(s) to entity %s/%s',
                         tally.num_contributions, tally.entity.entity_type, tally.entity.entity_id)
            indexer_cls = plugin.indexer_class()
            indexer = indexer_cls()
            tallies = {tally.entity: tally.num_contributions for tally in referrals}
            indexer.aggregate(tallies)

        if deferrals:
            for tally in deferrals:
                log.info('Deferring aggregation of %i contribution(s) to entity %s/%s',
                         tally.num_contributions, tally.entity.entity_type, tally.entity.entity_id)
            document_queue.send_messages(Entries=[dict(tally.to_message(), Id=str(i))
                                                  for i, tally in enumerate(deferrals)])

        document_queue.delete_messages(Entries=[dict(Id=str(i), ReceiptHandle=message.receipt_handle)
                                                for i, message in enumerate(chain(messages))])
        total -= len(messages)
        total += len(deferrals)
    else:
        tokens, expired_tokens = map(list, partition(Token.expired, tokens))
        if expired_tokens:
            expired_total = sum(token.value for token in expired_tokens)
            log.info('Expiring %i token(s) with a total value of %i', len(expired_tokens), expired_total)
            total -= expired_total

    if total > 0:
        expiration = max(token.expiration for token in tokens)
        tokens = Token.mint_many(total, expiration=expiration)
        delay = 0 if messages else round(random.uniform(30, 90))
        log.info('Recirculating %i token(s), after a delay of %is, for a total value of %i, expiring at %s',
                 len(tokens), delay, total, datetime.utcfromtimestamp(expiration).isoformat(timespec='seconds') + 'Z')
        _send_tokens(tokens, delay=delay)
    elif total < 0:
        # Discarding token deficit will lead to an overall surplus of token value which will expire at some point.
        log.info("Discarding token deficit of %i.", total)


@dataclass(frozen=True)
class DocumentTally:
    """
    Tracks the number of bundle contributions to a particular metadata entity.

    Each instance represents a message in the document queue.
    """
    entity: EntityReference
    num_contributions: int
    attempts: int

    @classmethod
    def from_message(cls, message) -> 'DocumentTally':
        body = json.loads(message.body)
        return cls(entity=EntityReference(entity_type=body['entity_type'],
                                          entity_id=body['entity_id']),
                   num_contributions=body['num_contributions'],
                   attempts=int(message.attributes['ApproximateReceiveCount']))

    @classmethod
    def for_entity(cls, entity: EntityReference, num_contributions: int) -> 'DocumentTally':
        return cls(entity=entity,
                   num_contributions=num_contributions,
                   attempts=0)

    def to_json(self) -> JSON:
        return {
            'entity_type': self.entity.entity_type,
            'entity_id': self.entity.entity_id,
            'num_contributions': self.num_contributions
        }

    def to_message(self) -> JSON:
        return dict(MessageBody=json.dumps(self.to_json()),
                    MessageGroupId=self.entity.entity_id,
                    MessageDeduplicationId=str(uuid.uuid4()))

    def consolidate(self, others: List['DocumentTally']) -> 'DocumentTally':
        assert all(self.entity == other.entity for other in others)
        return replace(self, num_contributions=sum((other.num_contributions for other in others),
                                                   self.num_contributions))


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

    >>> Token.mint(5) # doctest: +ELLIPSIS
    Token(uuid='...', value=5, expiration=...)

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
    log.info('Document grouping for messages received: %r', dict(message_group_sizes))


@app.schedule('rate(10 minutes)')
def nudge(event: chalice.app.CloudWatchEvent):
    """
    Work around token deficit. The hypothesis is that SQS trigger is dropping messages because I cannot see any other
    way in which the current implementation could result in a token deficit. There are two places where tokens are
    sent: index() and write(). In index() tokens are sent before documents, so a crash or exception would result in a
    token surplus. In write(), they are sent after the document messages have been deleted, to recirculate unused
    token value. But if an exception or crash prevents that from happening, SQS trigger should return all tokens,
    used and unused ones, which should also result in a token surplus.

    So what we do here is periodically check if more tokens are needed and mint them if necessary.
    """
    assert event is not None  # avoid linter warning
    num_tokens, num_documents = (sum(int(queue(queue_name).attributes['ApproximateNumberOfMessages' + k])
                                     for k in ('', 'Delayed', 'NotVisible'))
                                 for queue_name in (config.token_queue_name, config.document_queue_name))
    if num_documents and not num_tokens:
        tokens = Token.mint_many(num_documents)
        log.info('Nudging queue with %i token(s) for a total value of %i', len(tokens), num_documents)
        for batch in chunked(tokens, 10):
            _send_tokens(batch)
