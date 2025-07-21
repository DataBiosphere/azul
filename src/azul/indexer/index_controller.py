from collections.abc import (
    Iterable,
)
import http
import logging
from typing import (
    Any,
)
import uuid

import chalice
from chalice.app import (
    SQSRecord,
    UnauthorizedError,
)

from azul import (
    CatalogName,
    R,
    cached_property,
    config,
)
from azul.azulclient import (
    AzulClient,
)
from azul.chalice import (
    LambdaMetric,
)
from azul.hmac import (
    HMACAuthentication,
)
from azul.indexer import (
    BundlePartition,
)
from azul.indexer.action_controller import (
    ActionController,
)
from azul.indexer.index_queue_service import (
    DocumentTally,
    IndexAction,
    IndexQueueService,
)
from azul.openapi import (
    format_description as fd,
    params,
    schema,
)
from azul.openapi.responses import (
    json_content,
)
from azul.queues import (
    Queues,
    SQSFifoMessage,
)

log = logging.getLogger(__name__)


class IndexController(ActionController[IndexAction]):

    @cached_property
    def index_queue_service(self) -> IndexQueueService:
        return IndexQueueService()

    @cached_property
    def client(self) -> AzulClient:
        return AzulClient()

    def handlers(self) -> dict[str, Any]:
        @self.app.route(
            '/{catalog}/{action}',
            methods=['POST'],
            spec={
                'tags': ['Indexing'],
                'summary': 'Notify the indexer to perform an action on a bundle',
                'description': fd('''
                    Queue a bundle for addition to or deletion from the index.

                    The request must be authenticated using HMAC via the ``signature``
                    header. Each Azul deployment has its own unique HMAC key. The HMAC
                    components are the request method, request path, and the SHA256
                    digest of the request body.

                    A valid HMAC header proves that the client is in possession of the
                    secret HMAC key and that the request wasn't tampered with while
                    travelling between client and service, even though the latter is not
                    strictly necessary considering that TLS is used to encrypt the
                    entire exchange. Internal clients can obtain the secret key from the
                    environment they are running in, and that they share with the
                    service. External clients must have been given the secret key. The
                    now-defunct DSS was such an external client. The Azul indexer
                    provided the HMAC secret to DSS when it registered with DSS to be
                    notified about bundle additions/deletions. These days only internal
                    clients use this endpoint.
                '''),
                'requestBody': {
                    'description': 'Contents of the notification',
                    'required': True,
                    **json_content(schema.object(
                        bundle_fqid=schema.object(
                            uuid=str,
                            version=str,
                            source=schema.object(
                                id=str,
                                spec=str
                            )
                        )
                    ))
                },
                'parameters': [
                    params.path('catalog',
                                schema.enum(*config.catalogs),
                                description='The name of the catalog to notify.'),
                    params.path('action',
                                schema.enum(IndexAction.add.name, IndexAction.delete.name),
                                description='Which action to perform.'),
                    params.header('signature',
                                  str,
                                  description='HMAC authentication signature.')
                ],
                'responses': {
                    '200': {
                        'description': 'Notification was successfully queued for processing'
                    },
                    '400': {
                        'description': 'Request was rejected due to malformed parameters'
                    },
                    '401': {
                        'description': 'Request lacked a valid HMAC header'
                    }
                }
            }
        )
        def post_notification(catalog: CatalogName, action: str):
            """
            Receive a notification event and queue it for indexing or deletion.
            """
            return self.handle_notification(catalog, action)

        @self.app.metric_alarm(metric=LambdaMetric.errors,
                               threshold=int(config.contribution_concurrency(retry=False) * 2 / 3),
                               period=5 * 60)
        @self.app.metric_alarm(metric=LambdaMetric.throttles,
                               threshold=int(96000 / config.contribution_concurrency(retry=False)),
                               period=5 * 60)
        @self.app.on_sqs_message(
            queue=config.notifications_queue.name,
            batch_size=1
        )
        def contribute(event: chalice.app.SQSEvent):
            self.contribute(event)

        @self.app.metric_alarm(metric=LambdaMetric.errors,
                               threshold=int(config.aggregation_concurrency(retry=False) * 3),
                               period=5 * 60)
        @self.app.metric_alarm(metric=LambdaMetric.throttles,
                               threshold=int(37760 / config.aggregation_concurrency(retry=False)),
                               period=5 * 60)
        @self.app.on_sqs_message(
            queue=config.tallies_queue.name,
            batch_size=Queues.batch_size
        )
        def aggregate(event: chalice.app.SQSEvent):
            self.aggregate(event)

        # Any messages in the tallies queue that fail being processed will be
        # retried with more RAM in the tallies_retry queue.

        @self.app.metric_alarm(metric=LambdaMetric.errors,
                               threshold=int(config.aggregation_concurrency(retry=True) * 1 / 16),
                               period=5 * 60)
        @self.app.metric_alarm(metric=LambdaMetric.throttles,
                               threshold=0,
                               period=5 * 60)
        @self.app.on_sqs_message(
            queue=config.tallies_queue.to_retry.name,
            batch_size=Queues.batch_size
        )
        def aggregate_retry(event: chalice.app.SQSEvent):
            self.aggregate(event, retry=True)

        # Any messages in the notifications queue that fail being processed will
        # be retried with more RAM and a longer timeout in the
        # notifications_retry queue.

        @self.app.metric_alarm(metric=LambdaMetric.errors,
                               threshold=int(config.contribution_concurrency(retry=True) * 1 / 4),
                               period=5 * 60)
        @self.app.metric_alarm(metric=LambdaMetric.throttles,
                               threshold=int(31760 / config.contribution_concurrency(retry=True)),
                               period=5 * 60)
        @self.app.on_sqs_message(
            queue=config.notifications_queue.to_retry.name,
            batch_size=1
        )
        def contribute_retry(event: chalice.app.SQSEvent):
            self.contribute(event, retry=True)

        return locals()

    def handle_notification(self, catalog: CatalogName, action: str):
        request = self.current_request
        if isinstance(request.authentication, HMACAuthentication):
            assert request.authentication.identity() is not None
            try:
                config.Catalog.validate_name(catalog)
            except AssertionError as e:
                if R.caused(e):
                    raise R.propagate(e, chalice.BadRequestError)
            notification = request.json_body
            log.info('Received notification %r for catalog %r', notification, catalog)
            self._validate_notification(notification)
            service = self.index_queue_service
            message = service.index_bundle_message(self._load_action(action),
                                                   catalog,
                                                   notification['bundle_fqid'],
                                                   BundlePartition.root)
            service.queue_notification(message, retry=False)
            return chalice.app.Response(body='', status_code=http.HTTPStatus.ACCEPTED)
        else:
            raise UnauthorizedError()

    def _validate_notification(self, notification):
        try:
            bundle_fqid = notification['bundle_fqid']
        except KeyError:
            raise chalice.BadRequestError('Missing notification entry: bundle_fqid')

        try:
            bundle_uuid = bundle_fqid['uuid']
        except KeyError:
            raise chalice.BadRequestError('Missing notification entry: bundle_fqid.uuid')

        try:
            bundle_version = bundle_fqid['version']
        except KeyError:
            raise chalice.BadRequestError('Missing notification entry: bundle_fqid.version')

        if not isinstance(bundle_uuid, str):
            raise chalice.BadRequestError(f'Invalid type: uuid: {type(bundle_uuid)} (should be str)')

        if not isinstance(bundle_version, str):
            raise chalice.BadRequestError(f'Invalid type: version: {type(bundle_version)} (should be str)')

        if bundle_uuid.lower() != str(uuid.UUID(bundle_uuid)).lower():
            raise chalice.BadRequestError(f'Invalid syntax: {bundle_uuid} (should be a UUID)')

        if not bundle_version:
            raise chalice.BadRequestError('Invalid syntax: bundle_version can not be empty')

    def contribute(self, event: Iterable[SQSRecord], *, retry=False):
        self._handle_events(event, self.index_queue_service.contribute)

    def aggregate(self, event: Iterable[SQSRecord], *, retry=False):
        # Consolidate multiple tallies for the same entity and process entities
        # with only one message. Because SQS FIFO queues try to put as many
        # messages from the same message group in a reception batch, a single
        # message per group may indicate that that message is the last one in
        # the group. Inversely, multiple messages per group in a batch are a
        # likely indicator for the presence of even more queued messages in
        # that group. The more bundle contributions we defer, the higher the
        # amortized savings on aggregation become. Aggregating bundle
        # contributions is a costly operation for any entity with many
        # contributions e.g., a large project.
        #
        tallies = []
        for record in event:
            message = SQSFifoMessage.from_record(record)
            tally = DocumentTally.from_message(message)
            log.info('Attempt %i of handling %i contribution(s) for entity %s, '
                     'message ID %s, group ID %s',
                     tally.attempts, tally.num_contributions, tally.entity,
                     message.id, message.group_id)
            tallies.append(tally)
        try:
            self.index_queue_service.aggregate(tallies, retry=retry)
        except BaseException:
            # Note that another problematic outcome is for the Lambda invocation
            # to time out, in which case this log message will not be written.
            log.warning('Failed to aggregate tallies: %r', tallies, exc_info=True)
            raise
