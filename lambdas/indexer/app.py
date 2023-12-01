import logging
from typing import (
    Optional,
)

# noinspection PyPackageRequirements
import chalice
from chalice import (
    Response,
)

from azul import (
    CatalogName,
    cached_property,
    config,
)
from azul.chalice import (
    AzulChaliceApp,
)
from azul.deployment import (
    aws,
)
from azul.health import (
    HealthController,
)
from azul.hmac import (
    HMACAuthentication,
    SignatureHelper,
)
from azul.indexer.index_controller import (
    Action,
    IndexController,
)
from azul.indexer.log_forwarding_controller import (
    LogForwardingController,
)
from azul.indexer.notification_controller import (
    MonitoringController,
)
from azul.logging import (
    configure_app_logging,
)
from azul.openapi import (
    format_description as fd,
    params,
    schema,
)
from azul.openapi.responses import (
    json_content,
)
from azul.openapi.spec import (
    CommonEndpointSpecs,
)

log = logging.getLogger(__name__)

spec = {
    'openapi': '3.0.1',
    'info': {
        'title': config.indexer_name,
        'description': fd('''
            This is the internal API for Azul's indexer component.
        '''),
        # This property should be updated in any PR connected to an issue
        # labeled `API`. Increment the major version for backwards incompatible
        # changes and reset the minor version to zero. Otherwise, increment only
        # the minor version for backwards compatible changes. A backwards
        # compatible change is one that does not require updates to clients.
        'version': '1.0'
    }
}


class IndexerApp(AzulChaliceApp, SignatureHelper):

    @cached_property
    def health_controller(self):
        return self._controller(HealthController, lambda_name='indexer')

    @cached_property
    def monitoring_controller(self):
        return self._controller(MonitoringController)

    @cached_property
    def index_controller(self) -> IndexController:
        return self._controller(IndexController)

    @cached_property
    def log_controller(self) -> LogForwardingController:
        return self._controller(LogForwardingController)

    def __init__(self):
        super().__init__(app_name=config.indexer_name,
                         app_module_path=__file__,
                         # see LocalAppTestCase.setUpClass()
                         unit_test=globals().get('unit_test', False),
                         spec=spec)

    def log_forwarder(self, prefix: str):
        if config.enable_log_forwarding:
            return self.on_s3_event(bucket=aws.logs_bucket,
                                    events=['s3:ObjectCreated:*'],
                                    prefix=prefix)
        else:
            return lambda func: func

    @property
    def monitoring_sns_handler(self):
        if config.enable_monitoring:
            return self.on_sns_message(topic=aws.monitoring_topic_name)
        else:
            return lambda func: func

    def _authenticate(self) -> Optional[HMACAuthentication]:
        return self.auth_from_request(self.current_request)


app = IndexerApp()
configure_app_logging(app, log)


@app.route(
    '/',
    cors=True
)
def swagger_ui():
    return app.swagger_ui()


@app.route(
    '/static/{file}',
    cors=True
)
def static_resource(file):
    return app.swagger_resource(file)


common_specs = CommonEndpointSpecs(app_name='indexer')


@app.route(
    '/openapi',
    methods=['GET'],
    cors=True,
    **common_specs.openapi
)
def openapi():
    return Response(status_code=200,
                    headers={'content-type': 'application/json'},
                    body=app.spec())


@app.route(
    '/version',
    methods=['GET'],
    cors=True,
    **common_specs.version
)
def version():
    from azul.changelog import (
        compact_changes,
    )
    return {
        'git': config.lambda_git_status,
        'changes': compact_changes(limit=10)
    }


@app.route(
    '/health',
    methods=['GET'],
    cors=True,
    **common_specs.full_health
)
def health():
    return app.health_controller.health()


@app.route(
    '/health/basic',
    methods=['GET'],
    cors=True,
    **common_specs.basic_health
)
def basic_health():
    return app.health_controller.basic_health()


@app.route(
    '/health/cached',
    methods=['GET'],
    cors=True,
    **common_specs.cached_health
)
def cached_health():
    return app.health_controller.cached_health()


@app.route(
    '/health/fast',
    methods=['GET'],
    cors=True,
    **common_specs.fast_health
)
def fast_health():
    return app.health_controller.fast_health()


@app.route(
    '/health/{keys}',
    methods=['GET'],
    cors=True,
    **common_specs.custom_health
)
def health_by_key(keys: Optional[str] = None):
    return app.health_controller.custom_health(keys)


# FIXME: Remove redundant prefix from name
#        https://github.com/DataBiosphere/azul/issues/5337
@app.schedule(
    'rate(1 minute)',
    name='indexercachehealth'
)
def update_health_cache(_event: chalice.app.CloudWatchEvent):
    app.health_controller.update_cache()


@app.route(
    '/{catalog}/{action}',
    methods=['POST'],
    method_spec={
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
                        schema.enum(Action.add.name, Action.delete.name),
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
    return app.index_controller.handle_notification(catalog, action)


@app.threshold(
    errors=int(config.contribution_concurrency(retry=False) * 2 / 3),
    throttles=int(96000 / config.contribution_concurrency(retry=False))
)
@app.on_sqs_message(
    queue=config.notifications_queue_name(),
    batch_size=1
)
def contribute(event: chalice.app.SQSEvent):
    app.index_controller.contribute(event)


@app.threshold(
    errors=int(config.aggregation_concurrency(retry=False) * 3),
    throttles=int(12800 / config.aggregation_concurrency(retry=False))
)
@app.on_sqs_message(
    queue=config.tallies_queue_name(),
    batch_size=IndexController.document_batch_size
)
def aggregate(event: chalice.app.SQSEvent):
    app.index_controller.aggregate(event)


# Any messages in the tallies queue that fail being processed will be retried
# with more RAM in the tallies_retry queue.

@app.threshold(
    errors=int(config.aggregation_concurrency(retry=True) * 1 / 16),
    throttles=0
)
@app.on_sqs_message(
    queue=config.tallies_queue_name(retry=True),
    batch_size=IndexController.document_batch_size
)
def aggregate_retry(event: chalice.app.SQSEvent):
    app.index_controller.aggregate(event, retry=True)


# Any messages in the notifications queue that fail being processed will be
# retried with more RAM and a longer timeout in the notifications_retry queue.

@app.threshold(
    errors=int(config.contribution_concurrency(retry=True) * 1 / 4),
    throttles=int(15000 / config.contribution_concurrency(retry=True))
)
@app.on_sqs_message(
    queue=config.notifications_queue_name(retry=True),
    batch_size=1
)
def contribute_retry(event: chalice.app.SQSEvent):
    app.index_controller.contribute(event, retry=True)


@app.log_forwarder(
    config.alb_access_log_path_prefix(deployment=None)
)
def forward_alb_logs(event: chalice.app.S3Event):
    app.log_controller.forward_alb_logs(event)


@app.log_forwarder(
    config.s3_access_log_path_prefix(deployment=None)
)
def forward_s3_logs(event: chalice.app.S3Event):
    app.log_controller.forward_s3_access_logs(event)


@app.monitoring_sns_handler
def notify(event: chalice.app.SNSEvent):
    app.monitoring_controller.notify_group(event)
