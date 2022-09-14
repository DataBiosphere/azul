import logging
from typing import (
    Optional,
)

# noinspection PyPackageRequirements
import chalice

from azul import (
    CatalogName,
    cached_property,
    config,
    hmac,
)
from azul.chalice import (
    AzulChaliceApp,
)
from azul.health import (
    HealthController,
)
from azul.hmac import (
    HMACAuthentication,
)
from azul.indexer.index_controller import (
    IndexController,
)
from azul.logging import (
    configure_app_logging,
)

log = logging.getLogger(__name__)


class IndexerApp(AzulChaliceApp):

    @cached_property
    def health_controller(self):
        return self._controller(HealthController, lambda_name='indexer')

    @cached_property
    def index_controller(self) -> IndexController:
        return self._controller(IndexController)

    def __init__(self):
        super().__init__(app_name=config.indexer_name,
                         app_module_path=__file__,
                         # see LocalAppTestCase.setUpClass()
                         unit_test=globals().get('unit_test', False))

    def _authenticate(self) -> Optional[HMACAuthentication]:
        return hmac.auth_from_request(self.current_request)


app = IndexerApp()

configure_app_logging(app, log)


@app.route('/version', methods=['GET'], cors=True)
def version():
    from azul.changelog import (
        compact_changes,
    )
    return {
        'git': config.lambda_git_status,
        'changes': compact_changes(limit=10)
    }


@app.route('/health', methods=['GET'], cors=True)
def health():
    return app.health_controller.health()


@app.route('/health/basic', methods=['GET'], cors=True)
def basic_health():
    return app.health_controller.basic_health()


@app.route('/health/cached', methods=['GET'], cors=True)
def cached_health():
    return app.health_controller.cached_health()


@app.route('/health/fast', methods=['GET'], cors=True)
def fast_health():
    return app.health_controller.fast_health()


@app.route('/health/{keys}', methods=['GET'], cors=True)
def health_by_key(keys: Optional[str] = None):
    return app.health_controller.custom_health(keys)


@app.schedule('rate(1 minute)', name='indexercachehealth')
def update_health_cache(_event: chalice.app.CloudWatchEvent):
    app.health_controller.update_cache()


@app.route('/', cors=True)
def hello():
    return {'Hello': 'World!'}


@app.route('/{catalog}/{action}', methods=['POST'])
def post_notification(catalog: CatalogName, action: str):
    """
    Receive a notification event and queue it for indexing or deletion.
    """
    return app.index_controller.handle_notification(catalog, action)


@app.on_sqs_message(queue=config.notifications_queue_name(), batch_size=1)
def contribute(event: chalice.app.SQSEvent):
    app.index_controller.contribute(event)


@app.on_sqs_message(queue=config.tallies_queue_name(),
                    batch_size=IndexController.document_batch_size)
def aggregate(event: chalice.app.SQSEvent):
    app.index_controller.aggregate(event)


# Any messages in the tallies queue that fail being processed will be retried
# with more RAM in the tallies_retry queue.

@app.on_sqs_message(queue=config.tallies_queue_name(retry=True),
                    batch_size=IndexController.document_batch_size)
def aggregate_retry(event: chalice.app.SQSEvent):
    app.index_controller.aggregate(event, retry=True)


# Any messages in the notifications queue that fail being processed will be
# retried with more RAM and a longer timeout in the notifications_retry queue.

@app.on_sqs_message(queue=config.notifications_queue_name(retry=True),
                    batch_size=1)
def contribute_retry(event: chalice.app.SQSEvent):
    app.index_controller.contribute(event, retry=True)
