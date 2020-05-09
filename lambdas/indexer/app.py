import logging
from typing import (
    Optional,
)

from boltons.cacheutils import cachedproperty
# noinspection PyPackageRequirements
import chalice

from azul import (
    config,
)
from azul.chalice import AzulChaliceApp
from azul.health import HealthController
from azul.indexer.index_controller import IndexController
from azul.logging import configure_app_logging

log = logging.getLogger(__name__)


class IndexerApp(AzulChaliceApp):

    @property
    def health_controller(self):
        # Don't cache. Health controller is meant to be short-lived since it
        # applies it's own caching. If we cached the controller, we'd never
        # observe any changes in health.
        return HealthController(lambda_name='indexer')

    @cachedproperty
    def index_controller(self):
        return IndexController()

    def __init__(self):
        super().__init__(app_name=config.indexer_name,
                         # see LocalAppTestCase.setUpClass()
                         unit_test=globals().get('unit_test', False))


app = IndexerApp()

configure_app_logging(app, log)


@app.route('/version', methods=['GET'], cors=True)
def version():
    from azul.changelog import compact_changes
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


@app.schedule('rate(1 minute)', name=config.indexer_cache_health_lambda_basename)
def update_health_cache(_event: chalice.app.CloudWatchEvent):
    app.health_controller.update_cache()


@app.route('/', cors=True)
def hello():
    return {'Hello': 'World!'}


@app.route('/delete', methods=['POST'])
@app.route('/', methods=['POST'])
def post_notification():
    """
    Receive a notification event and queue it for indexing or deletion.
    """
    return app.index_controller.handle_notification(app.current_request)


# Work around https://github.com/aws/chalice/issues/856

def new_handler(self, event, context):
    app.lambda_context = context
    return old_handler(self, event, context)


old_handler = chalice.app.EventSourceHandler.__call__
chalice.app.EventSourceHandler.__call__ = new_handler


@app.on_sqs_message(queue=config.notify_queue_name, batch_size=1)
def index(event: chalice.app.SQSEvent):
    app.index_controller.contribute(event)


@app.on_sqs_message(queue=config.document_queue_name, batch_size=IndexController.document_batch_size)
def write(event: chalice.app.SQSEvent):
    app.index_controller.aggregate(event)
