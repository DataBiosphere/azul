import logging
from typing import (
    Optional,
)

import chalice

from azul import (
    JSON,
    cached_property,
    config,
)
from azul.chalice import (
    LambdaMetric,
)
from azul.deployment import (
    aws,
)
from azul.health import (
    HealthApp,
)
from azul.hmac import (
    HMACAuthentication,
    SignatureHelper,
)
from azul.indexer.index_controller import (
    IndexController,
)
from azul.indexer.log_forwarding_controller import (
    LogForwardingController,
)
from azul.indexer.mirror_controller import (
    MirrorController,
)
from azul.logging import (
    configure_app_logging,
)
from azul.openapi import (
    format_description as fd,
)
from azul.types import (
    not_none,
)

log = logging.getLogger(__name__)

spec: JSON = {
    'openapi': '3.0.1',
    'info': {
        'title': config.indexer_name,
        # The version property should be updated in any PR connected to an issue
        # labeled `API`. Increment the major version for backwards incompatible
        # changes and reset the minor version to zero. Otherwise, increment only
        # the minor version for backwards compatible changes. A backwards
        # compatible change is one that does not require updates to clients.
        'version': '3.3',
        'description': fd('''
            This is the internal API for Azul's indexer component.
        ''')
    }
}


class IndexerApp(HealthApp, SignatureHelper):

    @cached_property
    def index_controller(self) -> IndexController:
        return IndexController(app=self)

    @cached_property
    def mirror_controller(self) -> MirrorController:
        return MirrorController(app=self)

    @cached_property
    def log_controller(self) -> LogForwardingController:
        return LogForwardingController(app=self)

    def __init__(self):
        super().__init__(app_name=config.indexer_name,
                         globals=globals(),
                         spec=spec)

    def log_forwarder(self, prefix: str):
        if config.enable_log_forwarding:
            s3_decorator = self.on_s3_event(bucket=aws.logs_bucket,
                                            events=['s3:ObjectCreated:*'],
                                            prefix=prefix)
            error_decorator = self.metric_alarm(metric=LambdaMetric.errors,
                                                threshold=1,  # One alarm …
                                                period=24 * 60 * 60)  # … per day.
            throttle_decorator = self.metric_alarm(metric=LambdaMetric.throttles,
                                                   threshold=0,
                                                   period=5 * 60)
            retry_decorator = self.retry(num_retries=2)

            def decorator(f):
                return retry_decorator(throttle_decorator(error_decorator(s3_decorator(f))))

            return decorator
        else:
            return lambda func: func

    def _authenticate(self) -> Optional[HMACAuthentication]:
        return self.auth_from_request(not_none(self.current_request))


app = IndexerApp()
configure_app_logging(app, log)

globals().update(app.default_routes())

globals().update(app.index_controller.handlers())


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


globals().update(app.mirror_controller.handlers())
