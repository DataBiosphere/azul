import json
import sys

import chalice.app

from azul import (
    cached_property,
)
from azul.chalice import (
    AppController,
)
from azul.indexer.log_forwarding_service import (
    ALBLogForwardingService,
    LogForwardingService,
)


class LogForwardingController(AppController):
    """
    Forward logs from an Application Load Balancer (ALB) to standard output.
    When this behavior is invoked via an AWS Lambda function, the output is
    forwarded to the default CloudWatch log group associated with the function
    """

    @cached_property
    def alb(self) -> LogForwardingService:
        return ALBLogForwardingService()

    def _forward_logs(self, event: chalice.app.S3Event, service: LogForwardingService) -> None:
        # FIXME: Create alarm for log forwarding failures
        #        https://github.com/DataBiosphere/azul/issues/4997
        for message in service.read_logs(event.bucket, event.key):
            json.dump(message, sys.stdout)
            sys.stdout.write('\n')
            sys.stdout.flush()

    def forward_alb_logs(self, event: chalice.app.S3Event) -> None:
        self._forward_logs(event, self.alb)
