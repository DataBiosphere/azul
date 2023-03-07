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
    LogForwardingService,
)


class LogForwardingController(AppController):
    """
    Forward logs from an Application Load Balancer (ALB) to standard output.
    When this behavior is invoked via an AWS Lambda function, the output is
    forwarded to the default CloudWatch log group associated with the function
    """

    @cached_property
    def service(self) -> LogForwardingService:
        return LogForwardingService()

    def forward_logs(self, event: chalice.app.S3Event) -> None:
        # FIXME: Create alarm for log forwarding failures
        #        https://github.com/DataBiosphere/azul/issues/4997
        for message in self.service.read_logs(event.bucket, event.key):
            json.dump(message, sys.stdout)
            sys.stdout.write('\n')
            sys.stdout.flush()
