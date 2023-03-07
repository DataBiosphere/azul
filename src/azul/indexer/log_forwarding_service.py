import csv
import gzip
from typing import (
    Iterator,
)

from botocore.response import (
    StreamingBody,
)

from azul import (
    require,
)
from azul.deployment import (
    aws,
)
from azul.types import (
    MutableJSON,
)


class LogForwardingService:
    """
    Parse logs from an Application Load Balancer.
    Adapted from https://github.com/rupertbg/aws-load-balancer-logs-to-cloudwatch
    """

    def read_logs(self, bucket: str, key: str) -> Iterator[MutableJSON]:
        response = aws.s3.get_object(Bucket=bucket, Key=key)
        body: StreamingBody = response['Body']
        with gzip.open(body, mode='rt', encoding='ascii') as f:
            # CSV format escapes the quotechar by repeating it. This cannot
            # occur in the logs because quotations marks occurring within the
            # field values are escaped. AWS does not document how the access
            # logs are encoded, but our experiments indicate that characters
            # including quotation marks, backslashes, and non-ASCII characters
            # are escaped using a syntax based on NGINX log format
            # (http://nginx.org/en/docs/http/ngx_http_log_module.html#log_format).
            # For example, quotation marks are represented as `\x22`.
            for row in csv.reader(f, delimiter=' ', quotechar='"'):
                # When new fields are introduced, they are added at the end of
                # the log entry, so observing more fields than expected does not
                # indicate a problem.
                require(len(row) >= len(self.fields), 'Missing expected fields')
                fields = dict(zip(self.fields, row))
                yield fields

    # https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html#access-log-entry-syntax
    fields = [
        'type',
        'time',
        'elb',
        'client:port',
        'target:port',
        'request_processing_time',
        'target_processing_time',
        'response_processing_time',
        'elb_status_code',
        'target_status_code',
        'received_bytes',
        'sent_bytes',
        'request',
        'user_agent',
        'ssl_cipher',
        'ssl_protocol',
        'target_group_arn',
        'trace_id',
        'domain_name',
        'chosen_cert_arn',
        'matched_rule_priority',
        'request_creation_time',
        'actions_executed',
        'redirect_url',
        'error_reason',
        'target:port_list',
        'target_status_code_list'
    ]
