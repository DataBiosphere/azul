from abc import (
    ABCMeta,
    abstractmethod,
)
import csv
import gzip
from typing import (
    Iterable,
    Iterator,
    Sequence,
)

from botocore.response import (
    StreamingBody,
)

from azul import (
    cached_property,
    require,
)
from azul.deployment import (
    aws,
)
from azul.types import (
    MutableJSON,
)


class LogForwardingService(metaclass=ABCMeta):

    def read_logs(self, bucket: str, key: str) -> Iterator[MutableJSON]:
        response = aws.s3.get_object(Bucket=bucket, Key=key)
        body = self._read_log(response['Body'])
        return self._parse_log_lines(body)

    def _parse_log_lines(self, file_body: Iterable[str]) -> Iterator[MutableJSON]:
        # CSV format escapes the quotechar by repeating it. This cannot
        # occur in the logs because quotations marks occurring within the
        # field values are escaped. AWS does not document how the access
        # logs are encoded, but our experiments indicate that characters
        # including quotation marks, backslashes, and non-ASCII characters
        # are escaped using a syntax based on NGINX log format
        # (http://nginx.org/en/docs/http/ngx_http_log_module.html#log_format).
        # For example, quotation marks are represented as `\x22`.
        for row in csv.reader(file_body, delimiter=' ', quotechar='"'):
            # When new fields are introduced, they are added at the end of
            # the log entry, so observing more fields than expected does not
            # indicate a problem.
            require(len(row) >= len(self.fields), 'Missing expected fields')
            fields = dict(zip(self.fields, row))
            yield fields

    @abstractmethod
    def _read_log(self, response: StreamingBody) -> Iterable[str]:
        """
        Read the given body of an object from the log bucket and return the
        individual lines contained therein.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def fields(self) -> Sequence[str]:
        raise NotImplementedError


class ALBLogForwardingService(LogForwardingService):
    """
    Parse logs from an Application Load Balancer.
    Adapted from https://github.com/rupertbg/aws-load-balancer-logs-to-cloudwatch
    """

    def _read_log(self, response: StreamingBody) -> Iterable[str]:
        with gzip.open(response, mode='rt', encoding='ascii') as f:
            yield from f

    # https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html#access-log-entry-syntax
    @cached_property
    def fields(self) -> Sequence[str]:
        return [
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
