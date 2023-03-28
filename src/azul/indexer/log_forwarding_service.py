from abc import (
    ABCMeta,
    abstractmethod,
)
import csv
from datetime import (
    datetime,
)
import gzip
from typing import (
    Iterable,
    Iterator,
    Sequence,
)
import urllib.parse

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
        for message in self._parse_log_lines(body):
            message['_source_bucket'] = bucket
            message['_source_key'] = key
            yield message

    def _parse_log_lines(self, file_body: Iterable[str]) -> Iterator[MutableJSON]:
        # CSV format escapes the quotechar by repeating it. This cannot
        # occur in the logs because quotations marks occurring within the
        # field values are escaped. AWS does not document how the access
        # logs are encoded, but our experiments indicate that characters
        # including quotation marks, backslashes, and non-ASCII characters
        # are escaped when they occur in access logs. ALB logs using a syntax
        # based on NGINX log format
        # (http://nginx.org/en/docs/http/ngx_http_log_module.html#log_format),
        # while S3 logs use URL-encoding. For example, quotation marks are
        # represented as `\x22` and `%22` in ALB and S3 logs respectively.
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


class S3AccessLogForwardingService(LogForwardingService):
    """
    Parse access logs for S3.
    """

    def _read_log(self, response: StreamingBody) -> Iterable[str]:
        for line in response.iter_lines():
            yield line.decode('ascii')

    def _parse_log_lines(self, file_body: Iterable[str]) -> Iterator[MutableJSON]:
        for message in super()._parse_log_lines(file_body):
            # For some reason, AWS does not quote the `time` field,
            # which contains a space between the seconds and timezone offset.
            # All other fields appear to properly use quotes as needed.
            time = f"{message.pop('time_1')} {message.pop('time_2')}"
            # Verify that the restored field matches the expected format
            datetime.strptime(time, '[%d/%b/%Y:%H:%M:%S %z]')
            message['time'] = time.strip('[]')
            # Experiments indicate that the `key` field is url-encoded *twice*,
            # e.g., a quotation mark is represented as "%2522"
            message['key'] = urllib.parse.unquote(urllib.parse.unquote(message['key']))
            yield message

    @cached_property
    def fields(self) -> Sequence[str]:
        # https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html#log-record-fields
        return [
            'bucket_owner',
            'bucket',
            # See comment in `_read_logs`
            'time_1',
            'time_2',
            'remote_ip',
            'requester',
            'request_id',
            'operation',
            'key',
            'request_uri',
            'http_status',
            'error_code',
            'bytes_sent',
            'object_size',
            'total_time',
            'turn_around_time',
            'referer',
            'user_agent',
            'version_id',
            'host_id',
            'signature_version',
            'cipher_suite',
            'authentication_type',
            'host_header',
            'tls_version',
            'access_point_arn',
            'acl_required',
        ]
