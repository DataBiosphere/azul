"""
Search CloudWatch logs for unredacted security tokens (APATs, OAuth access
tokens, refresh tokens, and authorization codes) in the currently selected
deployment.
"""
import argparse
from collections import (
    Counter,
)
from datetime import (
    datetime,
    timezone,
)
import logging
import time

from azul import (
    config,
)
from azul.args import (
    AzulArgumentHelpFormatter,
)
from azul.deployment import (
    aws,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)

secret_types = {
    'APAT': r"['\x22= ]ey[IJ][A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+",
    'access_token': r'ya29\.[A-Za-z0-9_-]{40,}',
    'refresh_token': r"['\x22= ]1\/\/[A-Za-z0-9_-]{20,}",
    'auth_code': r"['\x22= ]4\/[A-Za-z0-9_-]{60,}",
}

indexer_handler_names = [
    None,
    'aggregate',
    'aggregate_retry',
    'contribute',
    'contribute_retry',
    'forward_alb_logs',
    'forward_s3_logs',
    'indexercachehealth',
    'mirror',
    'notify',
]

service_handler_names = [
    None,
    'manifest',
    'publish_to_sns',
    'servicecachehealth',
]


def log_group_names() -> list[str]:
    return [
        f'/aws/lambda/{config.indexer_function_name(h)}'
        for h in indexer_handler_names
    ] + [
        f'/aws/lambda/{config.service_function_name(h)}'
        for h in service_handler_names
    ]


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=AzulArgumentHelpFormatter)
    parser.add_argument('start',
                        help='Start time (ISO 8601, e.g. 2026-06-11T17:35:27Z)')
    parser.add_argument('end',
                        nargs='?',
                        default=None,
                        help='End time (ISO 8601). Defaults to the current time.')
    args = parser.parse_args()
    configure_script_logging(log)
    start_time = int(datetime.fromisoformat(args.start).timestamp())
    if args.end is None:
        end_time = int(datetime.now(tz=timezone.utc).timestamp())
    else:
        end_time = int(datetime.fromisoformat(args.end).timestamp())
    all_groups = log_group_names()
    logs_client = aws.client('logs')
    groups = [
        group
        for group in all_groups
        if logs_client.describe_log_groups(
            logGroupNamePrefix=group,
            limit=1
        )['logGroups']
    ]
    log.info('Querying %i of %i log groups in %r',
             len(groups), len(all_groups), config.deployment_stage)
    query_ids = {}
    for secret_type, pattern in secret_types.items():
        query = (
            f'filter @message =~ /{pattern}/'
            f' | filter @message not like "REDACTED"'
            f' | filter @message not like "bucket_owner"'
            f' | fields @log, @message'
            f' | sort @timestamp asc'
            f' | limit 10000'
        )
        response = logs_client.start_query(logGroupNames=groups,
                                           startTime=start_time,
                                           endTime=end_time,
                                           queryString=query)
        query_ids[secret_type] = response['queryId']
        log.info('Started query for %s: %s', secret_type, response['queryId'])
    for secret_type, query_id in query_ids.items():
        while True:
            response = logs_client.get_query_results(queryId=query_id)
            if response['status'] == 'Complete':
                break
            else:
                log.debug('Query %s still %s', secret_type, response['status'])
                time.sleep(5)
        results = response['results']
        if results:
            counts_by_log = Counter[str]()
            for row in results:
                fields = {f['field']: f['value'] for f in row}
                log_group = fields.get('@log', '?')
                message = fields.get('@message', '?')
                counts_by_log[log_group] += 1
                log.warning('[%s] %s: %s', secret_type, log_group, message)
            log.warning('Summary for %s:', secret_type)
            for log_group, count in counts_by_log.most_common():
                log.warning('  %s: %i', log_group, count)
        else:
            log.info('No unredacted %s found', secret_type)


if __name__ == '__main__':
    main()
