"""
Delete BigQuery reservation resources if no ongoing reindex is detected.
"""

import argparse
from datetime import (
    datetime,
    timedelta,
)
import sys
from typing import (
    Dict,
    FrozenSet,
    Iterable,
    List,
)

from azul import (
    RequirementError,
    cached_property,
    config,
    logging,
)
from azul.bigquery_reservation import (
    SlotManager,
)
from azul.deployment import (
    aws,
)
from azul.logging import (
    configure_script_logging,
)
from azul.modules import (
    load_app_module,
)
from args import (
    AzulArgumentHelpFormatter,
)

log = logging.getLogger(__name__)


class ReindexDetector:
    # Minutes
    interval = 5

    # Maximum number of contribution Lambda invocations per interval for a
    # reindexing to be considered inactive
    threshold = 0

    @cached_property
    def _cloudwatch(self):
        return aws.client('cloudwatch')

    @cached_property
    def _lambda(self):
        return aws.client('lambda')

    def is_reindex_active(self) -> bool:
        functions = self._list_contribution_lambda_functions()
        reindex_active = False
        for (function,
             num_invocations) in self._lambda_invocation_counts(functions).items():
            description = (f'{function}: {num_invocations} invocations in '
                           f'the last {self.interval} minutes')
            if num_invocations > self.threshold:
                log.info(f'Active reindex for {description}')
                reindex_active = True
                # Keep looping to log status of remaining lambdas
            else:
                log.debug(f'No active reindex for {description}')
        return reindex_active

    def _list_contribution_lambda_functions(self) -> List[str]:
        """
        Search Lambda functions for the names of contribution Lambdas.
        """
        contribution_lambdas = []
        paginator = self._lambda.get_paginator('list_functions')
        for response in paginator.paginate():
            for function in response['Functions']:
                function_name = function['FunctionName']
                if self._is_contribution_lambda(function_name):
                    contribution_lambdas.append(function_name)
        return contribution_lambdas

    @cached_property
    def _contribution_lambda_names(self) -> FrozenSet[str]:
        indexer = load_app_module('indexer')
        return frozenset((
            indexer.contribute.lambda_name,
            indexer.contribute_retry.lambda_name
        ))

    def _is_contribution_lambda(self, function_name: str) -> bool:
        for lambda_name in self._contribution_lambda_names:
            try:
                resource_name, _ = config.unqualified_resource_name(function_name,
                                                                    suffix=lambda_name)
            except RequirementError:
                pass
            else:
                if resource_name == 'indexer':
                    return True
        return False

    def _lambda_invocation_counts(self, function_names: Iterable[str]) -> Dict[str, int]:
        end = datetime.utcnow()
        start = end - timedelta(minutes=self.interval)
        response = self._cloudwatch.get_metric_data(
            MetricDataQueries=[
                {
                    'Id': f'invocation_count_{i}',
                    'Label': function_name,
                    'MetricStat': {
                        'Metric': {
                            'Namespace': 'AWS/Lambda',
                            'MetricName': 'Invocations',
                            'Dimensions': [{
                                'Name': 'FunctionName',
                                'Value': function_name
                            }]
                        },
                        'Period': 60 * self.interval,
                        'Stat': 'Sum'
                    }
                }
                for i, function_name in enumerate(function_names)
            ],
            StartTime=start,
            EndTime=end,
        )
        return {m['Label']: sum(m['Values']) for m in response['MetricDataResults']}


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=AzulArgumentHelpFormatter,
                                     add_help=True)
    parser.add_argument('--dry-run',
                        action='store_true',
                        help='Report status without altering resources')
    args = parser.parse_args(argv)

    # Listing BigQuery reservations is quicker than checking for an active
    # reindex, hence the order of checks
    slot_manager = SlotManager(dry_run=args.dry_run)
    if slot_manager.has_active_slots():
        monitor = ReindexDetector()
        if not monitor.is_reindex_active():
            slot_manager.ensure_slots_deleted()
    else:
        log.info('No slots are currently reserved.')


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
