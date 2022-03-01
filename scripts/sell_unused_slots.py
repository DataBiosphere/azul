"""
Delete BigQuery reservation resources if no ongoing reindex is detected.
"""

import argparse
from datetime import (
    datetime,
    timedelta,
)
import sys
import time
from typing import (
    Dict,
    List,
)

import attr

from azul import (
    cache,
    config,
    logging,
)
from azul.args import (
    AzulArgumentHelpFormatter,
)
from azul.bigquery_reservation import (
    BigQueryReservation,
)
from azul.deployment import (
    aws,
)
from azul.lambdas import (
    Lambda,
    Lambdas,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class ReindexDetector:
    location: str

    # Minutes
    interval = 5

    # Maximum number of contribution Lambda invocations per interval for a
    # reindexing to be considered inactive
    threshold = 0

    def is_reindex_active(self) -> bool:
        active = False
        for (function,
             num_invocations) in self._lambda_invocation_counts().items():
            description = (f'{function.name}: {num_invocations} invocations in '
                           f'the last {self.interval} minutes in location '
                           f'{function.slot_location}')
            if num_invocations > self.threshold:
                log.info(f'Active reindex for {description}')
                active = True
                # Keep looping to log status of remaining lambdas
            else:
                log.debug(f'No active reindex for {description}')
        return active

    @classmethod
    @cache
    def _list_contribution_lambda_functions(cls) -> List[Lambda]:
        """
        Search Lambda functions for the names of contribution Lambdas.
        """
        return [
            lambda_
            for lambda_ in Lambdas().list_lambdas()
            if lambda_.is_contribution_lambda
        ]

    def _lambda_invocation_counts(self) -> Dict[Lambda, int]:
        end = datetime.utcnow()
        start = end - timedelta(minutes=self.interval)
        lambdas_by_name = {
            lambda_.name: lambda_ for lambda_ in self._list_contribution_lambda_functions()
            if lambda_.slot_location == self.location
        }
        if lambdas_by_name:
            response = aws.cloudwatch.get_metric_data(
                MetricDataQueries=[
                    {
                        'Id': f'invocation_count_{i}',
                        'Label': lambda_.name,
                        'MetricStat': {
                            'Metric': {
                                'Namespace': 'AWS/Lambda',
                                'MetricName': 'Invocations',
                                'Dimensions': [{
                                    'Name': 'FunctionName',
                                    'Value': lambda_.name
                                }]
                            },
                            'Period': 60 * self.interval,
                            'Stat': 'Sum'
                        }
                    }
                    for i, lambda_ in enumerate(lambdas_by_name.values())
                ],
                StartTime=start,
                EndTime=end,
            )
            return {
                lambdas_by_name[m['Label']]: sum(m['Values'])
                for m in response['MetricDataResults']
            }
        else:
            log.info('No contribution lambdas in the current project are '
                     'configured to use location %r', self.location)
            return {}


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=AzulArgumentHelpFormatter,
                                     add_help=True)
    parser.add_argument('--dry-run',
                        action='store_true',
                        help='Report status without altering resources')
    args = parser.parse_args(argv)

    for location in config.tdr_allowed_source_locations:
        sell_unused_slots(location, args.dry_run)


def sell_unused_slots(location: str, dry_run: bool):
    # Listing BigQuery reservations is quicker than checking for an active
    # reindex, hence the order of checks
    reservation = BigQueryReservation(location=location, dry_run=dry_run)
    is_active = reservation.is_active
    if is_active is False:
        log.info('No slots are currently reserved in location %r.', location)
    elif is_active is True:
        min_reservation_age = 30 * 60
        reservation_age = time.time() - reservation.update_time
        assert reservation_age > 0, reservation_age
        if reservation_age < min_reservation_age:
            # Avoid race with recently started reindexing
            log.info('Reservation in location %r was updated %r < %r seconds ago; '
                     'taking no action.', location, reservation_age, min_reservation_age)
        elif not ReindexDetector(location=location).is_reindex_active():
            reservation.deactivate()
    elif is_active is None:
        log.warning('BigQuery slot commitment state in location %r is '
                    'inconsistent. Dangling resources will be deleted.',
                    location)
        reservation.deactivate()
    else:
        assert False


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
