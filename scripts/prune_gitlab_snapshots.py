"""
Delete old snapshots of the EBS data volume attached to the GitLab instance in
the currently selected deployment.
"""

import argparse
import calendar
from datetime import (
    datetime,
    timezone,
)
import logging
import sys
from typing import (
    TYPE_CHECKING,
)

from azul import (
    config,
)
from azul.args import (
    AzulArgumentHelpFormatter,
)
from azul.deployment import (
    aws,
)
from azul.lib import (
    R,
)
from azul.logging import (
    configure_script_logging,
)

if TYPE_CHECKING:
    from mypy_boto3_ec2.type_defs import (
        FilterTypeDef,
        SnapshotTypeDef,
    )

log = logging.getLogger(__name__)


def main(argv: list[str]):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=AzulArgumentHelpFormatter)
    parser.add_argument('--delete',
                        default=False,
                        action='store_true',
                        help='Actually delete the snapshots that are not to be '
                             'kept, instead of just listing them.')
    args = parser.parse_args(argv)
    assert config.terraform_component == 'gitlab', R(
        "Select the 'gitlab' component ('dev.gitlab' or 'prod.gitlab', for example)."
    )
    now = datetime.now(timezone.utc)
    plan = prune_plan(gitlab_snapshots(), now)
    for snapshot, keep, period in plan:
        log.info('%s %s %s %-7s %s',
                 'Keep  ' if keep else 'Delete',
                 snapshot['SnapshotId'],
                 snapshot['StartTime'].isoformat(timespec='seconds'),
                 period or 'recent',
                 snapshot['Description'])
    doomed = [snapshot['SnapshotId'] for snapshot, keep, _ in plan if not keep]
    log.info('Keeping %i and deleting %i of %i snapshots',
             len(plan) - len(doomed), len(doomed), len(plan))
    if args.delete:
        for snapshot_id in doomed:
            log.info('Deleting snapshot %r …', snapshot_id)
            aws.ec2.delete_snapshot(SnapshotId=snapshot_id)
    elif doomed:
        log.info('Rerun with --delete to delete them')


def gitlab_snapshots() -> list[SnapshotTypeDef]:
    paginator = aws.ec2.get_paginator('describe_snapshots')
    filters: list[FilterTypeDef] = [
        {'Name': 'tag:Name', 'Values': ['azul-gitlab']},
        {'Name': 'tag:deployment', 'Values': [config.deployment_stage]}
    ]
    return [
        snapshot
        for page in paginator.paginate(OwnerIds=['self'], Filters=filters)
        for snapshot in page['Snapshots']
    ]


def prune_plan(snapshots: list[SnapshotTypeDef],
               now: datetime
               ) -> list[tuple[SnapshotTypeDef, bool, str | None]]:
    """
    Decide which of the given snapshots to keep: every snapshot from the last 6
    months, the latest snapshot of each month for the 12 months before that,
    the latest snapshot of each quarter for the 12 months before that, and the
    latest snapshot of each year for anything older.

    Return a tuple for each of the given snapshots, newest first, consisting of
    the snapshot, whether to keep it and the period it represents, or None if
    the snapshot is recent enough to be kept unconditionally. Because the latest
    snapshot of a quarter or year is also the latest of one of its months or
    quarters, a snapshot kept by one run won't be deleted by a later one.
    """
    cutoffs = [months_before(now, months) for months in (6, 18, 30)]
    seen = set()
    plan = []
    for snapshot in sorted(snapshots, key=lambda s: s['StartTime'], reverse=True):
        period = snapshot_period(snapshot['StartTime'], *cutoffs)
        keep = period is None or period not in seen
        seen.add(period)
        plan.append((snapshot, keep, period))
    return plan


def snapshot_period(start: datetime,
                    recent: datetime,
                    monthly: datetime,
                    quarterly: datetime
                    ) -> str | None:
    if start >= recent:
        return None
    elif start >= monthly:
        return f'{start.year}-{start.month:02}'
    elif start >= quarterly:
        return f'{start.year}-Q{(start.month - 1) // 3 + 1}'
    else:
        return str(start.year)


def months_before(t: datetime, months: int) -> datetime:
    year, month = divmod(t.year * 12 + t.month - 1 - months, 12)
    month += 1
    day = min(t.day, calendar.monthrange(year, month)[1])
    return t.replace(year=year, month=month, day=day)


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
