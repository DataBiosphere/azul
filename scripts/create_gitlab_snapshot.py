"""
Create a snapshot of the EBS data volume attached to the GitLab instance in the
currently selected deployment.
"""

import datetime
import logging
import sys
from time import (
    sleep,
)
from typing import (
    Optional,
)

from more_itertools import (
    one,
    only,
)

from azul import (
    config,
    require,
)
from azul.deployment import (
    aws,
)
from azul.logging import (
    configure_script_logging,
)
from azul.types import (
    JSON,
)

log = logging.getLogger(__name__)


def main(argv):
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--no-restart',
                        dest='restart',
                        default=True,
                        action='store_false',
                        help='Leave the EC2 instance in a stopped state after '
                             'the snapshot has been created.')
    args = parser.parse_args(argv)
    require(config.terraform_component == 'gitlab',
            "Select the 'gitlab' component ('dev.gitlab' or 'prod.gitlab', for example).")
    volume = gitlab_volume_info()
    instance: Optional[JSON] = only(volume['Attachments'])
    if instance is None:
        log.info('Volume %r is not attached to any instances', volume['VolumeId'])
    else:
        shutdown_instance(instance)
    create_snapshot(volume)
    if instance and args.restart:
        start_instance(instance)


def gitlab_volume_info() -> JSON:
    filter = {
        'Name': 'tag:Name',
        'Values': ['azul-gitlab']
    }
    response = aws.ec2.describe_volumes(Filters=[filter])
    return one(response['Volumes'])


def shutdown_instance(instance: JSON):
    instance_id = instance['InstanceId']
    log.info('Preparing to stop GitLab instance for %r, waiting 10 seconds '
             'before proceeding. Hit Ctrl-C to abort …',
             config.deployment_stage)
    sleep(10)
    log.info('Stopping instance %r …', instance_id)
    aws.ec2.stop_instances(InstanceIds=[instance_id])
    waiter = aws.ec2.get_waiter('instance_stopped')
    waiter.wait(InstanceIds=[instance_id],
                WaiterConfig=dict(MaxAttempts=9999, Delay=15))
    log.info('Instance %r has stopped', instance_id)


def start_instance(instance: JSON):
    instance_id = instance['InstanceId']
    log.info('Starting instance %r …', config.deployment_stage)
    aws.ec2.start_instances(InstanceIds=[instance_id])
    waiter = aws.ec2.get_waiter('instance_status_ok')
    waiter.wait(InstanceIds=[instance_id],
                WaiterConfig=dict(MaxAttempts=9999, Delay=15))
    log.info('Instance %r is running', instance_id)


def create_snapshot(volume: JSON):
    # FIXME: Consolidate uses of sts.get_caller_identity
    #        https://github.com/DataBiosphere/azul/issues/3890
    role_id = aws.sts.get_caller_identity()['UserId'].split(':')[0]
    tags = {
        'component': 'azul-gitlab',
        'created-by': role_id,
        'deployment': config.deployment_stage,
        'Name': 'azul-gitlab',
        'owner': config.owner,
        'project': 'dcp',
        'service': 'azul',
    }
    date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    response = aws.ec2.create_snapshot(Description=f'{date} GitLab Update',
                                       VolumeId=volume['VolumeId'],
                                       TagSpecifications=[
                                           dict(ResourceType='snapshot',
                                                Tags=[{'Key': k, 'Value': v} for k, v in tags.items()])
                                       ])
    snapshot_id = response['SnapshotId']
    log.info('Snapshot %r of volume %r is being created …',
             snapshot_id, volume['VolumeId'])
    waiter = aws.ec2.get_waiter('snapshot_completed')
    waiter.wait(SnapshotIds=[snapshot_id],
                WaiterConfig=dict(MaxAttempts=9999, Delay=15))
    log.info('Snapshot %r of volume %r is complete',
             volume['VolumeId'], config.deployment_stage)


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
