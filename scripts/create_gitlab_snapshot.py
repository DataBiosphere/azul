"""
Create a snapshot of the EBS data volume attached to the GitLab instance in the
currently selected deployment.
"""

import datetime
import logging
import shlex
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
    reject,
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
    g = parser.add_mutually_exclusive_group()
    g.add_argument('--no-restart',
                   dest='restart',
                   default=True,
                   action='store_false',
                   help='Leave the EC2 instance in stopped state after '
                        'the snapshot has been created.')
    g.add_argument('--start-only',
                   dest='start',
                   default=False,
                   action='store_true',
                   help='Start the EC2 instance from the stopped state. '
                        'Fails if the instance is not stopped.')
    g.add_argument('--new-size',
                   dest='size',
                   action='store',
                   type=int,
                   help='After creating a new snapshot, restore the snapshot '
                        'to a new data volume of the given size (in GiB). '
                        'The new volume will be tagged so that subsequently '
                        'deploying the `gitlab` component will automatically '
                        'discover and attach it to the instance. The old '
                        'volume must be deleted manually. A shell command to '
                        'do so will be printed for your convenience.')
    args = parser.parse_args(argv)
    require(config.terraform_component == 'gitlab',
            "Select the 'gitlab' component ('dev.gitlab' or 'prod.gitlab', for example).")
    volume = gitlab_volume_info()
    instance: Optional[JSON] = only(volume['Attachments'])
    if instance is None:
        log.info('Volume %r is not attached to any instances', volume['VolumeId'])
    elif args.start:
        confirm_instance_is_stopped(instance['InstanceId'])
        start_instance(instance)
    else:
        shutdown_instance(instance)
        snapshot_id = create_snapshot(volume)
        if args.size:
            create_volume(snapshot_id, args.size, volume)
        elif args.restart:
            start_instance(instance)


def confirm_instance_is_stopped(id: str):
    response = aws.ec2.describe_instance_status(
        InstanceIds=[id],
    )
    instance_active = response['InstanceStatuses']
    reject(instance_active, f'Instance {id} is not in stopped state.')


def gitlab_volume_info() -> JSON:
    filter = {
        'Name': 'tag:Name',
        'Values': ['azul-gitlab']
    }
    response = aws.ec2.describe_volumes(Filters=[filter])
    return one(response['Volumes'])


def shutdown_instance(instance: JSON):
    instance_id = instance['InstanceId']
    log.info('Preparing to stop GitLab instance in %r. '
             'Waiting 10 seconds before proceeding. '
             'Hit Ctrl-C to abort …',
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
    log.info('Starting instance %r …', instance_id)
    aws.ec2.start_instances(InstanceIds=[instance_id])
    waiter = aws.ec2.get_waiter('instance_status_ok')
    waiter.wait(InstanceIds=[instance_id],
                WaiterConfig=dict(MaxAttempts=9999, Delay=15))
    log.info('Instance %r is running', instance_id)


def create_snapshot(volume: JSON) -> str:
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
    response = aws.ec2.create_snapshot(Description=f'{date} snapshot of GitLab data volume',
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
             snapshot_id, volume['VolumeId'])
    return snapshot_id


def create_volume(snapshot_id: str, size: int, old_volume: JSON):
    new_volume = aws.ec2.create_volume(AvailabilityZone=old_volume['AvailabilityZone'],
                                       Encrypted=old_volume['Encrypted'],
                                       KmsKeyId=old_volume['KmsKeyId'],
                                       Size=size,
                                       SnapshotId=snapshot_id,
                                       VolumeType=old_volume['VolumeType'],
                                       TagSpecifications=[dict(ResourceType='volume', Tags=old_volume['Tags'])],
                                       MultiAttachEnabled=old_volume['MultiAttachEnabled'])
    log.info('New volume %r is being created from snapshot %r …',
             new_volume['VolumeId'], snapshot_id)
    waiter = aws.ec2.get_waiter('volume_available')
    waiter.wait(VolumeIds=[new_volume['VolumeId']],
                WaiterConfig=dict(MaxAttempts=9999, Delay=15))
    log.info('New volume %r of size %s GiB is ready to attach',
             new_volume['VolumeId'], new_volume['Size'])

    tags = old_volume['Tags']
    assert isinstance(tags, list), tags
    log.info('Removing tags from volume %r …', old_volume['VolumeId'])
    aws.ec2.delete_tags(Resources=[old_volume['VolumeId']], Tags=tags)
    log.info('Tags removed')
    volume_id = old_volume['VolumeId']
    assert shlex.quote(volume_id) == volume_id, volume_id
    command = f'aws ec2 delete-volume --volume-id {volume_id}'
    log.info('Run %r to delete the volume after confirming that the instance '
             'is functional with the new volume attached', command)


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
