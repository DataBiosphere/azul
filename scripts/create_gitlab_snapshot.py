from collections.abc import (
    Mapping,
)
import datetime
import logging
from time import (
    sleep,
)

from more_itertools import (
    one,
)

from azul import (
    JSON,
    config,
    require,
)
from azul.deployment import (
    aws,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)


def main():
    require(config.terraform_component == 'gitlab',
            "Select the 'gitlab' component ('dev.gitlab' or 'prod.gitlab', for example).")
    volume = gitlab_volume_info()
    attachments = volume['Attachments']
    if attachments:
        instance = one(attachments)
        shutdown_instance(instance)
    else:
        log.info('Volume %s is not attached to any instances', volume['VolumeId'])
    create_snapshot(volume)


# This filter is used to locate the EBS data volume to be backed up
gitlab_filter = [
    {
        'Name': 'tag:Name',
        'Values': ['azul-gitlab']
    }
]


def gitlab_volume_info() -> Mapping[str, JSON]:
    return one(aws.ec2.describe_volumes(Filters=gitlab_filter)['Volumes'])


def shutdown_instance(instance: Mapping[str, JSON]):
    instance_ids = [instance['InstanceId']]
    log.info('Preparing to stop GitLab instance for %s, waiting 10 seconds '
             'before proceeding. Hit Ctrl-C to abort',
             config.deployment_stage)
    sleep(10)
    log.info('Stopping instance %s', instance_ids)
    aws.ec2.stop_instances(InstanceIds=instance_ids)
    log.info('Waiting for the GitLab instance to stop')
    waiter = aws.ec2.get_waiter('instance_stopped')
    waiter.wait(InstanceIds=instance_ids,
                WaiterConfig=dict(MaxAttempts=9999, Delay=15))
    log.info('Instance %s has stopped', instance_ids)


def create_snapshot(volume: Mapping[str, JSON]):
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
    snapshot = [response['SnapshotId']]
    log.info('Created snapshot %s for %s, waiting for completion',
             snapshot, volume['VolumeId'])
    waiter = aws.ec2.get_waiter('snapshot_completed')
    waiter.wait(SnapshotIds=snapshot,
                WaiterConfig=dict(MaxAttempts=9999, Delay=15))
    log.info('Snapshot for %s in %s is complete',
             volume['VolumeId'], config.deployment_stage)


if __name__ == '__main__':
    configure_script_logging(log)
    main()
