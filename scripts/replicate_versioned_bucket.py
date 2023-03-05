# FIXME: Move to attic once migration is done
#        https://github.com/DataBiosphere/azul/issues/4918

import logging
from operator import (
    itemgetter,
)
from time import (
    sleep,
)

import boto3
import botocore.exceptions

from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)


def main():
    if config.terraform_component == 'shared':
        replicate_versioned_bucket()
    else:
        log.info("The 'shared' component is not selected, skipping import.")


def replicate_versioned_bucket():
    s3c = boto3.client('s3control')

    log.info('Creating batch replication job …')
    try:
        response = s3c.create_job(
            AccountId=aws.account,
            Operation={
                'S3ReplicateObject': {}
            },
            Report={
                'Bucket': f'arn:aws:s3:::{aws.shared_bucket}',
                'Prefix': 'batch-replication-report',
                'Format': 'Report_CSV_20180820',
                'Enabled': True,
                'ReportScope': 'AllTasks'
            },
            # Amazon only allows one job with a given token in a given account.
            # To restart the replication, the token must be updated.
            ClientRequestToken='e7116070-2703-49e8-b7bf-9efdaa26aa3a',
            Priority=1,
            RoleArn=f"arn:aws:iam::{aws.account}:role/{config.qualified_resource_name('shared_temp')}",
            ConfirmationRequired=False,
            ManifestGenerator={
                'S3JobManifestGenerator': {
                    'ExpectedBucketOwner': aws.account,
                    'SourceBucket': f'arn:aws:s3:::{config.versioned_bucket}',
                    'EnableManifestOutput': False,
                    'Filter': {
                        'EligibleForReplication': True,
                        'ObjectReplicationStatuses': ['NONE', 'FAILED']
                    }
                }
            },
        )
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Message'] == 'Duplicate ClientRequestToken':
            # It's not clear under what situations this is raised. I've observed
            # it for failed and completed jobs. But I've also observed a second
            # invocation of create_job with the same ClientRequestToken to NOT
            # raise this and instead return the same job as the first invocation
            log.warning('There already is an incomplete matching job. Looking it up …')
            response = s3c.list_jobs(AccountId=aws.account, MaxResults=1000)
            assert 'NextToken' not in response
            job_id = sorted(response['Jobs'], key=itemgetter('CreationTime'), reverse=True)[0]['JobId']
        else:
            raise
    else:
        job_id = response['JobId']
        log.info('Created batch replication job %r. Waiting for it to complete …', job_id)

    while True:
        response = s3c.describe_job(AccountId=aws.account, JobId=job_id)
        status = response['Job']['Status']
        log.info('Job status is %r (%r)', status, response)
        if status == 'Complete':
            summary = response['Job']['ProgressSummary']
            failed = summary['NumberOfTasksFailed']
            succeeded = summary['NumberOfTasksSucceeded']
            total = summary['TotalNumberOfTasks']
            assert succeeded + failed == total, summary
            if failed > 0:
                raise RuntimeError('Job completed but some task(s) failed', failed)
            else:
                log.info('Replication batch job succeeded')
                break
        elif status in {'Failed', 'Cancelled'}:
            raise RuntimeError('Unexpected job status', status)
        else:
            sleep(10)


if __name__ == '__main__':
    configure_script_logging()
    main()
