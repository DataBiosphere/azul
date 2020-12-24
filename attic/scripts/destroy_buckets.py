"""
Tool to remove objects from s3 storage bucket, then delete the bucket.
    1.  Populate name of the buckets to delete within list `buckets_to_delete`.
    2.  Run the script.
    Note:   It's possible that some objects are not able to be deleted. Bucket
            ACLs or Bucket Policies may have to be modified to allow for
            deletion.
"""
import logging
from pprint import pformat
from time import sleep

import boto3
import botocore.exceptions
from azul.logging import configure_script_logging

logger = logging.getLogger(__name__)

s3_client = boto3.client('s3')
buckets_to_delete = [
    # Place bucket names here
]


def inspect_buckets():
    all_buckets = [bucket['Name'] for bucket in s3_client.list_buckets()['Buckets']]
    for bucket in buckets_to_delete:
        assert bucket in all_buckets, bucket


def delete_objects(bucket: str):
    logger.info('Starting deletion of objects in bucket: %s', bucket)
    object_paginator = s3_client.get_paginator('list_objects_v2')

    def delete_objects(objects):
        resp = s3_client.delete_objects(Bucket=bucket,
                                        Delete={'Objects': objects})
        if resp.get('Errors'):
            logger.warning('Unable to delete objects: $s', resp['Errors'])

    for page in object_paginator.paginate(Bucket=bucket,
                                          PaginationConfig={'PageSize': 1000}):
        if page.get('Contents'):
            objects = [{'Key': storage_object['Key']} for storage_object in page['Contents']]
            delete_objects(objects)

    if s3_client.get_bucket_versioning(Bucket=bucket).get('Status') == 'Enabled':
        logger.info('Detected versioning on bucket, attempting to remove versioned objects')
        version_paginator = s3_client.get_paginator('list_object_versions')

        def delete_versioned_objects(version_type):
            if page.get(version_type):
                versioned_objects = [{
                    'Key': storage_object['Key'],
                    'VersionId': storage_object['VersionId']} for storage_object in page[version_type]]
                delete_objects(versioned_objects)

        # must set a deletion marker, then delete that marker
        for page in version_paginator.paginate(Bucket=bucket):
            delete_versioned_objects('Versions')
        for page in version_paginator.paginate(Bucket=bucket):
            delete_versioned_objects('DeleteMarkers')


def delete_bucket(bucket: str):
    try:
        logger.info('Deleting bucket: %s', bucket)
        s3_client.delete_bucket(Bucket=bucket)
    except botocore.exceptions.ClientError:
        logger.warning('Unable To delete bucket: %s', bucket, exc_info=True)


if __name__ == '__main__':
    configure_script_logging(logger)

    inspect_buckets()
    caller = boto3.client('sts').get_caller_identity()
    logger.warning('Using IAM ARN: %s', caller['Arn'])
    logger.warning('Destroying Buckets: ')
    for line in pformat(buckets_to_delete).split('\n'):
        logger.warning(line)
    logger.warning('Waiting 10 seconds before starting...')
    sleep(10)
    for bucket in buckets_to_delete:
        delete_objects(bucket)
        delete_bucket(bucket)
