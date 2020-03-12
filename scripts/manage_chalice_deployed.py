#! /usr/bin/env python3

import logging
import os
import sys

import boto3

from azul import config
from azul.logging import configure_script_logging

logger = logging.getLogger(__name__)
configure_script_logging(logger)
app_name, command = sys.argv[1:]

bucket_name = config.terraform_backend_bucket
file_path = f'.chalice/deployed/{config.deployment_stage}.json'
key = f'azul-{app_name}-{config.deployment_stage}/deployed.json'

s3 = boto3.client('s3')

# TODO: Delete this whole file. But first, all deployments should be configured
# to deploy with Terraform.
if command == 'download':
    logger.info(f"Downloading s3://{bucket_name}/{key} to {file_path}")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    try:
        s3.download_file(Bucket=bucket_name, Key=key, Filename=file_path + '.partial')
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            logger.warning("Cannot find existing deployment. This is expected during the first attempt at deploying.")
        else:
            raise
    else:
        os.rename(file_path + '.partial', file_path)
elif command == 'upload':
    logger.info(f"Uploading {file_path} to s3://{bucket_name}/{key}")
    s3.upload_file(Filename=file_path, Bucket=bucket_name, Key=key)
else:
    raise ValueError(command)
