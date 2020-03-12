import argparse
import json
import logging
import sys

import boto3

from azul import config
from azul.deployment import aws
from azul.logging import configure_script_logging

"""
Warning: Running this script in isolation (outside the Makefile) to delete and then create a lambda function's IAM role
can leave the lambda function in an odd state where a role appears to be selected however fails to be applied,
producing KMS access denied errors found only in the AWS console. The fix for a lambda function once in this state is to
use the AWS console to manually toggle the function's selected role, changing it first to another value, saving changes,
then selecting the original value, and saving changes. This process will need to be completed for the function and other
functions with the same base name (eg. 'azul-service-daniel', 'azul-service-daniel-manifest')
Solution found at https://github.com/aws/chalice/issues/1103#issuecomment-530158030
"""

log = logging.getLogger(__name__)


# TODO: Delete this whole file. But first, all deployments should be configured
# to deploy with Terraform.
def main(argv):
    configure_script_logging(log)
    parser = argparse.ArgumentParser(description='Manage lambda IAM roles')
    subparsers = parser.add_subparsers(help='sub-command help', dest='command')

    sub_create = subparsers.add_parser('create', help='Create an IAM role and policy')
    sub_create.add_argument('lambda_name', help='Name of a lambda to create a policy for (e.g. indexer, service)')
    sub_create.add_argument('role_policy_file', help='Path to lambda policy file (e.g. lambda-policy.json)')

    sub_delete = subparsers.add_parser('delete', help='Delete an IAM role and its policies')
    sub_delete.add_argument('lambda_name', help='Name of a lambda to remove a policy from (e.g. indexer, service)')

    args = parser.parse_args(argv)

    if args.command == 'create':
        create_policy(lambda_name=args.lambda_name, role_policy_file=args.role_policy_file)
    elif args.command == 'delete':
        delete_policy(lambda_name=args.lambda_name)
    else:
        parser.print_usage()


def create_policy(lambda_name, role_policy_file):
    role_name = config.qualified_resource_name(lambda_name)

    with open(role_policy_file) as f:
        role_policy = json.load(f)

    assume_role_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "sts:AssumeRole",
                "Principal": {
                    "Service": "lambda.amazonaws.com"
                }
            },
            *(
                {
                    "Effect": "Allow",
                    "Action": "sts:AssumeRole",
                    "Principal": {
                        "AWS": f"arn:aws:iam::{account}:root"
                    },
                    # Wildcards are not supported in `Principal`, but they are in `Condition`
                    "Condition": {
                        "StringLike": {
                            "aws:PrincipalArn": [f"arn:aws:iam::{account}:role/{role}" for role in roles]
                        }
                    }
                }
                for account, roles in config.external_lambda_role_assumptors.items()
            )
        ]
    }
    iam = boto3.client('iam')

    try:
        response = iam.get_role(RoleName=role_name)
        role = response['Role']
        current_assume_role_policy = role['AssumeRolePolicyDocument']
        current_permissions_boundary = role.get('PermissionsBoundary', {}).get('PermissionsBoundaryArn')
    except iam.exceptions.NoSuchEntityException:
        current_assume_role_policy = None
        current_permissions_boundary = None

    try:
        response = iam.get_role_policy(RoleName=role_name, PolicyName=role_name)
        current_role_policy = response['PolicyDocument']
    except iam.exceptions.NoSuchEntityException:
        current_role_policy = None

    permissions_boundary = aws.permissions_boundary['Arn'] if aws.permissions_boundary else None

    log.info(f"Creating IAM Role {role_name}.")
    if (
        role_policy == current_role_policy
        and assume_role_policy == current_assume_role_policy
        and permissions_boundary == current_permissions_boundary
    ):
        log.info(f'Role {role_name} already up-to-date.')
    else:
        assume_role_policy = json.dumps(assume_role_policy)
        try:
            iam.create_role(RoleName=role_name,
                            AssumeRolePolicyDocument=assume_role_policy,
                            PermissionsBoundary=permissions_boundary)
        except iam.exceptions.EntityAlreadyExistsException:
            log.info(f'Role {role_name} already exists, updating...')
            iam.update_assume_role_policy(RoleName=role_name,
                                          PolicyDocument=assume_role_policy)
            iam.put_role_permissions_boundary(RoleName=role_name,
                                              PermissionsBoundary=permissions_boundary)
        log.info(f'Adding IAM Policy {role_name}.')
        iam.put_role_policy(RoleName=role_name,
                            PolicyName=role_name,
                            PolicyDocument=json.dumps(role_policy))


def delete_policy(lambda_name):
    role_name = config.qualified_resource_name(lambda_name)

    iam = boto3.client('iam')

    try:
        policies = iam.list_role_policies(RoleName=role_name)
    except iam.exceptions.NoSuchEntityException:
        pass
    else:
        for policy_name in policies['PolicyNames']:
            log.info(f"Deleting IAM Policy {policy_name}.")
            iam.delete_role_policy(RoleName=role_name, PolicyName=policy_name)

    log.info(f"Deleting IAM Role {role_name}.")
    try:
        iam.delete_role(RoleName=role_name)
    except iam.exceptions.NoSuchEntityException:
        log.info(f"IAM Role {role_name} not found.")


if __name__ == '__main__':
    main(sys.argv[1:])
