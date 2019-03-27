#! /usr/bin/env python3

import json
import sys

from azul import config
from azul.deployment import aws

lambda_name, role_policy_json = sys.argv[1:]
role_name = config.qualified_resource_name(lambda_name)

with open(role_policy_json) as f:
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
        }
    ]
}

try:
    response = aws.iam.get_role(RoleName=role_name)
    role = response['Role']
    current_assume_role_policy = role['AssumeRolePolicyDocument']
    current_permissions_boundary = role.get('PermissionsBoundary', {}).get('PermissionsBoundaryArn')
except aws.iam.exceptions.NoSuchEntityException:
    current_assume_role_policy = None
    current_permissions_boundary = None

try:
    response = aws.iam.get_role_policy(RoleName=role_name, PolicyName=role_name)
    current_role_policy = response['PolicyDocument']
except aws.iam.exceptions.NoSuchEntityException:
    current_role_policy = None

permissions_boundary = aws.permissions_boundary['Arn'] if aws.permissions_boundary else None

if (
    role_policy == current_role_policy
    and assume_role_policy == current_assume_role_policy
    and permissions_boundary == current_permissions_boundary
):
    print(f'Role {role_name} already up-to-date.')
else:
    assume_role_policy = json.dumps(assume_role_policy)
    try:
        aws.iam.create_role(RoleName=role_name,
                            AssumeRolePolicyDocument=assume_role_policy,
                            PermissionsBoundary=permissions_boundary)
    except aws.iam.exceptions.EntityAlreadyExistsException:
        aws.iam.update_assume_role_policy(RoleName=role_name,
                                          PolicyDocument=assume_role_policy)
        aws.iam.put_role_permissions_boundary(RoleName=role_name,
                                              PermissionsBoundary=permissions_boundary)
    aws.iam.put_role_policy(RoleName=role_name,
                            PolicyName=role_name,
                            PolicyDocument=json.dumps(role_policy))
