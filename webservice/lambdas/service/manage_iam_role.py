import json
import sys

import boto3

role_name, role_policy_json = sys.argv[1:]

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

iam = boto3.client('iam')

try:
    response = iam.get_role(RoleName=role_name)
    current_assume_role_policy = response['Role']['AssumeRolePolicyDocument']
except iam.exceptions.NoSuchEntityException:
    current_assume_role_policy = None

try:
    response = iam.get_role_policy(RoleName=role_name, PolicyName=role_name)
    current_role_policy = response['PolicyDocument']
except iam.exceptions.NoSuchEntityException:
    current_role_policy = None

if role_policy == current_role_policy and assume_role_policy == current_assume_role_policy:
    print(f'Role {role_name} already up-to-date.')
else:
    assume_role_policy = json.dumps(assume_role_policy)
    try:
        iam.create_role(RoleName=role_name,
                        AssumeRolePolicyDocument=assume_role_policy)
    except iam.exceptions.EntityAlreadyExistsException:
        iam.update_assume_role_policy(RoleName=role_name,
                                      PolicyDocument=assume_role_policy)
    iam.put_role_policy(RoleName=role_name,
                        PolicyName=role_name,
                        PolicyDocument=json.dumps(role_policy))
