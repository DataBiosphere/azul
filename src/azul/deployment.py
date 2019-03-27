import json
from typing import Mapping, Optional

import boto3
import botocore.session
import botocore.client
from more_itertools import one

from azul import Netloc, config
from azul.decorators import memoized_property


class AWS:

    @memoized_property
    def config(self):
        return botocore.client.Config()

    @memoized_property
    def profile(self):
        session = botocore.session.Session()
        profile_name = session.get_config_variable('profile')
        return {} if profile_name is None else session.full_config['profiles'][profile_name]

    @memoized_property
    def region_name(self):
        return self.sts.meta.region_name

    @memoized_property
    def sts(self):
        return boto3.client('sts', config=self.config)

    @memoized_property
    def lambda_(self):
        return boto3.client('lambda', config=self.config)

    @memoized_property
    def apigateway(self):
        return boto3.client('apigateway', config=self.config)

    @memoized_property
    def account(self):
        return self.sts.get_caller_identity()['Account']

    @memoized_property
    def es(self):
        return boto3.client('es', config=self.config)

    @memoized_property
    def secretsmanager(self):
        return boto3.client('secretsmanager', config=self.config)

    @memoized_property
    def stepfunctions(self):
        return boto3.client('stepfunctions', config=self.config)

    @memoized_property
    def iam(self):
        return boto3.client('iam', config=self.config)

    @memoized_property
    def dynamodb_resource(self):
        return boto3.resource('dynamodb', config=self.config)

    @memoized_property
    def sqs_resource(self):
        return boto3.resource('sqs', config=self.config)

    @memoized_property
    def s3(self):
        return boto3.client('s3', config=self.config)

    @memoized_property
    def s3_resource(self):
        return boto3.resource('s3', config=aws.config)

    def api_gateway_id(self, function_name: str, validate=True) -> Optional[str]:
        try:
            response = self.lambda_.get_policy(FunctionName=function_name)
        except self.lambda_.exceptions.ResourceNotFoundException:
            return None
        else:
            policy = json.loads(response['Policy'])
            # For unknown reasons, Chalice may create more than one statement. We should fail if that's the case.
            api_stage_arn = one(policy['Statement'])['Condition']['ArnLike']['AWS:SourceArn']
            api_gateway_id = api_stage_arn.split(':')[-1].split('/', 1)[0]
            if validate:
                try:
                    self.apigateway.get_rest_api(restApiId=api_gateway_id)
                except self.apigateway.exceptions.NotFoundException:
                    return None
            return api_gateway_id

    def api_getway_endpoint(self, function_name: str, api_gateway_stage: str) -> Optional[str]:
        api_gateway_id = self.api_gateway_id(function_name)
        if api_gateway_id is None:
            return None
        else:
            return f"https://{api_gateway_id}.execute-api.{self.region_name}.amazonaws.com/{api_gateway_stage}/"

    @property
    def es_endpoint(self) -> Netloc:
        es_domain_status = self.es.describe_elasticsearch_domain(DomainName=config.es_domain)
        return es_domain_status['DomainStatus']['Endpoint'], 443

    @property
    def lambda_env(self) -> Mapping[str, str]:
        return config.lambda_env(self.es_endpoint)

    def get_lambda_arn(self, function_name, suffix):
        return f"arn:aws:lambda:{aws.region_name}:{aws.account}:function:{function_name}-{suffix}"

    @memoized_property
    def permissions_boundary_arn(self) -> str:
        return f'arn:aws:iam::{self.account}:policy/{config.permissions_boundary_name}'

    @memoized_property
    def permissions_boundary(self):
        try:
            return self.iam.get_policy(PolicyArn=self.permissions_boundary_arn)['Policy']
        except self.iam.exceptions.NoSuchEntityException:
            return None

    @memoized_property
    def permissions_boundary_tf(self) -> Mapping[str, str]:
        return {} if self.permissions_boundary is None else {
            'permissions_boundary': self.permissions_boundary['Arn']
        }


aws = AWS()

del AWS
