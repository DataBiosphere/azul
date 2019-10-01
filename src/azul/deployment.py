from functools import lru_cache
import json
from typing import Mapping, Optional

import boto3
import botocore.session
from more_itertools import one

from azul import Netloc, config
from azul.decorators import memoized_property


class AWS:
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
        return boto3.client('sts')

    @memoized_property
    def lambda_(self):
        return boto3.client('lambda')

    @memoized_property
    def apigateway(self):
        return boto3.client('apigateway')

    @memoized_property
    def account(self):
        return self.sts.get_caller_identity()['Account']

    @memoized_property
    def es(self):
        return boto3.client('es')

    @memoized_property
    def stepfunctions(self):
        return boto3.client('stepfunctions')

    @memoized_property
    def iam(self):
        return boto3.client('iam')

    @memoized_property
    def secretsmanager(self):
        return boto3.client('secretsmanager')

    @lru_cache(maxsize=1)
    def dynamo(self, endpoint_url, region_name):
        return boto3.resource('dynamodb', endpoint_url=endpoint_url, region_name=region_name)

    def api_gateway_export(self, gateway_id):
        response = self.apigateway.get_export(restApiId=gateway_id,
                                              stageName=config.deployment_stage,
                                              exportType='oas30',
                                              accepts='application/json')
        return json.load(response['body'])

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

    def api_gateway_endpoint(self, function_name: str, api_gateway_stage: str) -> Optional[str]:
        api_gateway_id = self.api_gateway_id(function_name)
        if api_gateway_id is None:
            return None
        else:
            return f"https://{api_gateway_id}.execute-api.{self.region_name}.amazonaws.com/{api_gateway_stage}/"

    @property
    def es_endpoint(self) -> Netloc:
        es_domain_status = self.es.describe_elasticsearch_domain(DomainName=config.es_domain)
        return es_domain_status['DomainStatus']['Endpoint'], 443

    def lambda_env(self, function_name) -> Mapping[str, str]:
        gateway_id = self.api_gateway_id(function_name, validate=True)
        env = config.lambda_env(self.es_endpoint)
        return env if gateway_id is None else {**env, 'api_gateway_id': gateway_id}

    def get_lambda_arn(self, function_name, suffix):
        return f"arn:aws:lambda:{self.region_name}:{self.account}:function:{function_name}-{suffix}"

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

    def get_hmac_key_and_id(self):
        # Note: dict contains 'key' and 'key_id' as keys and is provisioned in scripts/provision_credentials.py
        response = self.secretsmanager.get_secret_value(SecretId=config.secrets_manager_secret_name('indexer', 'hmac'))
        secret_dict = json.loads(response['SecretString'])
        return secret_dict['key'], secret_dict['key_id']

    @lru_cache()
    def get_hmac_key_and_id_cached(self, cache_key_id):
        key, key_id = self.get_hmac_key_and_id()
        assert cache_key_id == key_id
        return key, key_id


aws = AWS()

del AWS
