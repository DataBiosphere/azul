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
        return session.full_config['profiles'][profile_name]

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

    @lru_cache(maxsize=1)
    def dynamo(self, endpoint_url, region_name):
        return boto3.resource('dynamodb', endpoint_url=endpoint_url, region_name=region_name)

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


aws = AWS()

del AWS
