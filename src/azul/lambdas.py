import ast
import logging

from azul import (
    config,
)
from azul.deployment import (
    aws,
)

logger = logging.getLogger(__name__)


class Lambdas:
    tag_name = 'azul-original-concurrency-limit'

    def __init__(self):
        self.lambda_ = aws.client('lambda')

    def manage_lambdas(self, enabled: bool):
        paginator = self.lambda_.get_paginator('list_functions')
        lambda_prefixes = [config.qualified_resource_name(lambda_infix) for lambda_infix in config.lambda_names()]
        assert all(lambda_prefixes)
        for lambda_page in paginator.paginate(FunctionVersion='ALL', MaxItems=500):
            for lambda_name in [metadata['FunctionName'] for metadata in lambda_page['Functions']]:
                if any(lambda_name.startswith(prefix) for prefix in lambda_prefixes):
                    self.manage_lambda(lambda_name, enabled)

    def manage_lambda(self, lambda_name: str, enable: bool):
        lambda_settings = self.lambda_.get_function(FunctionName=lambda_name)
        lambda_arn = lambda_settings['Configuration']['FunctionArn']
        lambda_tags = self.lambda_.list_tags(Resource=lambda_arn)['Tags']
        lambda_name = lambda_settings['Configuration']['FunctionName']
        if enable:
            if self.tag_name in lambda_tags.keys():
                original_concurrency_limit = ast.literal_eval(lambda_tags[self.tag_name])

                if original_concurrency_limit is not None:
                    logger.info(f'Setting concurrency limit for {lambda_name} back to {original_concurrency_limit}.')
                    self.lambda_.put_function_concurrency(FunctionName=lambda_name,
                                                          ReservedConcurrentExecutions=original_concurrency_limit)
                else:
                    logger.info(f'Removed concurrency limit for {lambda_name}.')
                    self.lambda_.delete_function_concurrency(FunctionName=lambda_name)

                lambda_arn = lambda_settings['Configuration']['FunctionArn']
                self.lambda_.untag_resource(Resource=lambda_arn, TagKeys=[self.tag_name])
            else:
                logger.warning(f'{lambda_name} is already enabled.')
        else:
            if self.tag_name not in lambda_tags.keys():
                try:
                    concurrency = lambda_settings['Concurrency']
                except KeyError:
                    # If a lambda doesn't have a limit for concurrency executions, Lambda.Client.get_function()
                    # doesn't return a response with the key, `Concurrency`.
                    concurrency_limit = None
                else:
                    concurrency_limit = concurrency['ReservedConcurrentExecutions']

                logger.info(f'Setting concurrency limit for {lambda_name} to zero.')
                new_tag = {self.tag_name: repr(concurrency_limit)}
                self.lambda_.tag_resource(Resource=lambda_settings['Configuration']['FunctionArn'], Tags=new_tag)
                self.lambda_.put_function_concurrency(FunctionName=lambda_name, ReservedConcurrentExecutions=0)
            else:
                logger.warning(f'{lambda_name} is already disabled.')
