import argparse
import ast
from azul import config
from azul.types import JSON
import boto3
import logging

logger = logging.getLogger(__name__)


class RedButton:
    def __init__(self):
        self.lambda_ = boto3.client('lambda')
        self.tag_name = 'azul-original-concurrency-limit'

    def enable_azul_lambdas(self, enabled: bool):
        paginator = self.lambda_.get_paginator('list_functions')
        lambda_prefixes = [config.qualified_resource_name(lambda_infix) for lambda_infix in config.lambda_names()]
        assert all(lambda_prefixes)
        for lambda_page in paginator.paginate(FunctionVersion='ALL', MaxItems=500):
            for lambda_name in [metadata['FunctionName'] for metadata in lambda_page['Functions']]:
                if any(lambda_name.startswith(prefix) for prefix in lambda_prefixes):
                    lambda_settings = self.lambda_.get_function(FunctionName=lambda_name)
                    lambda_arn = lambda_settings['Configuration']['FunctionArn']
                    lambda_tags = self.lambda_.list_tags(Resource=lambda_arn)['Tags']
                    if enabled:
                        self.enable_lambda(lambda_settings, lambda_tags)
                    else:
                        self.disable_lambda(lambda_settings, lambda_tags)

    def enable_lambda(self, lambda_settings: JSON, lambda_tags: JSON):
        lambda_name = lambda_settings['Configuration']['FunctionName']
        if self.tag_name in lambda_tags.keys():
            original_concurrency_limit = ast.literal_eval(lambda_tags[self.tag_name])

            if original_concurrency_limit is not None:
                logging.info(f'Setting concurrency limit for {lambda_name} back to {original_concurrency_limit}.')
                self.lambda_.put_function_concurrency(FunctionName=lambda_name,
                                                      ReservedConcurrentExecutions=original_concurrency_limit)
            else:
                logging.info(f'Removed concurrency limit for {lambda_name}.')
                self.lambda_.delete_function_concurrency(FunctionName=lambda_name)

            lambda_arn = lambda_settings['Configuration']['FunctionArn']
            self.lambda_.untag_resource(Resource=lambda_arn, TagKeys=[self.tag_name])
        else:
            logging.warning(f'{lambda_name} is already enabled.')

    def disable_lambda(self, lambda_settings: JSON, lambda_tags: JSON):
        lambda_name = lambda_settings['Configuration']['FunctionName']
        if self.tag_name not in lambda_tags.keys():
            try:
                concurrency = lambda_settings['Concurrency']
            except KeyError:
                # If a lambda doesn't have a limit for concurrency executions, Lambda.Client.get_function()
                # doesn't return a response with the key, `Concurrency`.
                concurrency_limit = None
            else:
                concurrency_limit = concurrency['ReservedConcurrentExecutions']

            logging.info(f'Setting concurrency limit for {lambda_name} to zero.')
            new_tag = {self.tag_name: repr(concurrency_limit)}
            self.lambda_.tag_resource(Resource=lambda_settings['Configuration']['FunctionArn'], Tags=new_tag)
            self.lambda_.put_function_concurrency(FunctionName=lambda_name, ReservedConcurrentExecutions=0)
        else:
            logging.warning(f'{lambda_name} is already disabled.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Enables or disables the lambdas in the current deployment.')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--enable', dest='enabled', action='store_true', default=None)
    group.add_argument('--disable', dest='enabled', action='store_false')
    args = parser.parse_args()
    assert args.enabled is not None
    RedButton().enable_azul_lambdas(args.enabled)
