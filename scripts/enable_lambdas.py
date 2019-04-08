import argparse
import ast
from azul import config
from azul.types import JSON
import boto3
import logging

logger = logging.getLogger(__name__)


class RedButton:
    def __init__(self):
        self.azul_lambda = boto3.client('lambda')

    def enable_azul_lambdas(self, enabled: bool):
        paginator = self.azul_lambda.get_paginator('list_functions')
        lambda_prefixes = [config.qualified_resource_name(lambda_infix) for lambda_infix in config.lambda_names()]
        assert all(lambda_prefixes)
        for lambda_page in paginator.paginate(FunctionVersion='ALL', MaxItems=500):
            for lambda_name in [metadata['FunctionName'] for metadata in lambda_page['Functions']]:
                if any(lambda_name.startswith(prefix) for prefix in lambda_prefixes):
                    lambda_settings = self.azul_lambda.get_function(FunctionName=lambda_name)
                    lambda_arn = lambda_settings['Configuration']['FunctionArn']
                    lambda_tags = self.azul_lambda.list_tags(Resource=lambda_arn)['Tags']
                    if enabled:
                        self.enable_lambda(lambda_settings, lambda_tags)
                    else:
                        self.disable_lambda(lambda_settings, lambda_tags)

    def enable_lambda(self, lambda_settings: JSON, lambda_tags: JSON):
        lambda_name = lambda_settings['Configuration']['FunctionName']
        if 'azul-original-concurrency-limit' in lambda_tags.keys():
            azul_original_concurrency_limit = ast.literal_eval(lambda_tags['azul-original-concurrency-limit'])

            if azul_original_concurrency_limit is not None:
                logging.info(f'Setting concurrency limit for {lambda_name} back to {azul_original_concurrency_limit}.')
                self.azul_lambda.put_function_concurrency(FunctionName=lambda_name,
                                                          ReservedConcurrentExecutions=azul_original_concurrency_limit)
            else:
                logging.info(f'Removed concurrency limit for {lambda_name}.')
                self.azul_lambda.delete_function_concurrency(FunctionName=lambda_name)

            lambda_arn = lambda_settings['Configuration']['FunctionArn']
            self.azul_lambda.untag_resource(Resource=lambda_arn, TagKeys=['azul-original-concurrency-limit'])
        else:
            logging.warning(f'{lambda_name} is already enabled.')

    def disable_lambda(self, lambda_settings: JSON, lambda_tags: JSON):
        lambda_name = lambda_settings['Configuration']['FunctionName']
        if 'azul-original-concurrency-limit' not in lambda_tags.keys():
            try:
                concurrency_settings = lambda_settings['Concurrency']
            except KeyError:
                # If a lambda doesn't have a limit for concurrency executions, Lambda.Client.get_function()
                # doesn't return a response with the key, `Concurrency`.
                concurrency_limit = None
            else:
                concurrency_limit = concurrency_settings['ReservedConcurrentExecutions']

            logging.info(f'Setting concurrency limit for {lambda_name} to zero.')
            new_tag = {'azul-original-concurrency-limit': repr(concurrency_limit)}
            self.azul_lambda.tag_resource(Resource=lambda_settings['Configuration']['FunctionArn'], Tags=new_tag)
            self.azul_lambda.put_function_concurrency(FunctionName=lambda_name, ReservedConcurrentExecutions=0)
        else:
            logging.warning(f'{lambda_name} is already disabled.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Enables or disables all the lambdas in Azul.')
    parser.add_argument('--enable', dest='enabled', action='store_true', default=True)
    parser.add_argument('--disable', dest='enabled', action='store_false')
    args = parser.parse_args()

    RedButton().enable_azul_lambdas(args.enabled)
