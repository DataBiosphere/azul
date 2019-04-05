import argparse
import ast
from azul import config
import boto3
import logging

logger = logging.getLogger(__name__)


def enable_azul_lambdas(enabled: bool):
    client = boto3.client('lambda')
    paginator = client.get_paginator('list_functions')
    lambda_prefixes = [config.qualified_resource_name(azul_lambda_infix) for azul_lambda_infix in config.lambda_names()]

    assert all(lambda_prefixes)

    for lambda_page in paginator.paginate(FunctionVersion='ALL', MaxItems=500):
        for lambda_name in [metadata['FunctionName'] for metadata in lambda_page['Functions']]:
            if any(lambda_name.startswith(prefix) for prefix in lambda_prefixes):
                lambda_settings = client.get_function(FunctionName=lambda_name)
                lambda_arn = lambda_settings['Configuration']['FunctionArn']
                lambda_tags = client.list_tags(Resource=lambda_arn)['Tags']

                if enabled:
                    enable_lambda(client, lambda_name, lambda_arn, lambda_tags)
                else:
                    disable_lambda(client, lambda_name, lambda_settings, lambda_tags)


def enable_lambda(client: 'botocore.client.Lambda', lambda_name: str, lambda_arn: str, lambda_tags: dict):
    if 'azul-original-concurrency-limit' in lambda_tags.keys():
        azul_original_concurrency_limit = ast.literal_eval(lambda_tags['azul-original-concurrency-limit'])

        if azul_original_concurrency_limit is not None:
            logging.info(f'Setting concurrency limit for {lambda_name} back to {azul_original_concurrency_limit}.')
            client.put_function_concurrency(FunctionName=lambda_name,
                                            ReservedConcurrentExecutions=azul_original_concurrency_limit)
        else:
            logging.info(f'Removed concurrency limit for {lambda_name}.')
            client.delete_function_concurrency(FunctionName=lambda_name)

        client.untag_resource(Resource=lambda_arn, TagKeys=['azul-original-concurrency-limit'])
    else:
        logging.warning(f'{lambda_name} is already enabled.')


def disable_lambda(client: 'botocore.client.Lambda', lambda_name: str, lambda_settings: dict, lambda_tags: dict):
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
        new_tag = {'azul-original-concurrency-limit': str(concurrency_limit)}
        client.tag_resource(Resource=lambda_settings['Configuration']['FunctionArn'], Tags=new_tag)
        client.put_function_concurrency(FunctionName=lambda_name, ReservedConcurrentExecutions=0)
    else:
        logging.warning(f'{lambda_name} is already disabled.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Enables or disables all the lambdas in Azul.')
    parser.add_argument('--enable', dest='enabled', action='store_true', default=True)
    parser.add_argument('--disable', dest='enabled', action='store_false')
    args = parser.parse_args()

    enable_azul_lambdas(args.enabled)
