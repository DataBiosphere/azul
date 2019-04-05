import argparse
from azul import config, str_to_bool
import boto3
import logging

logger = logging.getLogger(__name__)


def enable_lambda_concurrency(enabled: bool):
    client = boto3.client('lambda')
    paginator = client.get_paginator('list_functions')
    lambda_prefixes = [config.qualified_resource_name(azul_lambda_infix) for azul_lambda_infix in config.lambda_names()]

    assert all(prefix != '' for prefix in lambda_prefixes)

    for lambda_page in paginator.paginate(FunctionVersion='ALL', MaxItems=500):
        for lambda_name in [metadata['FunctionName'] for metadata in lambda_page['Functions']]:

            if any(lambda_name.startswith(prefix) for prefix in lambda_prefixes):
                lambda_settings = client.get_function(FunctionName=lambda_name)
                lambda_arn = lambda_settings['Configuration']['FunctionArn']
                tags = client.list_tags(Resource=lambda_arn)['Tags']
                disabled_by_red_button = tags.get('disabled-by-red-button', 'False')

                if enabled:
                    if not str_to_bool(disabled_by_red_button):
                        logging.warning(f'{lambda_name} is already enabled.')
                        continue

                    old_concurrency_limit = tags.get('old-concurrency-limit')

                    if old_concurrency_limit is not None:
                        logging.info(f'Setting concurrency limit for {lambda_name} back to {old_concurrency_limit}.')
                        client.put_function_concurrency(FunctionName=lambda_name,
                                                        ReservedConcurrentExecutions=int(old_concurrency_limit))
                    else:
                        logging.info(f'Removed concurrency limit for {lambda_name}.')
                        client.delete_function_concurrency(FunctionName=lambda_name)

                    client.untag_resource(Resource=lambda_arn,
                                          TagKeys=['old-concurrency-limit', 'disabled-by-red-button'])
                else:
                    try:
                        concurrency_settings = lambda_settings['Concurrency']
                    except KeyError:
                        # If a lambda doesn't have a limit for concurrency executions, Lambda.Client.get_function()
                        # doesn't return a response with the key, `Concurrency`.
                        concurrency_limit = None
                    else:
                        concurrency_limit = concurrency_settings['ReservedConcurrentExecutions']

                    if str_to_bool(disabled_by_red_button):
                        logging.warning(f'{lambda_name} is already disabled.')
                        continue

                    logging.info(f'Setting concurrency limit for {lambda_name} to zero.')
                    new_tag = {'disabled-by-red-button': 'True'}

                    if concurrency_limit is not None:
                        new_tag.update({'old-concurrency-limit': str(concurrency_limit)})

                    client.tag_resource(Resource=lambda_arn, Tags=new_tag)
                    client.put_function_concurrency(FunctionName=lambda_name, ReservedConcurrentExecutions=0)


parser = argparse.ArgumentParser(description='Enables or disables all the lambdas in Azul.')
parser.add_argument('--enable', dest='enabled', action='store_true', default=True)
parser.add_argument('--disable', dest='enabled', action='store_false')
args = parser.parse_args()

enable_lambda_concurrency(args.enabled)
