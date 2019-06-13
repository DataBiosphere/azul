from functools import lru_cache

import boto3
import requests
from botocore.exceptions import ClientError

from azul import config
from azul.decorators import memoized_property
from azul.es import ESClientFactory
from azul.types import JSON


class Health:

    def __init__(self, lambda_name):
        self.lambda_name = lambda_name

    default_keys = (
        'elastic_search',
        'queues',
        'api_endpoints',
        'other_lambdas',
        'progress'
    )

    endpoints = (
        '/repository/summary', *(
            f'/repository/{entity_type}?size=1'
            for entity_type in ('projects', 'samples', 'files', 'bundles')
        )
    )

    def as_json(self, keys=default_keys) -> JSON:
        json = {k: getattr(self, k) for k in keys if k in self.default_keys}
        json['up'] = all(v['up'] for v in json.values())
        return json

    @memoized_property
    def other_lambdas(self):
        response = {
            lambda_name: self._lambda(lambda_name)
            for lambda_name in config.lambda_names()
            if lambda_name != self.lambda_name
        }
        response['up'] = all(v['up'] for v in response.values())
        return response

    @memoized_property
    def queues(self):
        sqs = boto3.resource('sqs')
        response = {'up': True}
        for queue in config.all_queue_names:
            try:
                queue_instance = sqs.get_queue_by_name(QueueName=queue).attributes
            except ClientError as ex:
                response[queue] = {
                    'up': False,
                    'error': ex.response['Error']['Message']
                }
                response['up'] = False
            else:
                response[queue] = {
                    'up': True,
                    'messages': {
                        'delayed': int(queue_instance['ApproximateNumberOfMessagesDelayed']),
                        'invisible': int(queue_instance['ApproximateNumberOfMessagesNotVisible']),
                        'queued': int(queue_instance['ApproximateNumberOfMessages'])
                    }
                }
        return response

    @memoized_property
    def progress(self) -> JSON:
        return {
            'up': True,
            'unindexed_bundles': sum(self.queues[config.notify_queue_name].get('messages', {}).values()),
            'unindexed_documents': sum(self.queues[config.document_queue_name].get('messages', {}).values())
        }

    @memoized_property
    def api_endpoints(self):
        status = {'up': True}

        for endpoint in self.endpoints:
            response = requests.head(config.service_endpoint() + endpoint)
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                status[config.service_endpoint() + endpoint] = {
                    'up': False,
                    'error': str(e)
                }
                status['up'] = False
            else:
                status[config.service_endpoint() + endpoint] = {
                    'up': True
                }
        return status

    @memoized_property
    def elastic_search(self):
        return {
            'up': ESClientFactory.get().ping(),
        }

    @memoized_property
    def up(self):
        return all(getattr(self, k)['up'] for k in self.default_keys)

    @lru_cache()
    def _lambda(self, lambda_name) -> JSON:
        try:
            up = requests.get(f'{config.lambda_endpoint(lambda_name)}/health/basic').json()['up']
        except BaseException as e:
            return {
                'up': False,
                'error': str(e)
            }
        else:
            return {
                'up': up,
            }
