from functools import lru_cache

import requests
from botocore.exceptions import ClientError

from azul import config
from azul.decorators import memoized_property
from azul.deployment import aws
from azul.es import ESClientFactory
from azul.types import JSON


class Health:

    def __init__(self, lambda_name):
        self.lambda_name = lambda_name

    @memoized_property
    def as_json(self) -> JSON:
        return {
            'up': self.up,
            'elastic_search': self.elastic_search,
            'queues': self.queues,
            **({
                lambda_name: self._lambda(lambda_name)
                for lambda_name in config.lambda_names()
                if lambda_name != self.lambda_name
            }),
            'unindexed_bundles': sum(self.queues[config.notify_queue_name].get('messages',{}).values()),

            'unindexed_documents': sum(self.queues[config.document_queue_name].get('messages',{}).values())
        }

    @memoized_property
    def queues(self):
        response = {'up': True}
        for queue in config.all_queue_names:
            try:
                queue_instance = aws.sqs_resource.get_queue_by_name(QueueName=queue).attributes
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
    def elastic_search(self):
        return {
            'up': ESClientFactory.get().ping(),
        }

    @memoized_property
    def up(self):
        return (self.elastic_search['up'] and
                self.queues['up'] and
                all(self._lambda(lambda_name)['up']
                    for lambda_name in config.lambda_names()
                    if lambda_name != self.lambda_name))

    @lru_cache()
    def _lambda(self, lambda_name) -> JSON:
        try:
            up = requests.get(f'{config.lambda_endpoint(lambda_name)}/health/basic').json()['up']
        except BaseException as e:
            return {
                'up': False,
                'message': str(e)
            }
        else:
            return {
                'up': up,
            }

