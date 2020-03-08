import json
import time
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from typing import (
    Tuple,
    Iterable,
)

from boltons.cacheutils import cachedproperty
import boto3
import requests
from botocore.exceptions import ClientError

from azul import (
    config,
    require,
    RequirementError,
)
from azul.es import ESClientFactory
from azul.service.storage_service import StorageService
from azul.types import JSON
from chalice import (
    Response,
    ChaliceViewError,
)


class HealthController:
    """
    Encapsulates information about the health status of an Azul deployment. All
    aspects of health are exposed as lazily loaded properties. Instantiating the
    class does not examine any resources, only accessing the individual
    properties does, or using the `to_json` method.
    """

    all_keys = {
        'elasticsearch',
        'queues',
        'progress',
        'api_endpoints',
        'other_lambdas',
    }

    keys_by_service = {
        'indexer': {
            'elasticsearch',
            'queues',
            'progress'
        },
        'service': {
            'elasticsearch',
            'api_endpoints',
        }
    }

    endpoints = [f'/repository/{entity_type}?size=1'
                 for entity_type in ('projects', 'samples', 'files', 'bundles')]

    def __init__(self, lambda_name: str):
        self.lambda_name = lambda_name
        self.storage_service = StorageService()

    def as_json(self, keys: Iterable[str]) -> JSON:
        keys = set(keys)
        if keys:
            require(keys.issubset(self.all_keys))
        else:
            keys = self.all_keys
        json = {k: getattr(self, k) for k in keys}
        json['up'] = all(v['up'] for v in json.values())
        return json

    def as_json_fast(self):
        return self.as_json(self.keys_by_service[self.lambda_name])

    def fast_response(self):
        return self.as_json_fast()

    @cachedproperty
    def other_lambdas(self):
        response = {
            lambda_name: self._lambda(lambda_name)
            for lambda_name in config.lambda_names()
            if lambda_name != self.lambda_name
        }
        response['up'] = all(v['up'] for v in response.values())
        return response

    @cachedproperty
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

    @cachedproperty
    def progress(self) -> JSON:
        return {
            'up': True,
            'unindexed_bundles': sum(self.queues[config.notify_queue_name].get('messages', {}).values()),
            'unindexed_documents': sum(self.queues[config.document_queue_name].get('messages', {}).values())
        }

    def _api_endpoint(self, path: str) -> Tuple[str, JSON]:
        url = config.service_endpoint() + path
        response = requests.head(url)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            return url, {'up': False, 'error': repr(e)}
        else:
            return url, {'up': True}

    @cachedproperty
    def api_endpoints(self):
        endpoints = self.endpoints
        with ThreadPoolExecutor(len(endpoints)) as tpe:
            status = dict(tpe.map(self._api_endpoint, endpoints))
        status['up'] = all(v['up'] for v in status.values())
        return status

    @cachedproperty
    def elasticsearch(self):
        return {
            'up': ESClientFactory.get().ping(),
        }

    @cachedproperty
    def up(self):
        return all(getattr(self, k)['up'] for k in self.all_keys)

    @lru_cache()
    def _lambda(self, lambda_name) -> JSON:
        try:
            response = requests.get(config.lambda_endpoint(lambda_name) + '/health/basic')
            response.raise_for_status()
            up = response.json()['up']
        except Exception as e:
            return {
                'up': False,
                'error': repr(e)
            }
        else:
            return {
                'up': up,
            }

    def full_response(self):
        return self.as_json(self.all_keys)

    def response(self, keys):
        if keys is None:
            body = self.full_response()
        elif keys == 'basic':
            body = self.basic_response()
        elif keys == 'cached':
            body = self.cached_response()
        elif keys == 'fast':
            body = self.fast_response()
        elif isinstance(keys, str):
            assert keys  # Chalice maps empty string to None
            keys = keys.split(',')
            try:
                body = self.as_json(keys)
            except RequirementError:
                body = {'Message': 'Invalid health keys'}
        else:
            body = {'Message': 'Invalid health keys'}
        try:
            up = body['up']
        except KeyError:
            status = 400
        else:
            status = 200 if up else 503
        return Response(body=json.dumps(body), status_code=status)

    def generate_cache(self):
        health_object = dict(time=time.time(), health=self.as_json_fast())
        self.storage_service.put(object_key=f'health/{self.lambda_name}', data=json.dumps(health_object))

    def cached_response(self):
        try:
            cache = json.loads(self.storage_service.get(f'health/{self.lambda_name}'))
        except self.storage_service.client.exceptions.NoSuchKey:
            raise ChaliceViewError('Cached health object does not exist')
        else:
            max_age = 2 * 60
            if time.time() - cache['time'] > max_age:
                raise ChaliceViewError('Cached health object is stale')
            else:
                body = cache['health']
        return body

    def basic_response(self):
        return {'up': True}
