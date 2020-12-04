from concurrent.futures import (
    ThreadPoolExecutor,
)
from itertools import (
    chain,
)
import json
import time
from typing import (
    Iterable,
    Mapping,
    Set,
    Tuple,
)

from botocore.exceptions import (
    ClientError,
)
from chalice import (
    ChaliceViewError,
    Response,
)
import requests

from azul import (
    RequirementError,
    cached_property,
    config,
    lru_cache,
    require,
)
from azul.deployment import (
    aws,
)
from azul.es import (
    ESClientFactory,
)
from azul.service.storage_service import (
    StorageService,
)
from azul.types import (
    JSON,
)


# noinspection PyPep8Naming
class health_property(cached_property):
    """
    Use this to decorate any methods you would like to be automatically
    returned by HealthController.as_json(). Be sure to provide a docstring in
    the decorated method.
    """

    @property
    def key(self):
        return self.func.__name__

    @property
    def description(self):
        return self.func.__doc__


class HealthController:
    """
    Encapsulates information about the health status of an Azul deployment. All
    aspects of health are exposed as lazily loaded properties. Instantiating the
    class does not examine any resources, only accessing the individual
    properties does, or using the `to_json` method.
    """

    def __init__(self, lambda_name: str):
        self.lambda_name = lambda_name
        self.storage_service = StorageService()

    def as_json(self, keys: Iterable[str]) -> JSON:
        keys = set(keys)
        if keys:
            require(keys.issubset(self.all_keys()))
        else:
            keys = self.all_keys()
        json = {k: getattr(self, k) for k in keys}
        json['up'] = all(v['up'] for v in json.values())
        return json

    @health_property
    def other_lambdas(self):
        """
        Indicates whether the companion REST API responds to HTTP requests.
        """
        response = {
            lambda_name: self._lambda(lambda_name)
            for lambda_name in config.lambda_names()
            if lambda_name != self.lambda_name
        }
        response['up'] = all(v['up'] for v in response.values())
        return response

    @health_property
    def queues(self):
        """
        Returns information about the SQS queues used by the indexer.
        """
        sqs = aws.resource('sqs')
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

    @health_property
    def progress(self) -> JSON:
        """
        The number of Data Store bundles pending to be indexed and the number
        of index documents in need of updating.
        """
        return {
            'up': True,
            'unindexed_bundles': sum(self.queues[config.notifications_queue_name()].get('messages', {}).values()),
            'unindexed_documents': sum(chain.from_iterable(
                self.queues[config.tallies_queue_name(retry=retry)].get('messages', {}).values()
                for retry in (False, True)
            ))
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

    @health_property
    def api_endpoints(self):
        """
        Indicates whether important service API endpoints are operational.
        """
        endpoints = [
            f'/index/{entity_type}?size=1'
            for entity_type in ('projects', 'samples', 'files', 'bundles')
        ]
        with ThreadPoolExecutor(len(endpoints)) as tpe:
            status = dict(tpe.map(self._api_endpoint, endpoints))
        status['up'] = all(v['up'] for v in status.values())
        return status

    @health_property
    def elasticsearch(self):
        """
        Indicates whether the Elasticsearch cluster is responsive.
        """
        return {
            'up': ESClientFactory.get().ping(),
        }

    @lru_cache
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

    def _make_response(self, body: JSON) -> Response:
        try:
            up = body['up']
        except KeyError:
            status = 400
        else:
            status = 200 if up else 503
        return Response(body=json.dumps(body), status_code=status)

    def basic_health(self) -> Response:
        return self._make_response({'up': True})

    def health(self) -> Response:
        return self._make_response(self.as_json(self.all_keys()))

    def custom_health(self, keys) -> Response:
        if keys is None:
            body = self.as_json(self.all_keys())
        elif isinstance(keys, str):
            assert keys  # Chalice maps empty string to None
            keys = keys.split(',')
            try:
                body = self.as_json(keys)
            except RequirementError:
                body = {'Message': 'Invalid health keys'}
        else:
            body = {'Message': 'Invalid health keys'}
        return self._make_response(body)

    def fast_health(self) -> Response:
        return self._make_response(self._as_json_fast())

    def cached_health(self) -> JSON:
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

    fast_properties: Mapping[str, Set[health_property]] = {
        'indexer': {
            elasticsearch,
            queues,
            progress
        },
        'service': {
            elasticsearch,
            api_endpoints,
        }
    }

    def _as_json_fast(self) -> JSON:
        return self.as_json(p.key for p in self.fast_properties[self.lambda_name])

    def update_cache(self) -> None:
        health_object = dict(time=time.time(), health=self._as_json_fast())
        self.storage_service.put(object_key=f'health/{self.lambda_name}',
                                 data=json.dumps(health_object).encode())

    all_properties: Set[health_property] = {
        p for p in locals().values() if isinstance(p, health_property)
    }

    @classmethod
    def all_keys(cls) -> Set[str]:
        return {p.key for p in cls.all_properties}
