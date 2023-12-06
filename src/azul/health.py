from collections.abc import (
    Iterable,
    Mapping,
    Set,
)
from itertools import (
    chain,
)
import json
import logging
import random
import time
from typing import (
    ClassVar,
    Optional,
)

import attr
from botocore.exceptions import (
    ClientError,
)
from chalice import (
    ChaliceViewError,
    NotFoundError,
    Response,
)
from furl import (
    furl,
)
import requests

from azul import (
    CatalogName,
    RequirementError,
    cache,
    cached_property,
    config,
    lru_cache,
    require,
)
from azul.chalice import (
    AppController,
)
from azul.deployment import (
    aws,
)
from azul.es import (
    ESClientFactory,
)
from azul.plugins import (
    MetadataPlugin,
)
from azul.service.storage_service import (
    StorageService,
)
from azul.types import (
    JSON,
)

log = logging.getLogger(__name__)


# noinspection PyPep8Naming
class health_property(cached_property):
    """
    Use this to decorate any methods you would like to be automatically
    returned by HealthController.as_json(). Be sure to provide a docstring in
    the decorated method.
    """

    def __get__(self, obj, objtype=None):
        log.info('Getting health property %r', self.key)
        return super().__get__(obj, objtype=objtype)

    @property
    def key(self):
        return self.fget.__name__

    @property
    def description(self):
        return self.fget.__doc__


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class HealthController(AppController):
    lambda_name: str

    @cached_property
    def storage_service(self):
        return StorageService()

    @cache
    def metadata_plugin(self, catalog: CatalogName) -> MetadataPlugin:
        return MetadataPlugin.load(catalog).create()

    def basic_health(self) -> Response:
        return self._make_response({'up': True})

    def health(self) -> Response:
        return self._make_response(self._health.as_json(Health.all_keys))

    def custom_health(self, keys: Optional[str]) -> Response:
        if keys is None:
            body = self._health.as_json(Health.all_keys)
        elif isinstance(keys, str):
            assert keys  # Chalice maps empty string to None
            keys = keys.split(',')
            try:
                body = self._health.as_json(keys)
            except RequirementError:
                body = {'Message': 'Invalid health keys'}
        else:
            body = {'Message': 'Invalid health keys'}
        return self._make_response(body)

    def fast_health(self) -> Response:
        return self._make_response(self._health.as_json_fast())

    def cached_health(self) -> JSON:
        if self.app.catalog != config.default_catalog:
            raise NotFoundError('Health is only cached for default catalog',
                                self.app.catalog, config.default_catalog)
        else:
            try:
                cache = json.loads(self.storage_service.get(f'health/{self.lambda_name}'))
            except self.storage_service.client.exceptions.NoSuchKey:
                raise NotFoundError('Cached health object does not exist')
            else:
                max_age = 2 * 60
                if time.time() - cache['time'] > max_age:
                    raise ChaliceViewError('Cached health object is stale')
                else:
                    body = cache['health']
            return body

    def update_cache(self) -> None:
        assert self.app.catalog == config.default_catalog
        health_object = dict(time=time.time(), health=self._health.as_json_fast())
        self.storage_service.put(object_key=f'health/{self.lambda_name}',
                                 data=json.dumps(health_object).encode())

    @property
    def _health(self):
        # Don't cache. A Health instance is meant to be short-lived since it
        # applies its own caching. If we cached the instance, we'd never observe
        # any changes in health.
        return Health(controller=self, catalog=self.app.catalog)

    def _make_response(self, body: JSON) -> Response:
        try:
            up = body['up']
        except KeyError:
            status = 400
        else:
            status = 200 if up else 503
        return Response(body=json.dumps(body), status_code=status)


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class Health:
    """
    Encapsulates information about the health status of an Azul deployment. All
    aspects of health are exposed as lazily loaded properties. Instantiating the
    class does not examine any resources, only accessing the individual
    properties does, or using the `to_json` method.
    """
    controller: HealthController
    catalog: str
    _random: ClassVar[random.Random] = random.Random()

    @property
    def lambda_name(self):
        return self.controller.lambda_name

    def as_json(self, keys: Iterable[str]) -> JSON:
        keys = set(keys)
        if keys:
            require(keys.issubset(self.all_keys))
        else:
            keys = self.all_keys
        json = {k: getattr(self, k) for k in sorted(keys)}
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
        sqs = aws.resource('sqs', azul_logging=True)
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

    def _api_endpoint(self, entity_type: str) -> JSON:
        relative_url = furl(path=('index', entity_type), args={'size': '1'})
        url = str(config.service_endpoint.join(relative_url))
        log.info('Making HEAD request to %s', url)
        start = time.time()
        response = requests.head(url)
        log.info('Got %s response after %.3fs from HEAD request to %s',
                 response.status_code, time.time() - start, url)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            return {'up': False, 'error': repr(e)}
        else:
            return {'up': True}

    @cached_property
    def entity_types(self):
        return self.controller.metadata_plugin(self.catalog).exposed_indices.keys()

    @health_property
    def api_endpoints(self):
        """
        Indicates whether important service API endpoints are operational.
        """
        entity_type = self._random.choice(list(self.entity_types))
        return self._api_endpoint(entity_type)

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
            url = config.lambda_endpoint(lambda_name).set(path='/health/basic',
                                                          args={'catalog': self.catalog})
            log.info('Requesting %r', url)
            response = requests.get(str(url))
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

    fast_properties: ClassVar[Mapping[str, Iterable[health_property]]] = {
        'indexer': (
            elasticsearch,
            queues,
            progress
        ),
        'service': (
            elasticsearch,
            api_endpoints,
        )
    }

    def as_json_fast(self) -> JSON:
        return self.as_json(p.key for p in self.fast_properties[self.lambda_name])

    all_properties: ClassVar[Iterable[health_property]] = tuple(
        p for p in locals().values() if isinstance(p, health_property)
    )

    all_keys: ClassVar[Set[str]] = frozenset(p.key for p in all_properties)
