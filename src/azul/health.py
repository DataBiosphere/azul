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
import chalice
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
    R,
    cache,
    cached_property,
    config,
    lru_cache,
    require,
)
from azul.chalice import (
    AppController,
    AzulChaliceApp,
    LambdaMetric,
)
from azul.deployment import (
    aws,
)
from azul.es import (
    ESClientFactory,
)
from azul.openapi import (
    format_description,
    params,
    responses,
    schema,
)
from azul.plugins import (
    MetadataPlugin,
)
from azul.service.storage_service import (
    StorageObjectNotFound,
    StorageService,
)
from azul.types import (
    JSON,
    MutableJSON,
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
            except AssertionError as e:
                if R.caused(e):
                    body = {'Message': 'Invalid health keys'}
                else:
                    raise
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
            except StorageObjectNotFound:
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
    def other_lambdas(self) -> JSON:
        """
        Indicates whether the companion REST API responds to HTTP requests.
        """
        response: MutableJSON = {
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


class HealthApp(AzulChaliceApp):

    @cached_property
    def health_controller(self) -> HealthController:
        return self._controller(HealthController,
                                lambda_name=self.unqualified_app_name)

    def default_routes(self):
        _routes = super().default_routes()
        _app_name = self.unqualified_app_name

        _up_key = {
            'up': format_description('''
                indicates the overall result of the health check
            '''),
        }

        _fast_keys = {
            **{
                prop.key: format_description(prop.description)
                for prop in Health.fast_properties[_app_name]
            },
            **_up_key
        }

        _all_keys = {
            **{
                prop.key: format_description(prop.description)
                for prop in Health.all_properties
            },
            **_up_key
        }

        def _health_spec(health_keys: dict) -> JSON:
            return {
                'responses': {
                    f'{200 if up else 503}': {
                        'description': format_description(f'''
                            {'The' if up else 'At least one of the'} checked resources
                            {'are' if up else 'is not'} healthy.

                            The response consists of the following keys:

                        ''') + ''.join(f'* `{k}` {v}' for k, v in health_keys.items()) + format_description(f'''

                            The top-level `up` key of the response is
                            `{'true' if up else 'false'}`.

                        ''') + (format_description(f'''
                            {'All' if up else 'At least one'} of the nested `up` keys
                            {'are `true`' if up else 'is `false`'}.
                        ''') if len(health_keys) > 1 else ''),
                        **responses.json_content(
                            schema.object(
                                additionalProperties=schema.object(
                                    additionalProperties=True,
                                    up=schema.enum(up)
                                ),
                                up=schema.enum(up)
                            ),
                            example={
                                k: up if k == 'up' else {} for k in health_keys
                            }
                        )
                    } for up in [True, False]
                },
                'tags': ['Auxiliary']
            }

        @self.route(
            '/health',
            methods=['GET'],
            cors=True,
            spec={
                'summary': 'Complete health check',
                'description': format_description(f'''
                            Health check of the {_app_name} REST API and all
                            resources it depends on. This may take long time to complete
                            and exerts considerable load on the API. For that reason it
                            should not be requested frequently or by automated
                            monitoring facilities that would be better served by the
                            [`/health/fast`](#operations-Auxiliary-get_health_fast) or
                            [`/health/cached`](#operations-Auxiliary-get_health_cached)
                            endpoints.
                        '''),
                **_health_spec(_all_keys)
            }
        )
        def health():
            return self.health_controller.health()

        @self.route(
            '/health/basic',
            methods=['GET'],
            cors=True,
            spec={
                'summary': 'Basic health check',
                'description': format_description(f'''
                                Health check of only the REST API itself, excluding other
                                resources that it depends on. A 200 response indicates that
                                the {_app_name} is reachable via HTTP(S) but nothing
                                more.
                            '''),
                **_health_spec(_up_key)
            }
        )
        def basic_health():
            return self.health_controller.basic_health()

        @self.route(
            '/health/cached',
            methods=['GET'],
            cors=True,
            spec={
                'summary': 'Cached health check for continuous monitoring',
                'description': format_description(f'''
                                Return a cached copy of the
                                [`/health/fast`](#operations-Auxiliary-get_health_fast)
                                response. This endpoint is optimized for continuously
                                running, distributed health monitors such as Route 53 health
                                checks. The cache ensures that the {_app_name} is not
                                overloaded by these types of health monitors. The cache is
                                updated every minute.
                            '''),
                **_health_spec(_fast_keys)
            }
        )
        def cached_health():
            return self.health_controller.cached_health()

        @self.route(
            '/health/fast',
            methods=['GET'],
            cors=True,
            spec={
                'summary': 'Fast health check',
                'description': format_description('''
                                Performance-optimized health check of the REST API and other
                                critical resources tht it depends on. This endpoint can be
                                requested more frequently than
                                [`/health`](#operations-Auxiliary-get_health) but
                                periodically scheduled, automated requests should be made to
                                [`/health/cached`](#operations-Auxiliary-get_health_cached).
                            '''),
                **_health_spec(_fast_keys)
            }
        )
        def fast_health():
            return self.health_controller.fast_health()

        @self.route(
            '/health/{keys}',
            methods=['GET'],
            cors=True,
            spec={
                'summary': 'Selective health check',
                'description': format_description('''
                                This endpoint allows clients to request a health check on a
                                specific set of resources. Each resource is identified by a
                                *key*, the same key under which the resource appears in a
                                [`/health`](#operations-Auxiliary-get_health) response.
                            '''),
                **_health_spec(_all_keys)
            }, path_spec={
                'parameters': [
                    params.path(
                        'keys',
                        form=schema.array(schema.enum(*sorted(Health.all_keys))),
                        description='''
                                        A comma-separated list of keys selecting the health
                                        checks to be performed. Each key corresponds to an
                                        entry in the response.
                                    ''')
                ]
            }
        )
        def custom_health(keys: Optional[str] = None):
            return self.health_controller.custom_health(keys)

        @self.metric_alarm(metric=LambdaMetric.errors,
                           threshold=1,
                           period=24 * 60 * 60)
        @self.metric_alarm(metric=LambdaMetric.throttles,
                           threshold=0,
                           period=5 * 60)
        @self.retry(num_retries=0)
        # FIXME: Remove redundant prefix from name
        #        https://github.com/DataBiosphere/azul/issues/5337
        @self.schedule(
            'rate(1 minute)',
            name=self.unqualified_app_name + 'cachehealth'
        )
        def update_health_cache(_event: chalice.app.CloudWatchEvent):
            self.health_controller.update_cache()

        return {
            **_routes,
            **{k: v for k, v in locals().items() if not k.startswith('_')}
        }
