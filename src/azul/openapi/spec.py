import attr

from azul import (
    JSON,
)
from azul.health import (
    Health,
)
from azul.openapi import (
    format_description,
    params,
    responses,
    schema,
)


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class CommonEndpointSpecs:
    app_name: str

    _up_key = {
        'up': format_description('''
            indicates the overall result of the health check
        '''),
    }

    @property
    def _fast_keys(self):
        return {
            **{
                prop.key: format_description(prop.description)
                for prop in Health.fast_properties[self.app_name]
            },
            **self._up_key
        }

    _all_keys = {
        **{
            prop.key: format_description(prop.description)
            for prop in Health.all_properties
        },
        **_up_key
    }

    def _health_spec(self, health_keys: dict) -> JSON:
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
                            additional_properties=schema.object(
                                additional_properties=True,
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

    @property
    def full_health(self) -> JSON:
        return {
            'method_spec': {
                'summary': 'Complete health check',
                'description': format_description(f'''
                    Health check of the {self.app_name} REST API and all
                    resources it depends on. This may take long time to complete
                    and exerts considerable load on the API. For that reason it
                    should not be requested frequently or by automated
                    monitoring facilities that would be better served by the
                    [`/health/fast`](#operations-Auxiliary-get_health_fast) or
                    [`/health/cached`](#operations-Auxiliary-get_health_cached)
                    endpoints.
                '''),
                **self._health_spec(self._all_keys)
            }
        }

    @property
    def basic_health(self) -> JSON:
        return {
            'method_spec': {
                'summary': 'Basic health check',
                'description': format_description(f'''
                    Health check of only the REST API itself, excluding other
                    resources that it depends on. A 200 response indicates that
                    the {self.app_name} is reachable via HTTP(S) but nothing
                    more.
                '''),
                **self._health_spec(self._up_key)
            }
        }

    @property
    def cached_health(self) -> JSON:
        return {
            'method_spec': {
                'summary': 'Cached health check for continuous monitoring',
                'description': format_description(f'''
                    Return a cached copy of the
                    [`/health/fast`](#operations-Auxiliary-get_health_fast)
                    response. This endpoint is optimized for continuously
                    running, distributed health monitors such as Route 53 health
                    checks. The cache ensures that the {self.app_name} is not
                    overloaded by these types of health monitors. The cache is
                    updated every minute.
                '''),
                **self._health_spec(self._fast_keys)
            }
        }

    @property
    def fast_health(self) -> JSON:
        return {
            'method_spec': {
                'summary': 'Fast health check',
                'description': format_description('''
                    Performance-optimized health check of the REST API and other
                    critical resources tht it depends on. This endpoint can be
                    requested more frequently than
                    [`/health`](#operations-Auxiliary-get_health) but
                    periodically scheduled, automated requests should be made to
                    [`/health/cached`](#operations-Auxiliary-get_health_cached).
                '''),
                **self._health_spec(self._fast_keys)
            }
        }

    @property
    def custom_health(self) -> JSON:
        return {
            'method_spec': {
                'summary': 'Selective health check',
                'description': format_description('''
                    This endpoint allows clients to request a health check on a
                    specific set of resources. Each resource is identified by a
                    *key*, the same key under which the resource appears in a
                    [`/health`](#operations-Auxiliary-get_health) response.
                '''),
                **self._health_spec(self._all_keys)
            },
            'path_spec': {
                'parameters': [
                    params.path(
                        'keys',
                        type_=schema.array(schema.enum(*sorted(Health.all_keys))),
                        description='''
                            A comma-separated list of keys selecting the health
                            checks to be performed. Each key corresponds to an
                            entry in the response.
                        ''')
                ]
            }
        }

    @property
    def openapi(self) -> JSON:
        return {
            'method_spec': {
                'summary': 'Return OpenAPI specifications for this REST API',
                'description': format_description('''
                    This endpoint returns the [OpenAPI specifications]'
                    (https://github.com/OAI/OpenAPI-Specification) for this REST
                    API. These are the specifications used to generate the page
                    you are visiting now.
                '''),
                'responses': {
                    '200': {
                        'description': '200 response',
                        **responses.json_content(
                            schema.object(
                                openapi=str,
                                **{
                                    k: schema.object()
                                    for k in ('info', 'tags', 'servers', 'paths', 'components')
                                }
                            )
                        )
                    }
                },
                'tags': ['Auxiliary']
            }
        }

    @property
    def version(self) -> JSON:
        return {
            'method_spec': {
                'summary': 'Describe current version of this REST API',
                'tags': ['Auxiliary'],
                'responses': {
                    '200': {
                        'description': 'Version endpoint is reachable.',
                        **responses.json_content(
                            schema.object(
                                git=schema.object(
                                    commit=str,
                                    dirty=bool
                                ),
                                changes=schema.array(
                                    schema.object(
                                        title=str,
                                        issues=schema.array(str),
                                        upgrade=schema.array(str),
                                        notes=schema.optional(str)
                                    )
                                )
                            )
                        )
                    }
                }
            }
        }

    @property
    def http_504_response(self) -> JSON:
        return {
            '504': {
                'description': 'Request timed out. When handling this response,'
                               ' clients should wait the number of seconds'
                               ' specified in the `Retry-After` header and then'
                               ' retry the request.'
            }
        }
