from collections import (
    ChainMap,
)
from enum import (
    Enum,
)
import functools
import logging
import os
import re
import shlex
from typing import (
    AbstractSet,
    BinaryIO,
    ClassVar,
    Dict,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    TYPE_CHECKING,
    TextIO,
    Tuple,
    Union,
)

import attr
from more_itertools import (
    first,
)

import azul.caching
from azul.caching import (
    lru_cache_per_thread,
)
from azul.json_freeze import (
    freeze,
)
from azul.types import (
    JSON,
    LambdaContext,
)

log = logging.getLogger(__name__)

Netloc = Tuple[str, int]

CatalogName = str

cached_property = property if TYPE_CHECKING else azul.caching.CachedProperty

lru_cache = functools.lru_cache

if TYPE_CHECKING:
    def cache(f, /):
        return f
else:
    def cache(f, /):
        """
        This is anticipating the addition of functools.cache in 3.9
        (https://github.com/python/cpython/blob/3.9/Lib/functools.py#L650)
        """
        return lru_cache(maxsize=None)(f)

if TYPE_CHECKING:
    def cache_per_thread(f, /):
        return f
else:
    def cache_per_thread(f, /):
        return lru_cache_per_thread(maxsize=None)(f)


class Config:
    """
    See `environment` for documentation of these settings.
    """

    @property
    def environ(self):
        return ChainMap(os.environ, self._outsourced_environ)

    @property
    def owner(self):
        return self.environ['AZUL_OWNER']

    def _boolean(self, value: str) -> bool:
        if value == "0":
            return False
        elif value == "1":
            return True
        else:
            raise ValueError('Expected "0" or "1"', value)

    @property
    def debug(self) -> int:
        debug = int(self.environ['AZUL_DEBUG'])
        self._validate_debug(debug)
        return debug

    @debug.setter
    def debug(self, debug: int):
        self._validate_debug(debug)
        self.environ['AZUL_DEBUG'] = str(debug)

    def _validate_debug(self, debug):
        require(debug in (0, 1, 2), "AZUL_DEBUG must be either 0, 1 or 2")

    _es_endpoint_env_name = 'AZUL_ES_ENDPOINT'

    @property
    def es_endpoint(self) -> Optional[Netloc]:
        try:
            es_endpoint = self.environ[self._es_endpoint_env_name]
        except KeyError:
            return None
        else:
            host, _, port = es_endpoint.partition(':')
            return host, int(port)

    def es_endpoint_env(self,
                        es_endpoint: Union[Netloc, str],
                        es_instance_count: Union[int, str]
                        ) -> Mapping[str, str]:
        if isinstance(es_endpoint, tuple):
            host, port = es_endpoint
            assert isinstance(host, str), host
            assert isinstance(port, int), port
            es_endpoint = f'{host}:{port}'
        elif isinstance(es_endpoint, str):
            pass
        else:
            assert False, es_endpoint
        return {
            self._es_endpoint_env_name: es_endpoint,
            self._es_instance_count_env_name: str(es_instance_count)
        }

    @property
    def aws_account_id(self) -> str:
        return self.environ['AZUL_AWS_ACCOUNT_ID']

    @property
    def project_root(self) -> str:
        return self.environ['project_root']

    @property
    def es_domain(self) -> str:
        return self.environ['AZUL_ES_DOMAIN']

    @property
    def share_es_domain(self) -> bool:
        return self._boolean(self.environ['AZUL_SHARE_ES_DOMAIN'])

    @property
    def s3_bucket(self) -> str:
        return self.environ['AZUL_S3_BUCKET']

    @property
    def manifest_expiration(self) -> int:
        """
        Number of days before a manifest will be deleted from the storage bucket
        """
        return 7

    @property
    def manifest_expiration_margin(self) -> float:
        """
        Minimum duration (in seconds) before a manifest in the storage bucket
        is considered too close to expiration for use
        """
        return 60 * 15

    @property
    def url_redirect_full_domain_name(self) -> str:
        return self.environ['AZUL_URL_REDIRECT_FULL_DOMAIN_NAME']

    @property
    def url_redirect_base_domain_name(self) -> str:
        return self.environ['AZUL_URL_REDIRECT_BASE_DOMAIN_NAME']

    @property
    def es_timeout(self) -> int:
        return int(self.environ['AZUL_ES_TIMEOUT'])

    @property
    def data_browser_domain(self):
        dcp_domain = 'data.humancellatlas.org'
        if self.deployment_stage == 'prod':
            return dcp_domain
        elif self.deployment_stage in ('integration', 'staging'):
            return f'{self.deployment_stage}.{dcp_domain}'
        else:
            return f'dev.{dcp_domain}'

    @property
    def data_browser_name(self):
        return f'{self.resource_prefix}-data-browser-{self.deployment_stage}'

    @property
    def data_portal_name(self):
        return f'{self.resource_prefix}-data-portal-{self.deployment_stage}'

    @property
    def dss_endpoint(self) -> Optional[str]:
        if self.dss_source is None:
            return None
        else:
            from azul.indexer import (
                SimpleSourceSpec,
            )
            return SimpleSourceSpec.parse(self.dss_source).name

    @property
    def dss_source(self) -> Optional[str]:
        return self.environ.get('AZUL_DSS_SOURCE')

    def sources(self, catalog: CatalogName) -> AbstractSet[str]:
        return config.catalogs[catalog].sources

    @property
    def tdr_allowed_source_locations(self) -> AbstractSet[str]:
        # FIXME: Eliminate local import
        #        https://github.com/DataBiosphere/azul/issues/3133
        import json
        return frozenset(json.loads(self.environ['AZUL_TDR_ALLOWED_SOURCE_LOCATIONS']))

    @property
    def tdr_source_location(self) -> str:
        location = self.environ['AZUL_TDR_SOURCE_LOCATION']
        allowed_locations = self.tdr_allowed_source_locations
        require(location in allowed_locations,
                f'{location!r} is not one of {allowed_locations!r}')
        return location

    @property
    def tdr_service_url(self) -> str:
        return self.environ['AZUL_TDR_SERVICE_URL']

    @property
    def sam_service_url(self):
        return self.environ['AZUL_SAM_SERVICE_URL']

    @property
    def dss_query_prefix(self) -> str:
        return self.environ.get('AZUL_DSS_QUERY_PREFIX', '')

    @property
    def partition_prefix_length(self) -> int:
        return int(self.environ['AZUL_PARTITION_PREFIX_LENGTH'])

    # Remove once https://github.com/HumanCellAtlas/data-store/issues/1837 is resolved

    def dss_deployment_stage(self, dss_endpoint: str) -> str:
        """
        >>> config.dss_deployment_stage('https://dss.staging.data.humancellatlas.org/v1')
        'staging'
        >>> config.dss_deployment_stage('https://dss.data.humancellatlas.org/v1')
        'prod'
        """
        from urllib.parse import (
            urlparse,
        )
        user, _, domain = urlparse(dss_endpoint).netloc.rpartition('@')
        domain = domain.split('.')
        require(domain[-3:] == ['data', 'humancellatlas', 'org'])
        require(domain[0] == 'dss')
        stage = domain[1:-3]
        assert len(stage) < 2
        return 'prod' if stage == [] else stage[0]

    @property
    def dss_direct_access(self) -> bool:
        return self._boolean(self.environ['AZUL_DSS_DIRECT_ACCESS'])

    def dss_direct_access_role(self, lambda_name: str, stage: Optional[str] = None) -> Optional[str]:
        key = 'AZUL_DSS_DIRECT_ACCESS_ROLE'
        try:
            role_arn = self.environ[key]
        except KeyError:
            return None
        else:
            arn, partition, service, region, account_id, resource = role_arn.split(':')
            require(arn == 'arn')
            require(partition == 'aws')
            require(service == 'iam')
            require(region == '')
            require(account_id)
            resource_type, resource_id = resource.split('/')
            require(resource_type == 'role')
            try:
                lambda_name_template, default_stage = self.unqualified_resource_name(resource_id)
                require(lambda_name_template == '*')
                if stage is None:
                    stage = default_stage
                role_name = self.qualified_resource_name(lambda_name, stage=stage)
                return f'arn:aws:iam::{account_id}:role/{role_name}'
            except RequirementError:
                # If we fail to parse the role name, we can't parameterize it
                # and must return the ARN verbatim.
                return role_arn

    @property
    def num_dss_workers(self) -> int:
        return int(self.environ['AZUL_DSS_WORKERS'])

    @property
    def num_tdr_workers(self) -> int:
        return int(self.environ['AZUL_TDR_WORKERS'])

    @property
    def external_lambda_role_assumptors(self) -> MutableMapping[str, List[str]]:
        try:
            accounts = self.environ['AZUL_EXTERNAL_LAMBDA_ROLE_ASSUMPTORS']
        except KeyError:
            return {}
        else:
            return self._parse_principals(accounts)

    def _parse_principals(self, accounts) -> MutableMapping[str, List[str]]:
        # noinspection PyProtectedMember
        """
        >>> from azul import config  # Without this import, these doctests fail
        ...                          # in Pycharm since the fully qualified
        ...                          # class name of the exception would be
        ...                          # azul.RequirementError

        >>> config._parse_principals('123,foo*')
        {'123': ['foo*']}

        >>> config._parse_principals('123, foo*: 456,bar ,fubaz')
        {'123': ['foo*'], '456': ['bar', 'fubaz']}

        >>> config._parse_principals('')
        Traceback (most recent call last):
        ...
        azul.RequirementError: ('An account ID and at least one role must be specified', '')

        >>> config._parse_principals(' ')
        Traceback (most recent call last):
        ...
        azul.RequirementError: ('An account ID and at least one role must be specified', ' ')

        >>> config._parse_principals(':')
        Traceback (most recent call last):
        ...
        azul.RequirementError: ('An account ID and at least one role must be specified', '')

        >>> config._parse_principals(',')
        Traceback (most recent call last):
        ...
        azul.RequirementError: ('An account ID and at least one role must be specified', ',')

        >>> config._parse_principals(',:')
        Traceback (most recent call last):
        ...
        azul.RequirementError: ('An account ID and at least one role must be specified', ',')

        >>> config._parse_principals('123')
        Traceback (most recent call last):
        ...
        azul.RequirementError: ('An account ID and at least one role must be specified', '123')

        >>> config._parse_principals('123:')
        Traceback (most recent call last):
        ...
        azul.RequirementError: ('An account ID and at least one role must be specified', '123')

        >>> config._parse_principals('123 ,:')
        Traceback (most recent call last):
        ...
        azul.RequirementError: ('An account ID and at least one role must be specified', '123 ,')
        """
        result = {}
        for account in accounts.split(':'):
            account_id, *roles = map(str.strip, account.split(','))
            require(account_id and roles and all(roles),
                    'An account ID and at least one role must be specified', account)
            result[account_id] = roles
        return result

    @property
    def resource_prefix(self):
        prefix = self.environ['AZUL_RESOURCE_PREFIX']
        self.validate_prefix(prefix)
        return prefix

    def qualified_resource_name(self, resource_name, suffix='', stage=None):
        self._validate_term(resource_name)
        if stage is None:
            stage = self.deployment_stage
        return f"{self.resource_prefix}-{resource_name}-{stage}{suffix}"

    def unqualified_resource_name(self,
                                  qualified_resource_name: str,
                                  suffix: str = ''
                                  ) -> Tuple[str, str]:
        """
        >>> config.unqualified_resource_name('azul-foo-dev')
        ('foo', 'dev')

        >>> config.unqualified_resource_name('azul-foo')
        Traceback (most recent call last):
        ...
        azul.RequirementError: ['azul', 'foo']

        >>> config.unqualified_resource_name('azul-object_versions-dev')
        ('object_versions', 'dev')

        >>> config.unqualified_resource_name('azul-object-versions-dev')
        Traceback (most recent call last):
        ...
        azul.RequirementError: ['azul', 'object', 'versions', 'dev']
        """
        require(qualified_resource_name.endswith(suffix))
        if suffix:
            qualified_resource_name = qualified_resource_name[:-len(suffix)]
        # FIXME: Eliminate hardcoded separator
        #        https://github.com/databiosphere/azul/issues/2964
        components = qualified_resource_name.split('-')
        require(len(components) == 3, components)
        prefix, resource_name, deployment_stage = components
        require(prefix == self.resource_prefix)
        return resource_name, deployment_stage

    def subdomain(self, lambda_name):
        return self.environ['AZUL_SUBDOMAIN_TEMPLATE'].replace('*', lambda_name)

    def api_lambda_domain(self, lambda_name: str) -> str:
        return self.subdomain(lambda_name) + "." + self.domain_name

    @property
    def drs_domain(self):
        return self.environ['AZUL_DRS_DOMAIN_NAME']

    def api_lambda_domain_aliases(self, lambda_name):
        """
        Additional alias domain names for the given API lambda
        """
        return [self.drs_domain] if lambda_name == 'service' and self.drs_domain else []

    def lambda_endpoint(self, lambda_name: str) -> str:
        return "https://" + self.api_lambda_domain(lambda_name)

    def indexer_endpoint(self) -> str:
        return self.lambda_endpoint('indexer')

    def service_endpoint(self) -> str:
        return self.lambda_endpoint('service')

    def drs_endpoint(self):
        if self.drs_domain:
            return "https://" + self.drs_domain
        else:
            return self.service_endpoint()

    def lambda_names(self) -> List[str]:
        return ['indexer', 'service']

    @property
    def indexer_name(self) -> str:
        return self.indexer_function_name()

    @property
    def service_name(self) -> str:
        return self.service_function_name()

    def indexer_function_name(self, handler_name: Optional[str] = None):
        return self._function_name('indexer', handler_name)

    def service_function_name(self, handler_name: Optional[str] = None):
        return self._function_name('service', handler_name)

    def _function_name(self, lambda_name: str, handler_name: Optional[str]):
        if handler_name is None:
            return self.qualified_resource_name(lambda_name)
        else:
            # FIXME: Eliminate hardcoded separator
            #        https://github.com/databiosphere/azul/issues/2964
            return self.qualified_resource_name(lambda_name, suffix='-' + handler_name)

    deployment_name_re = re.compile(r'[a-z][a-z0-9]{1,16}')

    @classmethod
    def validate_prefix(cls, prefix):
        reject(cls.deployment_name_re.fullmatch(prefix) is None,
               f"Prefix '{prefix}' is to short, too long or contains invalid characters.")

    @classmethod
    def validate_deployment_name(cls, deployment_name):
        reject(cls.deployment_name_re.fullmatch(deployment_name) is None,
               f"Deployment name '{deployment_name}' is to short, too long or contains invalid characters.")

    @property
    def deployment_stage(self) -> str:
        deployment_name = self.environ['AZUL_DEPLOYMENT_STAGE']
        self.validate_deployment_name(deployment_name)
        return deployment_name

    @property
    def region(self) -> str:
        return self.environ['AWS_DEFAULT_REGION']

    @property
    def terraform_backend_bucket(self) -> str:
        return self.versioned_bucket

    @property
    def versioned_bucket(self):
        return self.environ['AZUL_VERSIONED_BUCKET']

    @property
    def enable_monitoring(self) -> bool:
        return self._boolean(self.environ['AZUL_ENABLE_MONITORING'])

    @property
    def disable_monitoring(self) -> bool:
        return not self.enable_monitoring

    @property
    def es_instance_type(self) -> str:
        return self.environ['AZUL_ES_INSTANCE_TYPE']

    _es_instance_count_env_name = 'AZUL_ES_INSTANCE_COUNT'

    @property
    def es_instance_count(self) -> int:
        return int(self.environ[self._es_instance_count_env_name])

    @property
    def es_volume_size(self) -> int:
        return int(self.environ['AZUL_ES_VOLUME_SIZE'])

    @property
    def _index_prefix(self) -> str:
        return self._term_from_env('AZUL_INDEX_PREFIX')

    # Because this property is relatively expensive to produce and frequently
    # used we are applying aggressive caching here, knowing very well that
    # this eliminates the option to reconfigure the running process by
    # manipulating os.environ['AZUL_CATALOGS'].
    #
    # It also means that mocking/patching would need to be done on this property
    # and that the mocked property would be inconsistent with the environment
    # variable. We feel that the performance gain is worth these concessions.

    @attr.s(frozen=True, kw_only=True, auto_attribs=True)
    class Catalog:
        """
        >>> plugins = dict(metadata=dict(name='hca'), repository=dict(name='tdr'))
        >>> kwargs = dict(atlas='', plugins=plugins, sources='')
        >>> c = Config.Catalog

        >>> c(name='dcp', internal=False, **kwargs) # doctest: +NORMALIZE_WHITESPACE
        Config.Catalog(name='dcp',
                       atlas='',
                       internal=False,
                       plugins={'metadata': {'name': 'hca'},
                                'repository': {'name': 'tdr'}},
                       sources='')

        >>> c(name='dcp-it', internal=True, **kwargs).is_integration_test_catalog
        True

        >>> c(name='foo-bar', internal=False, **kwargs).name
        'foo-bar'

        >>> c(name='foo-bar-it', internal=True, **kwargs).name
        'foo-bar-it'

        >>> c(name='a' * 61 + '-it', internal=True, **kwargs).is_integration_test_catalog
        True

        >>> c(name='a' * 62 + '-it', internal=True, **kwargs) # doctest: +NORMALIZE_WHITESPACE
        Traceback (most recent call last):
        ...
        azul.RequirementError: ('Catalog name is invalid',
                                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-it')
        """

        @attr.s(frozen=True, kw_only=True, auto_attribs=True)
        class Plugin:
            name: str

        name: str
        atlas: str
        internal: bool
        plugins: Mapping[str, Plugin]
        sources: AbstractSet[str]

        _catalog_re = r'([a-z][a-z0-9]*(-[a-z0-9]+)*)'
        _catalog_re = r'(?=.{1,64}$)' + _catalog_re
        _it_catalog_suffix: ClassVar[str] = '-it'
        _it_catalog_re: ClassVar[re.Pattern] = re.compile(
            _catalog_re + rf'(?<={re.escape(_it_catalog_suffix)})'
        )
        _catalog_re: ClassVar[re.Pattern] = re.compile(_catalog_re)

        def __attrs_post_init__(self):
            self.validate_name(self.name)
            # Import locally to avoid cyclical import
            from azul.plugins import (
                Plugin,
            )
            all_types = set(p.type_name() for p in Plugin.types())
            configured_types = self.plugins.keys()
            require(all_types == configured_types,
                    'Missing or extra plugin types',
                    all_types.symmetric_difference(configured_types))
            if self.internal:
                assert self.is_integration_test_catalog is True, self

        @cached_property
        def is_integration_test_catalog(self) -> bool:
            if self._it_catalog_re.match(self.name) is None:
                return False
            else:
                require(self.internal, 'IT catalogs must be internal', self)
                return True

        @cached_property
        def it_catalog(self) -> CatalogName:
            if self.is_integration_test_catalog:
                return self.name
            else:
                name = self.name + self._it_catalog_suffix
                assert self._it_catalog_re.match(name), name
                return name

        @classmethod
        def from_json(cls, name: str, spec: JSON) -> 'Config.Catalog':
            plugins = {
                plugin_type: cls.Plugin(**plugin_spec)
                for plugin_type, plugin_spec in spec['plugins'].items()
            }
            return cls(name=name,
                       atlas=spec['atlas'],
                       internal=spec['internal'],
                       plugins=plugins,
                       sources=set(spec['sources']))

        @classmethod
        def validate_name(cls, catalog, **kwargs):
            reject(cls._catalog_re.fullmatch(catalog) is None,
                   'Catalog name is invalid', catalog, **kwargs)

    @cached_property
    def catalogs(self) -> Mapping[CatalogName, Catalog]:
        """
        A mapping from catalog name to a mapping from plugin type to plugin
        package name.
        """
        # FIXME: Eliminate local import
        #        https://github.com/DataBiosphere/azul/issues/3133
        import json
        catalogs = json.loads(self.environ['AZUL_CATALOGS'])
        require(bool(catalogs), 'No catalogs configured')
        return {
            name: self.Catalog.from_json(name, catalog)
            for name, catalog in catalogs.items()
        }

    @property
    def default_catalog(self) -> CatalogName:
        return first(self.catalogs)

    def it_catalog_for(self, catalog: CatalogName) -> Optional[CatalogName]:
        it_catalog = self.catalogs[catalog].it_catalog
        assert it_catalog in self.integration_test_catalogs, it_catalog
        return it_catalog

    def is_dss_enabled(self, catalog: Optional[str] = None) -> bool:
        return self._is_plugin_enabled('dss', catalog)

    def is_tdr_enabled(self, catalog: Optional[str] = None) -> bool:
        return self._is_plugin_enabled('tdr', catalog)

    def _is_plugin_enabled(self, plugin: str, catalog: Optional[str]) -> bool:
        def predicate(catalog):
            return catalog.plugins['repository'].name == plugin

        if catalog is None:
            return any(map(predicate, self.catalogs.values()))
        else:
            return predicate(self.catalogs[catalog])

    @cached_property
    def integration_test_catalogs(self) -> Mapping[CatalogName, Catalog]:
        return {
            name: catalog
            for name, catalog in self.catalogs.items()
            if catalog.is_integration_test_catalog
        }

    def es_index_name(self, catalog: CatalogName, entity_type: str, aggregate: bool) -> str:
        return str(IndexName(prefix=self._index_prefix,
                             version=2,
                             deployment=self.deployment_stage,
                             catalog=catalog,
                             entity_type=entity_type,
                             aggregate=aggregate))

    def parse_es_index_name(self, index_name: str) -> 'IndexName':
        """
        Parse the name of an index in the current deployment.
        """
        index_name = IndexName.parse(index_name)
        assert index_name.prefix == self._index_prefix
        assert index_name.deployment == self.deployment_stage
        return index_name

    def parse_foreign_es_index_name(self, index_name) -> 'IndexName':
        """
        Parse the name of an index in any deployment and from any version of
        Azul provided that the deployment doesn't override the default index
        name prefix (AZUL_INDEX_PREFIX).
        """
        return IndexName.parse(index_name, expected_prefix=self._index_prefix)

    @property
    def domain_name(self) -> str:
        return self.environ['AZUL_DOMAIN_NAME']

    main_deployments_by_branch: Mapping[str, Sequence[str]] = freeze({
        'develop': ['dev'],
        'integration': ['integration'],
        'staging': ['staging'],
        'prod': ['prod', 'prod2']
    })

    main_branches_by_deployment: Mapping[str, str] = freeze({
        deployment: branch
        for branch, deployments in main_deployments_by_branch.items()
        for deployment in deployments
    })

    def is_main_deployment(self, stage: str = None) -> bool:
        if stage is None:
            stage = self.deployment_stage
        return stage in self.main_branches_by_deployment

    def is_stable_deployment(self, stage=None) -> bool:
        if stage is None:
            stage = self.deployment_stage
        return stage in ('staging', 'prod')

    @property
    def _git_status(self) -> Mapping[str, str]:
        import git
        repo = git.Repo(config.project_root)
        return {
            'azul_git_commit': repo.head.object.hexsha,
            'azul_git_dirty': str(repo.is_dirty()),
        }

    @property
    def lambda_git_status(self) -> Mapping[str, str]:
        return {
            'commit': self.environ['azul_git_commit'],
            'dirty': str_to_bool(self.environ['azul_git_dirty'])
        }

    @property
    def lambda_env(self) -> Dict[str, str]:
        """
        A dictionary with the environment variables to be used by a deployed AWS
        Lambda function or `chalice local`. Only includes those variables that
        don't need to be outsourced.
        """
        return {
            **self._lambda_env(outsource=False),
            **self._git_status
        }

    @property
    def lambda_env_for_outsourcing(self) -> Dict[str, str]:
        """
        Same as :meth:`lambda_env` but only for variables that need to be
        outsourced.
        """
        return self._lambda_env(outsource=True)

    def _lambda_env(self, *, outsource: bool) -> Dict[str, str]:
        return {
            k: v
            for k, v in os.environ.items()
            if k.startswith('AZUL_') and (len(v) > 128) == outsource
        }

    @cached_property
    def _outsourced_environ(self) -> Dict[str, str]:
        # FIXME: Eliminate local import
        #        https://github.com/DataBiosphere/azul/issues/3133
        import json
        try:
            with open_resource('environ.json') as f:
                return json.load(f)
        except NotInLambdaContextException:
            # An outsourced environment is only defined in a Lambda context,
            # outside of one the real environment still contains all variables
            # that would be outsourced in a Lambda context.
            return {}

    def contribution_lambda_timeout(self, *, retry: bool) -> int:
        return (15 if retry else 5) * 60

    def aggregation_lambda_timeout(self, *, retry: bool) -> int:
        return (10 if retry else 1) * 60

    # For the period from 05/31/2020 to 06/06/2020, the max was 18s and the
    # average + 3 standard deviations was 1.8s in the `dev` deployment.
    #
    health_lambda_timeout = 10

    service_lambda_timeout = 15 * 60

    api_gateway_timeout = 29

    # The number of seconds to extend the timeout of a Lambda fronted by API Gateway so that API Gateway times out
    # before the Lambda. We pad the Lambda timeout so we get consistent behaviour. Without this padding we'd have a
    # race between the Lambda being killed and API Gateway timing out.
    #
    api_gateway_timeout_padding = 2

    @property
    def api_gateway_lambda_timeout(self) -> int:
        return self.api_gateway_timeout + self.api_gateway_timeout_padding

    # This is set dynamically at runtime
    lambda_context: Optional[LambdaContext] = None

    @property
    def terra_client_timeout(self) -> float:
        value = os.environ['AZUL_TERRA_TIMEOUT']
        short_timeout, long_timeout = map(float, value.split(':'))
        require(short_timeout <= long_timeout, short_timeout, long_timeout)
        if self.lambda_context is None:
            return long_timeout
        else:
            remaining = self.lambda_context.get_remaining_time_in_millis() / 1000
            return (short_timeout
                    if remaining <= self.api_gateway_lambda_timeout else
                    long_timeout)

    term_re = re.compile("[a-z][a-z0-9_]{1,28}[a-z0-9]")

    def _term_from_env(self, env_var_name: str, optional=False) -> str:
        value = self.environ.get(env_var_name, default='')
        if value == '' and optional:
            return value
        else:
            self._validate_term(value, name=env_var_name)
            return value

    @classmethod
    def _validate_term(cls, term: str, name: str = 'Term') -> None:
        require(cls.term_re.fullmatch(term) is not None,
                f"{name} is either too short, too long or contains invalid characters: '{term}'")

    @classmethod
    def validate_entity_type(cls, entity_type: str) -> None:
        cls._validate_term(entity_type, name='entity_type')

    def secrets_manager_secret_name(self, *args):
        return '/'.join(['dcp', 'azul', self.deployment_stage, *args])

    def enable_gcp(self):
        return 'GOOGLE_PROJECT' in self.environ

    class ServiceAccount(Enum):
        indexer = ''
        public = '_public'
        unregistered = '_unregistered'

        @property
        def id(self) -> str:
            return os.environ['AZUL_GOOGLE_SERVICE_ACCOUNT' + self.value.upper()]

        @property
        def secret_name(self) -> str:
            return 'google_service_account' + self.value

    def state_machine_name(self, lambda_name):
        return config.qualified_resource_name(lambda_name)

    def _concurrency(self, value: str, retry: bool) -> int:
        """
        >>> config._concurrency('123', False)
        123
        >>> config._concurrency('123', True)
        123
        >>> config._concurrency('123/456', False)
        123
        >>> config._concurrency('123/456', True)
        456
        >>> config._concurrency('foo', False)
        Traceback (most recent call last):
        ...
        ValueError: invalid literal for int() with base 10: 'foo'
        >>> config._concurrency('123/foo', False)
        Traceback (most recent call last):
        ...
        ValueError: invalid literal for int() with base 10: 'foo'
        >>> config._concurrency('123/', False)
        Traceback (most recent call last):
        ...
        ValueError: invalid literal for int() with base 10: ''
        >>> config._concurrency('123/456/789', False)
        Traceback (most recent call last):
        ...
        ValueError: invalid literal for int() with base 10: '456/789'
        """
        value, sep, retry_value = value.partition('/')
        value = int(value)
        if sep:
            retry_value = int(retry_value)
        else:
            assert not retry_value
            retry_value = value
        return retry_value if retry else value

    def contribution_concurrency(self, *, retry: bool) -> int:
        return self._concurrency(self.environ['AZUL_CONTRIBUTION_CONCURRENCY'], retry)

    def aggregation_concurrency(self, *, retry: bool) -> int:
        return self._concurrency(self.environ['AZUL_AGGREGATION_CONCURRENCY'], retry)

    @property
    def bigquery_reserved_slots(self) -> int:
        """
        The number of BigQuery slots to reserve when reindexing a catalog from a
        repository that stores its data in Google BigQuery.
        """
        # Slots must be purchased in intervals of 100
        min_slots = 100
        concurrency = self.contribution_concurrency(retry=False)
        return max(1, round(concurrency / min_slots)) * min_slots

    @property
    def bigquery_batch_mode(self) -> bool:
        return self._boolean(os.environ['AZUL_BIGQUERY_BATCH_MODE'])

    def notifications_queue_name(self, *, retry=False, fail=False) -> str:
        name = self.unqual_notifications_queue_name(retry=retry, fail=fail)
        return self.qualified_resource_name(name)

    def unqual_notifications_queue_name(self, *, retry=False, fail=False):
        return self._unqual_queue_name('notifications', retry, fail)

    def tallies_queue_name(self, *, retry=False, fail=False) -> str:
        name = self.unqual_tallies_queue_name(retry=retry, fail=fail)
        return config.qualified_resource_name(name, suffix='.fifo')

    def unqual_tallies_queue_name(self, *, retry=False, fail=False):
        return self._unqual_queue_name('tallies', retry, fail)

    def _unqual_queue_name(self, basename: str, retry: bool, fail: bool) -> str:
        parts = [basename]
        if fail:
            assert not retry
            parts.append('fail')
        elif retry:
            parts.append('retry')
        return '_'.join(parts)

    @property
    def all_queue_names(self) -> List[str]:
        return self.work_queue_names + self.fail_queue_names

    @property
    def fail_queue_names(self) -> List[str]:
        return [
            self.tallies_queue_name(fail=True),
            self.notifications_queue_name(fail=True)
        ]

    @property
    def work_queue_names(self) -> List[str]:
        return [
            queue_name(retry=retry)
            for queue_name in (self.notifications_queue_name, self.tallies_queue_name)
            for retry in (False, True)
        ]

    url_shortener_whitelist = [
        r'([^.]+\.)*humancellatlas\.org',
        r'([^.]+\.)*singlecell\.gi\.ucsc\.edu'
    ]

    @property
    def es_refresh_interval(self) -> int:
        """
        Integral number of seconds between index refreshes in Elasticsearch
        """
        return 1

    # FIXME: Should depend on ES instance size
    #        https://github.com/DataBiosphere/azul/issues/2903
    #        https://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/aes-limits.html#network-limits
    max_chunk_size = 10 * 1024 * 1024

    #: The maximum number of contributions to read in a single request during
    #: aggregation. A value that's too large could cause the response to be
    #: truncated by AWS, resulting a SerializationError. A value that's too
    #: small will result in poor performance due to latency accruing from an
    #: excessive number of requests being made.
    #:
    contribution_page_size = 100

    @property
    def terraform_component(self):
        return self._term_from_env('azul_terraform_component', optional=True)

    permissions_boundary_name = 'azul-boundary'

    @property
    def github_project(self) -> str:
        return self.environ['azul_github_project']

    @property
    def github_access_token(self) -> str:
        return self.environ['azul_github_access_token']

    @property
    def portal_db_bucket(self) -> str:
        return self.versioned_bucket

    def portal_db_object_key(self, catalog_source: str) -> str:
        return f'azul/{self.deployment_stage}/portals/{catalog_source}-db.json'

    @property
    def lambda_layer_bucket(self) -> str:
        return self.versioned_bucket

    @property
    def lambda_layer_key(self) -> str:
        return 'lambda_layers'

    @property
    def dynamo_object_version_table_name(self) -> str:
        return self.qualified_resource_name('object_versions')

    @property
    def dynamo_sources_cache_table_name(self) -> str:
        return self.qualified_resource_name('sources_cache_by_auth')

    @property
    def reindex_sources(self) -> List[str]:
        sources = shlex.split(self.environ.get('azul_reindex_sources', '*'))
        require(sources, 'Sources cannot be empty', sources)
        return sources

    terms_aggregation_size = 99999

    precision_threshold = 40000

    minimum_compression_size = 0

    @property
    def google_oauth2_client_id(self) -> Optional[str]:
        return os.environ.get('AZUL_GOOGLE_OAUTH2_CLIENT_ID')


config: Config = Config()  # yes, the type hint does help PyCharm


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class IndexName:
    """
    The name of an Elasticsearch index used by an Azul deployment, parsed into
    its components. The index naming scheme underwent a number of changes during
    the evolution of Azul. The different naming schemes are captured in a
    `version` component. Note that the first version of the index name syntax
    did not carry an explicit version. The resulting ambiguity requires entity
    types to not match the version regex below.
    """
    #: Every index name starts with this prefix
    prefix: str = 'azul'

    #: The version of the index naming scheme
    version: int

    #: The name of the deployment the index belongs to
    deployment: str

    #: The catalog the index belongs to or None for v1 indices.
    catalog: Optional[CatalogName] = attr.ib(default=None)

    #: The type of entities this index contains metadata about
    entity_type: str

    #: Whether the documents in the index are contributions or aggregates
    aggregate: bool = False

    index_name_version_re: ClassVar[re.Pattern] = re.compile(r'v(\d+)')

    def __attrs_post_init__(self):
        """
        >>> IndexName(prefix='azul', version=1, deployment='dev', entity_type='foo_bar')
        IndexName(prefix='azul', version=1, deployment='dev', catalog=None, entity_type='foo_bar', aggregate=False)

        >>> IndexName(prefix='azul', version=1, deployment='dev', catalog=None, entity_type='foo_bar')
        IndexName(prefix='azul', version=1, deployment='dev', catalog=None, entity_type='foo_bar', aggregate=False)

        >>> IndexName(prefix='azul', version=2, deployment='dev', catalog='main', entity_type='foo_bar')
        IndexName(prefix='azul', version=2, deployment='dev', catalog='main', entity_type='foo_bar', aggregate=False)

        >>> IndexName(prefix='azul', version=1, deployment='dev', catalog='hca', entity_type='foo')
        Traceback (most recent call last):
        ...
        azul.RequirementError: Version 1 prohibits a catalog name ('hca').

        >>> IndexName(prefix='azul', version=2, deployment='dev', entity_type='foo')
        Traceback (most recent call last):
        ...
        azul.RequirementError: Version 2 requires a catalog name (None).

        >>> IndexName(prefix='azul', version=2, deployment='dev', catalog=None, entity_type='foo')
        Traceback (most recent call last):
        ...
        azul.RequirementError: Version 2 requires a catalog name (None).

        >>> IndexName(prefix='_', version=2, deployment='dev', catalog='foo', entity_type='bar')
        Traceback (most recent call last):
        ...
        azul.RequirementError: Prefix '_' is to short, too long or contains invalid characters.

        >>> IndexName(prefix='azul', version=2, deployment='_', catalog='foo', entity_type='bar')
        Traceback (most recent call last):
        ...
        azul.RequirementError: Deployment name '_' is to short, too long or contains invalid characters.

        >>> IndexName(prefix='azul', version=2, deployment='dev', catalog='_', entity_type='bar')
        Traceback (most recent call last):
        ...
        azul.RequirementError: ('Catalog name is invalid', '_')

        >>> IndexName(prefix='azul', version=2, deployment='dev', catalog='foo', entity_type='_')
        Traceback (most recent call last):
        ...
        azul.RequirementError: entity_type is either too short, too long or contains invalid characters: '_'
        """
        Config.validate_prefix(self.prefix)
        require(self.version > 0, f'Version must be at least 1, not {self.version}.')
        Config.validate_deployment_name(self.deployment)
        if self.version == 1:
            require(self.catalog is None,
                    f'Version {self.version} prohibits a catalog name ({self.catalog!r}).')
        else:
            require(self.catalog is not None,
                    f'Version {self.version} requires a catalog name ({self.catalog!r}).')
            config.Catalog.validate_name(self.catalog)
        Config.validate_entity_type(self.entity_type)
        assert '_' not in self.prefix, self.prefix
        assert '_' not in self.deployment, self.deployment
        assert self.catalog is None or '_' not in self.catalog, self.catalog

    @classmethod
    def parse(cls, index_name, expected_prefix=prefix) -> 'IndexName':
        """
        Parse the name of an index from any deployment and any version of Azul.

        >>> IndexName.parse('azul_foo_dev')
        IndexName(prefix='azul', version=1, deployment='dev', catalog=None, entity_type='foo', aggregate=False)

        >>> IndexName.parse('azul_foo_aggregate_dev')
        IndexName(prefix='azul', version=1, deployment='dev', catalog=None, entity_type='foo', aggregate=True)

        >>> IndexName.parse('azul_foo_bar_dev')
        IndexName(prefix='azul', version=1, deployment='dev', catalog=None, entity_type='foo_bar', aggregate=False)

        >>> IndexName.parse('azul_foo_bar_aggregate_dev')
        IndexName(prefix='azul', version=1, deployment='dev', catalog=None, entity_type='foo_bar', aggregate=True)

        >>> IndexName.parse('good_foo_dev', expected_prefix='good')
        IndexName(prefix='good', version=1, deployment='dev', catalog=None, entity_type='foo', aggregate=False)

        >>> IndexName.parse('bad_foo_dev')
        Traceback (most recent call last):
        ...
        azul.RequirementError: bad

        >>> IndexName.parse('azul_dev')
        Traceback (most recent call last):
        ...
        azul.RequirementError: ['azul', 'dev']

        >>> IndexName.parse('azul_aggregate_dev') # doctest: +ELLIPSIS
        Traceback (most recent call last):
        ...
        azul.RequirementError: entity_type ... ''

        >>> IndexName.parse('azul_v2_dev_main_foo')
        IndexName(prefix='azul', version=2, deployment='dev', catalog='main', entity_type='foo', aggregate=False)

        >>> IndexName.parse('azul_v2_dev_main_foo_aggregate')
        IndexName(prefix='azul', version=2, deployment='dev', catalog='main', entity_type='foo', aggregate=True)

        >>> IndexName.parse('azul_v2_dev_main_foo_bar')
        IndexName(prefix='azul', version=2, deployment='dev', catalog='main', entity_type='foo_bar', aggregate=False)

        >>> IndexName.parse('azul_v2_dev_main_foo_bar_aggregate')
        IndexName(prefix='azul', version=2, deployment='dev', catalog='main', entity_type='foo_bar', aggregate=True)

        >>> IndexName.parse('azul_v2_staging_hca_foo_bar_aggregate')
        IndexName(prefix='azul', version=2, deployment='staging', catalog='hca', entity_type='foo_bar', aggregate=True)

        >>> IndexName.parse('azul_v2_staging__foo_bar__aggregate') # doctest: +ELLIPSIS
        Traceback (most recent call last):
            ...
        azul.RequirementError: entity_type ... 'foo_bar_'

        >>> IndexName.parse('azul_v3_bla')
        Traceback (most recent call last):
        ...
        azul.RequirementError: 3

        """
        index_name = index_name.split('_')
        require(len(index_name) > 2, index_name)
        prefix, *index_name = index_name
        require(prefix == expected_prefix, prefix)
        version = cls.index_name_version_re.fullmatch(index_name[0])
        if version:
            _, *index_name = index_name
            version = int(version.group(1))
            require(version == 2, version)
            deployment, catalog, *index_name = index_name
        else:
            version = 1
            catalog = None
            *index_name, deployment = index_name
        if index_name[-1] == 'aggregate':
            *index_name, _ = index_name
            aggregate = True
        else:
            aggregate = False
        entity_type = '_'.join(index_name)
        Config.validate_entity_type(entity_type)
        return cls(prefix=prefix,
                   version=version,
                   deployment=deployment,
                   catalog=catalog,
                   entity_type=entity_type,
                   aggregate=aggregate)

    def __str__(self) -> str:
        """
        >>> str(IndexName(version=1, deployment='dev', entity_type='foo'))
        'azul_foo_dev'

        >>> str(IndexName(version=1, deployment='dev', entity_type='foo', aggregate=True))
        'azul_foo_aggregate_dev'

        >>> str(IndexName(version=1, deployment='dev', entity_type='foo_bar'))
        'azul_foo_bar_dev'

        >>> str(IndexName(version=1, deployment='dev', entity_type='foo_bar', aggregate=True))
        'azul_foo_bar_aggregate_dev'

        >>> str(IndexName(version=2, deployment='dev', catalog='main', entity_type='foo'))
        'azul_v2_dev_main_foo'

        >>> str(IndexName(version=2, deployment='dev', catalog='main', entity_type='foo', aggregate=True))
        'azul_v2_dev_main_foo_aggregate'

        >>> str(IndexName(version=2, deployment='dev', catalog='main', entity_type='foo_bar'))
        'azul_v2_dev_main_foo_bar'

        >>> str(IndexName(version=2, deployment='dev', catalog='main', entity_type='foo_bar', aggregate=True))
        'azul_v2_dev_main_foo_bar_aggregate'

        >>> str(IndexName(version=2, deployment='staging', catalog='hca', entity_type='foo_bar', aggregate=True))
        'azul_v2_staging_hca_foo_bar_aggregate'
        """
        aggregate = ['aggregate'] if self.aggregate else []
        if self.version == 1:
            require(self.catalog is None)
            return '_'.join([
                self.prefix,
                self.entity_type,
                *aggregate,
                self.deployment
            ])
        elif self.version == 2:
            require(self.catalog is not None, self.catalog)
            return '_'.join([
                self.prefix,
                f'v{self.version}',
                self.deployment,
                self.catalog,
                self.entity_type,
                *aggregate,
            ])
        else:
            assert False, self.version


class RequirementError(RuntimeError):
    """
    Unlike assertions, unsatisfied requirements do not consitute a bug in the program.
    """


def require(condition: bool, *args, exception: type = RequirementError):
    """
    Raise a RequirementError, or an instance of the given exception class, if
    the given condition is False.

    :param condition: The boolean condition to be required.

    :param args: optional positional arguments to be passed to the exception
                 constructor. Typically this should be a string containing a
                 textual description of the requirement, and optionally one or
                 more values involved in the required condition.

    :param exception: A custom exception class to be instantiated and raised if
                      the condition does not hold.
    """
    reject(not condition, *args, exception=exception)


def reject(condition: bool, *args, exception: type = RequirementError):
    """
    Raise a RequirementError, or an instance of the given exception class, if
    the given condition is True.

    :param condition: The boolean condition to be rejected.

    :param args: Optional positional arguments to be passed to the exception
                 constructor. Typically this should be a string containing a
                 textual description of the rejected condition, and optionally
                 one or more values involved in the rejected condition.

    :param exception: A custom exception class to be instantiated and raised if
                      the condition occurs.
    """
    if condition:
        raise exception(*args)


def open_resource(*path: str,
                  package_root: Optional[str] = None,
                  binary=False) -> Union[TextIO, BinaryIO]:
    """
    Return a file object for the resources at the given path. A resource is
    a source file that can be loaded at runtime. Resources typically aren't
    Python code. We further distinguish between static resources that are
    committed to source control and dynamic ones that are generated at build
    time. Static resources can be accessed by passing 'static' as the first
    positional argument.

    This method must be called from within a real AWS Lambda execution context.
    A fake one created by `chalice local` or LocalAppTestCase will do provided
    that the `package_root` argument is passed and points to the directory
    that contains the `app.py` module and the `vendor` directory.

    :param path: The path to the resource relative to the `vendor/resources`
                 directory. The last positional argument is the file name.

    :param package_root: See description above

    :param binary: True to load a binary resource
    """
    require(len(path) > 0, 'Must pass at least the file name of the resource')
    if package_root is None:
        module_dir = os.path.dirname(os.path.abspath(__file__))
        assert module_dir.endswith('/azul'), module_dir
        package_root = os.path.dirname(module_dir)
    reject(package_root.endswith('/src'), package_root,
           exception=NotInLambdaContextException)
    vendor_dir = os.path.join(package_root, 'vendor')
    # The `chalice package` command dissolves the content of the `vendor`
    # directory into the package root so in a deployed Lambda function, the
    # vendor directory is gone. During `chalice local` or in a running
    # LocalAppTestCase, the vendor directory still exists.
    resource_dir = vendor_dir if os.path.exists(vendor_dir) else package_root
    resource_file = os.path.join(resource_dir, 'resources', *path)
    return open(resource_file, mode='rb' if binary else 'r')


class NotInLambdaContextException(RequirementError):

    def __init__(self, package_root) -> None:
        super().__init__('The package root suggests that no lambda context is active',
                         package_root)


def str_to_bool(string: str):
    if string == 'True':
        return True
    elif string == 'False':
        return False
    else:
        raise ValueError(string)
