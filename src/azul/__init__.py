from collections import (
    ChainMap,
)
from collections.abc import (
    Mapping,
    Sequence,
    Set,
)
from enum import (
    Enum,
)
import functools
from itertools import (
    chain,
)
import logging
import os
from pathlib import (
    Path,
)
import re
import shlex
from typing import (
    BinaryIO,
    ClassVar,
    NotRequired,
    Optional,
    TYPE_CHECKING,
    TextIO,
    TypeVar,
    TypedDict,
    Union,
)

import attr
from furl import (
    furl,
)
from more_itertools import (
    first,
)

import azul.caching
from azul.caching import (
    lru_cache_per_thread,
)
from azul.collections import (
    atuple,
)
from azul.types import (
    JSON,
)
from azul.vendored.frozendict import (
    frozendict,
)

log = logging.getLogger(__name__)

Netloc = tuple[str, int]

CatalogName = str

cached_property = property if TYPE_CHECKING else azul.caching.CachedProperty

lru_cache = functools.lru_cache

if TYPE_CHECKING:
    def cache(f, /):
        return f
else:
    cache = functools.cache

if TYPE_CHECKING:
    def cache_per_thread(f, /):
        return f
else:
    def cache_per_thread(f, /):
        return lru_cache_per_thread(maxsize=None)(f)

#: A type alias for annotating the return value of methods that return a
#: ``furl`` instance that can be modified without side effects in the object
#: whose method returned it.
#
mutable_furl = furl


class Config:
    """
    See `environment` for documentation of these settings.
    """

    @property
    def environ(self):
        return ChainMap(os.environ, self._outsourced_environ)

    @property
    def billing(self):
        return self.environ['AZUL_BILLING']

    @property
    def owner(self):
        return self.environ['AZUL_OWNER']

    @property
    def aws_support_roles(self) -> list[str]:
        # FIXME: Eliminate local import
        #        https://github.com/DataBiosphere/azul/issues/3133
        import json
        variable = 'azul_aws_support_roles'
        roles = json.loads(self.environ[variable])
        require(isinstance(roles, list),
                f'{variable} must be a list', roles)
        require(all(isinstance(role, str) for role in roles),
                f'{variable} must contain only strings', roles)
        return roles

    def _boolean(self, value: str) -> bool:
        if value == '0':
            return False
        elif value == '1':
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
        require(debug in (0, 1, 2), 'AZUL_DEBUG must be either 0, 1 or 2')

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
                        *,
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
    def chalice_bin(self) -> str:
        return self.environ['azul_chalice_bin']

    @property
    def es_domain(self) -> str:
        return self.environ['AZUL_ES_DOMAIN']

    @property
    def share_es_domain(self) -> bool:
        return self._boolean(self.environ['AZUL_SHARE_ES_DOMAIN'])

    def qualified_bucket_name(self,
                              *,
                              account_name: str,
                              region_name: str,
                              bucket_name: str,
                              deployment_name: str | None = None
                              ) -> str:
        # Allow wildcard for use in ARN patterns
        if bucket_name != '*':
            self._validate_term(bucket_name, name='bucket_name')
        components = ['edu', 'ucsc', 'gi', account_name, bucket_name]
        if deployment_name is not None:
            self.validate_deployment_name(deployment_name)
            components.append(deployment_name)
        return '-'.join(components) + '.' + region_name

    aws_config_term = 'awsconfig'

    logs_term = 'logs'

    shared_term = 'shared'

    storage_term = 'storage'

    current = object()

    def alb_access_log_path_prefix(self,
                                   *component: str,
                                   deployment: Optional[str] = current,
                                   ) -> str:
        """
        :param deployment: Which deployment name to use in the path. Omit this
                           parameter to use the current deployment. Pass `None`
                           to omit the deployment name from the path.

        :param component: Other component names to append at the end of the path
        """
        return self._log_path_prefix(['alb', 'access'], deployment, *component)

    def s3_access_log_path_prefix(self,
                                  *component: str,
                                  deployment: Optional[str] = current,
                                  ) -> str:
        """
        :param deployment: Which deployment name to use in the path. Omit this
                           parameter to use the current deployment. Pass `None`
                           to omit the deployment name from the path.

        :param component: Other component names to append at the end of the path
        """
        return self._log_path_prefix(['s3', 'access'], deployment, *component)

    def _log_path_prefix(self,
                         prefix: list[str],
                         deployment: Optional[str],
                         *component: str,
                         ):
        if deployment is self.current:
            deployment = self.deployment_stage
        return '/'.join([*prefix, *atuple(deployment), *component])

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

    manifest_kms_key_tf_name = 'manifest'

    @property
    def manifest_kms_alias(self) -> str:
        """
        The name of the KMS key that is used to sign manifest keys.
        """
        # KMS requires that aliases start with '/alias'
        return 'alias/' + self.qualified_resource_name(self.manifest_kms_key_tf_name)

    audit_log_retention_days = 180  # FedRAMP mandates 90 days

    @property
    def es_timeout(self) -> int:
        return int(self.environ['AZUL_ES_TIMEOUT'])

    @property
    def data_browser_domain(self):
        domain = self.domain_name
        # FIXME: Remove 'azul.' prefix from AZUL_DOMAIN_NAME in prod
        #        https://github.com/DataBiosphere/azul/issues/5122
        if self.deployment_stage == 'prod':
            domain = domain.removeprefix('azul.')
        return domain

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

    def sources(self, catalog: CatalogName) -> Set[str]:
        return self.catalogs[catalog].sources

    @property
    def tdr_allowed_source_locations(self) -> Set[str]:
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
    def tdr_service_url(self) -> mutable_furl:
        return furl(self.environ['AZUL_TDR_SERVICE_URL'])

    @property
    def sam_service_url(self) -> mutable_furl:
        return furl(self.environ['AZUL_SAM_SERVICE_URL'])

    @property
    def duos_service_url(self) -> Optional[mutable_furl]:
        url = self.environ.get('AZUL_DUOS_SERVICE_URL')
        return None if url is None else furl(url)

    @property
    def terra_service_url(self) -> mutable_furl:
        return furl(self.environ['AZUL_TERRA_SERVICE_URL'])

    @property
    def dss_query_prefix(self) -> str:
        return self.environ.get('AZUL_DSS_QUERY_PREFIX', '')

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

    def dss_direct_access_role(self,
                               lambda_name: str,
                               stage: Optional[str] = None
                               ) -> Optional[str]:
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
    def external_lambda_role_assumptors(self) -> dict[str, list[str]]:
        try:
            accounts = self.environ['AZUL_EXTERNAL_LAMBDA_ROLE_ASSUMPTORS']
        except KeyError:
            return {}
        else:
            return self._parse_principals(accounts)

    def _parse_principals(self, accounts) -> dict[str, list[str]]:
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
        return f'{self.resource_prefix}-{resource_name}-{stage}{suffix}'

    # FIXME: Eliminate hard-coded separator
    #        https://github.com/databiosphere/azul/issues/2964
    resource_name_separator = '-'

    def unqualified_resource_name(self,
                                  qualified_name: str,
                                  suffix: str = ''
                                  ) -> tuple[str, str]:
        """
        Extract the unqualified resource name, deployment name and suffix from
        given qualified resource name, assuming that the qualified resource name
        has no suffix (the default) or the given suffix.

        >>> f = config.unqualified_resource_name
        >>> f('azul-foo-dev')
        ('foo', 'dev')

        >>> f('foo-bar-dev')
        Traceback (most recent call last):
            ...
        azul.RequirementError: ("Expected prefix 'azul'", 'foo', 'foo-bar-dev')

        >>> f('azul-foo')  # doctest: +NORMALIZE_WHITESPACE
        Traceback (most recent call last):
            ...
        azul.RequirementError: \
            ('Expected 3 name components', \
            ['azul', 'foo'], \
            'azul-foo')

        >>> f('azul-object_versions-dev')
        ('object_versions', 'dev')

        >>> f('azul-object-versions-dev')  # doctest: +NORMALIZE_WHITESPACE
        Traceback (most recent call last):
            ...
        azul.RequirementError:
            ('Expected 3 name components', \
            ['azul', 'object', 'versions', 'dev'], \
            'azul-object-versions-dev')

        >>> f('azul-tallies_retry-dev0.fifo')  # doctest: +NORMALIZE_WHITESPACE
        Traceback (most recent call last):
            ...
        azul.RequirementError: \
            ('Invalid deployment name', \
            'dev0.fifo', \
            'azul-tallies_retry-dev0.fifo')

        >>> f('azul-tallies_retry-dev0.fifo', suffix='.fifo')
        ('tallies_retry', 'dev0')

        >>> f('azul-tallies_retry-dev0', suffix='.fifo')
        Traceback (most recent call last):
            ...
        azul.RequirementError: ("Expected suffix '.fifo'", 'azul-tallies_retry-dev0')
        """
        # We could implement this using unqualified_resource_name_and_suffix
        # and that would be equivalent semantically but the error messages would
        # be less obvious.
        require(qualified_name.endswith(suffix),
                f'Expected suffix {suffix!r}', qualified_name)
        if suffix:
            qualified_name = qualified_name[:-len(suffix)]
        components = qualified_name.split(self.resource_name_separator)
        num_components = 3
        require(len(components) == num_components,
                f'Expected {num_components!r} name components', components, qualified_name)
        prefix, resource_name, deployment_stage = components
        require(prefix == self.resource_prefix,
                f'Expected prefix {self.resource_prefix!r}', prefix, qualified_name)
        require(self._is_valid_qualifier(deployment_stage),
                'Invalid deployment name', deployment_stage, qualified_name)
        require(self._is_valid_term(resource_name),
                'Invalid resource name', resource_name, qualified_name)
        return resource_name, deployment_stage

    def unqualified_resource_name_and_suffix(self,
                                             qualified_name: str
                                             ) -> tuple[str, str, str]:
        """
        Extract the unqualified resource name, deployment name and suffix from
        the given qualified resource name.

        >>> f = config.unqualified_resource_name_and_suffix
        >>> f('azul-foo-dev')
        ('foo', 'dev', '')

        >>> f('foo-bar-dev')  # doctest: +NORMALIZE_WHITESPACE
        Traceback (most recent call last):
            ...
        azul.RequirementError: ("Expected prefix 'azul'", 'foo', 'foo-bar-dev')

        >>> f('azul-foo')  # doctest: +NORMALIZE_WHITESPACE
        Traceback (most recent call last):
            ...
        azul.RequirementError: ('Expected 3 name components', \
            ['azul', 'foo'], \
            'azul-foo')

        >>> f('azul-object_versions-dev')
        ('object_versions', 'dev', '')

        This syntax is ambiguous. It would be better to flag this as an invalid
        resource name (`object-versions`) but that would require knowing that
        `versions` isn't a deployment name.

        >>> f('azul-object-versions-dev')
        ('object', 'versions', '-dev')

        >>> f('azul-tallies_retry-dev0.fifo')
        ('tallies_retry', 'dev0', '.fifo')

        >>> f('azul-tallies_retry-dev0')
        ('tallies_retry', 'dev0', '')

        >>> f('azul-0foo-dev')
        Traceback (most recent call last):
            ...
        azul.RequirementError: ('Invalid resource name', '0foo')

        >>> f('azul-tallies_retry-0dev.fifo')
        Traceback (most recent call last):
            ...
        azul.RequirementError: ('Invalid deployment name', '0dev.fifo')
        """
        num_components = 3
        components = qualified_name.split(self.resource_name_separator,
                                          maxsplit=num_components - 1)
        require(len(components) == num_components,
                f'Expected {num_components!r} name components', components, qualified_name)
        prefix, resource_name, deployment_stage = components
        require(prefix == self.resource_prefix,
                f'Expected prefix {self.resource_prefix!r}', prefix, qualified_name)
        require(self._is_valid_term(resource_name),
                'Invalid resource name', resource_name)
        match = self.qualifier_re.match(deployment_stage)
        reject(match is None,
               'Invalid deployment name', deployment_stage)
        index = match.end()
        deployment_stage, suffix = deployment_stage[0:index], deployment_stage[index:]
        assert self._is_valid_term(deployment_stage), qualified_name
        return resource_name, deployment_stage, suffix

    def subdomain(self, lambda_name):
        return self.environ['AZUL_SUBDOMAIN_TEMPLATE'].replace('*', lambda_name)

    def api_lambda_domain(self, lambda_name: str) -> str:
        return self.subdomain(lambda_name) + '.' + self.domain_name

    @property
    def drs_domain(self):
        return self.environ['AZUL_DRS_DOMAIN_NAME']

    def api_lambda_domain_aliases(self, lambda_name):
        """
        Additional alias domain names for the given API lambda
        """
        return [self.drs_domain] if lambda_name == 'service' and self.drs_domain else []

    def lambda_endpoint(self, lambda_name: str) -> mutable_furl:
        return furl(scheme='https', netloc=self.api_lambda_domain(lambda_name))

    @property
    def indexer_endpoint(self) -> mutable_furl:
        return self.lambda_endpoint('indexer')

    @property
    def service_endpoint(self) -> mutable_furl:
        return self.lambda_endpoint('service')

    @property
    def drs_endpoint(self) -> mutable_furl:
        if self.drs_domain:
            return furl(scheme='https', netloc=self.drs_domain)
        else:
            return self.service_endpoint

    def lambda_names(self) -> list[str]:
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

    qualifier_re = re.compile(r'[a-z][a-z0-9]{1,16}')

    @classmethod
    def validate_prefix(cls, prefix):
        require(cls._is_valid_qualifier(prefix),
                f'Prefix {prefix!r} is too short, '
                f'too long or contains invalid characters.')

    @classmethod
    def validate_deployment_name(cls, deployment_name):
        require(cls._is_valid_qualifier(deployment_name),
                f'Deployment name {deployment_name!r} is too short, '
                f'too long or contains invalid characters.')

    @classmethod
    def _is_valid_qualifier(cls, deployment_name: str) -> bool:
        return cls.qualifier_re.fullmatch(deployment_name) is not None

    @property
    def deployment_stage(self) -> str:
        """
        The name of the current deployment.
        """
        deployment_name = self.environ['AZUL_DEPLOYMENT_STAGE']
        self.validate_deployment_name(deployment_name)
        return deployment_name

    @cached_property
    def main_deployment_stage(self) -> str:
        """
        The name of the main deployment the current deployment is collocated
        with. If the current deployment is a main deployment, the return value
        is the name of the current deployment.
        """
        name = self.aws_account_name
        group, project, stage = name.split('-')
        # Some unit tests use `test`
        assert group in ('platform', 'test'), name
        prefix = '' if project == 'hca' else project
        return prefix + stage

    @property
    def deployment_incarnation(self) -> str:
        return self.environ['AZUL_DEPLOYMENT_INCARNATION']

    @property
    def region(self) -> str:
        return self.environ['AWS_DEFAULT_REGION']

    @property
    def enable_monitoring(self) -> bool:
        return self._boolean(self.environ['AZUL_ENABLE_MONITORING'])

    @property
    def disable_monitoring(self) -> bool:
        return not self.enable_monitoring

    @property
    def enable_log_forwarding(self) -> bool:
        # The main deployment in a given account is responsible for forwarding
        # logs from every deployment in that account. We expect this to be more
        # efficient than having one forwarder per deployment because logs are
        # delivered very frequently so each log forwarder Lambda will be
        # constantly active.
        return self.deployment_stage == self.main_deployment_stage

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
    def enable_replicas(self) -> bool:
        return self._boolean(self.environ['AZUL_ENABLE_REPLICAS'])

    @property
    def replica_conflict_limit(self) -> int:
        return int(self.environ['AZUL_REPLICA_CONFLICT_LIMIT'])

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
        >>> plugins = dict(metadata=dict(name='hca'), repository=dict(name='tdr_hca'))
        >>> kwargs = dict(atlas='hca', plugins=plugins, sources='')
        >>> c = Config.Catalog.from_json

        >>> c(name='dcp', spec=dict(internal=False, **kwargs)) # doctest: +NORMALIZE_WHITESPACE
        Config.Catalog(name='dcp',
                       atlas='hca',
                       internal=False,
                       plugins={'metadata': Config.Catalog.Plugin(name='hca'),
                                'repository': Config.Catalog.Plugin(name='tdr_hca')},
                       sources=set())

        >>> c(name='dcp-it', spec=dict(internal=True, **kwargs)).is_integration_test_catalog
        True

        >>> c(name='foo-bar', spec=dict(internal=False, **kwargs)).name
        'foo-bar'

        >>> c(name='foo-bar-it', spec=dict(internal=True, **kwargs)).name
        'foo-bar-it'

        >>> c(name='a' * 61 + '-it', spec=dict(internal=True, **kwargs)).is_integration_test_catalog
        True

        >>> c(name='a' * 62 + '-it', spec=dict(internal=True, **kwargs)) # doctest: +NORMALIZE_WHITESPACE
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
        sources: Set[str]

        _it_catalog_suffix: ClassVar[str] = '-it'

        _catalog_re: str = r'([a-z][a-z0-9]*(-[a-z0-9]+)*)'
        _catalog_re = r'(?=.{1,64}$)' + _catalog_re
        _it_catalog_re: str = _catalog_re + rf'(?<={re.escape(_it_catalog_suffix)})'
        _it_catalog_re: ClassVar[re.Pattern] = re.compile(_it_catalog_re)
        _catalog_re: ClassVar[re.Pattern] = re.compile(_catalog_re)

        def __attrs_post_init__(self):
            self.validate_name(self.name)
            # Import locally to avoid cyclical import
            from azul.plugins import (
                MetadataPlugin,
                Plugin,
                RepositoryPlugin,
            )
            all_types = set(p.type_name() for p in Plugin.types())
            configured_types = self.plugins.keys()
            require(all_types == configured_types,
                    'Catalog is missing or has extra plugin types',
                    self.name, all_types.symmetric_difference(configured_types))
            if self.internal:
                assert self.is_integration_test_catalog is True, self

            repository_bundle_cls, metadata_bundle_cls = (
                plugin_type.bundle_cls(self.plugins[plugin_type.type_name()].name)
                for plugin_type in [RepositoryPlugin, MetadataPlugin]
            )

            require(issubclass(repository_bundle_cls, metadata_bundle_cls),
                    'Catalog combines incompatible metadata and repository plugins',
                    self.name, repository_bundle_cls, metadata_bundle_cls)

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
        catalogs = self.environ['AZUL_CATALOGS']
        if catalogs.startswith('Qlpo'):  # bzip2 header, `BZh`, base64-encoded
            import bz2
            import base64
            catalogs = bz2.decompress(base64.b64decode(catalogs)).decode()
        catalogs = json.loads(catalogs)
        require(bool(catalogs), 'No catalogs configured')
        return {
            name: self.Catalog.from_json(name, catalog)
            for name, catalog in catalogs.items()
        }

    @property
    def default_catalog(self) -> CatalogName:
        return first(self.catalogs)

    @property
    def current_catalog(self) -> Optional[str]:
        return self.environ.get('azul_current_catalog')

    def it_catalog_for(self, catalog: CatalogName) -> Optional[CatalogName]:
        it_catalog = self.catalogs[catalog].it_catalog
        assert it_catalog in self.integration_test_catalogs, it_catalog
        return it_catalog

    def is_dss_enabled(self, catalog: Optional[str] = None) -> bool:
        return self._is_plugin_enabled('dss', catalog)

    def is_tdr_enabled(self, catalog: Optional[str] = None) -> bool:
        return self._is_plugin_enabled('tdr', catalog)

    def is_hca_enabled(self, catalog: Optional[str] = None) -> bool:
        return self._is_plugin_enabled('hca', catalog)

    def is_anvil_enabled(self, catalog: Optional[str] = None) -> bool:
        return self._is_plugin_enabled('anvil', catalog)

    def _is_plugin_enabled(self,
                           plugin_prefix: str,
                           catalog: Optional[str]
                           ) -> bool:
        def predicate(catalog):
            return any(
                plugin.name.split('_')[0] == plugin_prefix
                for plugin in catalog.plugins.values()
            )

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

    @property
    def domain_name(self) -> str:
        return self.environ['AZUL_DOMAIN_NAME']

    @property
    def private_api(self) -> bool:
        return self._boolean(self.environ['AZUL_PRIVATE_API'])

    @attr.s(frozen=True, kw_only=False, auto_attribs=True)
    class Deployment:
        name: str

        test_name = 'dummy'

        @property
        def is_shared(self) -> bool:
            """
            ``True`` if this deployment is a shared deployment, or ``False`` if
            it is a personal deployment.
            """
            return self in set(chain.from_iterable(config._shared_deployments.values()))

        #: The set of branches that are used for development and that are
        #: usually deployed to personal, lower and main deployments, but never
        #: stable ones. The set member ``None`` represents a feature branch or
        #: detached HEAD.
        #:
        unstable_branches = {'develop', None}

        @property
        def is_stable(self) -> bool:
            """
            ``True`` if this deployment must be kept functional for public use
            at all times.
            """
            if self.is_sandbox:
                return False
            else:
                branches = set(
                    branch
                    for branch, deployments in config._shared_deployments.items()
                    if self in deployments
                )
                return bool(branches) and branches.isdisjoint(self.unstable_branches)

        @property
        def is_sandbox(self) -> bool:
            """
            ``True`` if this deployment is a shared deployment primarily used
            for testing branches prior to merging.
            """
            return 'box' in self.name

        @property
        def is_personal(self) -> bool:
            """
            ``True`` if this deployment is managed by an individual developer.
            """
            return not self.is_shared

        @property
        def is_sandbox_or_personal(self) -> bool:
            """
            ``True`` if this deployment is managed by an individual developer or
            is a shared deployment primarily used for testing branches prior to
            merging.
            """
            return self.is_sandbox or self.is_personal

        @property
        def is_main(self) -> bool:
            """
            ``True`` if this deployment is a main deployment.

            Main deployments are deployed from long-lived (as opposed to
            feature) branches and serve some public-facing purpose, be that
            testing (a lower deployment) or production (a stable deployment).
            """
            return not self.is_sandbox_or_personal

        @property
        def is_lower(self) -> bool:
            """
            ``True`` if this deployment is an unstable main deployment.
            """
            return self.is_main and not self.is_stable

        @property
        def is_lower_sandbox(self) -> bool:
            """
            ``True`` if this deployment is a sandbox for a lower deployment.

            Note: This method currently only works for the current deployment,
                  i.e., the one created obtained from ``config.deployment``
            """
            require(self.name == config.deployment_stage, exception=NotImplementedError)
            return (
                self.is_sandbox
                and config.Deployment(config.main_deployment_stage).is_lower
            )

        @property
        def is_unit_test(self):
            return self.name == self.test_name

    @property
    def deployment(self) -> Deployment:
        return self.Deployment(self.deployment_stage)

    @property
    def _shared_deployments(self) -> Mapping[Optional[str], Sequence[Deployment]]:
        """
        Maps a branch name to a sequence of names of shared deployments the
        branch can be deployed to. The key of None signifies any other branch
        not mapped explicitly, or a detached head.
        """
        # FIXME: Eliminate local import
        #        https://github.com/DataBiosphere/azul/issues/3133
        import json
        deployments = json.loads(self.environ['azul_shared_deployments'])
        require(all(isinstance(v, list) and v for v in deployments.values()),
                'Invalid value for azul_shared_deployments')
        return frozendict(
            (k if k else None, tuple(self.Deployment(n) for n in v))
            for k, v in deployments.items()
        )

    def shared_deployments_for_branch(self,
                                      branch: str | None,
                                      ) -> Sequence[Deployment] | None:
        """
        The list of names of shared deployments the given branch can be deployed
        to or `None` of no such deployments exist. An argument of `None`
        indicates a detached head. If a list is returned, it will not be empty
        and the first element denotes the default deployment. The default
        deployment is the one that GitLab deploys a branch to when it builds a
        commit on that branch.
        """
        deployments = self._shared_deployments
        try:
            return deployments[branch]
        except KeyError:
            return None if branch is None else deployments.get(None)

    class BrowserSite(TypedDict):
        domain: str
        bucket: str
        tarball_path: str
        real_path: str

    @property
    def browser_sites(self
                      ) -> Mapping[str, Mapping[str, Mapping[str, BrowserSite]]]:
        import json
        return json.loads(self.environ['azul_browser_sites'])

    class GitStatus(TypedDict):
        commit: str
        dirty: bool

    @property
    def _git_status_env(self) -> dict[str, str]:
        return {'azul_git_' + k: str(v) for k, v in self.git_status.items()}

    @property
    def git_status(self) -> GitStatus:
        import git
        repo = git.Repo(self.project_root)
        return {
            'commit': repo.head.object.hexsha,
            'dirty': repo.is_dirty()
        }

    @property
    def lambda_git_status(self) -> GitStatus:
        return {
            'commit': self.environ['azul_git_commit'],
            'dirty': str_to_bool(self.environ['azul_git_dirty'])
        }

    @property
    def _aws_account_name(self) -> dict[str, str]:
        return {
            'azul_aws_account_name': self.aws_account_name
        }

    @property
    def aws_account_name(self) -> str:
        """
        When in invoked in a Lambda context, this method will retrieve the AWS
        account name from the Lambda environment, avoiding a round trip to IAM.
        """
        if self.is_in_lambda:
            return self.environ['azul_aws_account_name']
        else:
            from azul.deployment import (
                aws,
            )
            return aws.account_name

    @property
    def is_in_lambda(self) -> bool:
        return 'AWS_LAMBDA_FUNCTION_NAME' in self.environ

    @property
    def lambda_env(self) -> dict[str, str]:
        """
        A dictionary with the environment variables to be used by a deployed AWS
        Lambda function or `chalice local`. Only includes those variables that
        don't need to be outsourced.
        """
        return (
            self._lambda_env(outsource=False)
            | self._git_status_env
            | self._aws_account_name
        )

    @property
    def lambda_env_for_outsourcing(self) -> dict[str, str]:
        """
        Same as :meth:`lambda_env` but only for variables that need to be
        outsourced.
        """
        return self._lambda_env(outsource=True)

    #: A set of names of other environment variables to export to the Lambda
    #: function environment, in addition to those starting in `AZUL_`

    lambda_env_variables = frozenset([
        'BOTO_DISABLE_COMMONNAME',
        'GOOGLE_PROJECT'
    ])

    def _lambda_env(self, *, outsource: bool) -> dict[str, str]:
        return {
            k: v
            for k, v in os.environ.items()
            if (
                (
                    k.startswith('AZUL_')
                    # FIXME: Remove once we upgrade to botocore 1.28.x
                    #        https://github.com/DataBiosphere/azul/issues/4560
                    or k in self.lambda_env_variables
                )
                and (len(v) > 128) == outsource)
        }

    @cached_property
    def _outsourced_environ(self) -> dict[str, str]:
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

    service_lambda_timeout = 15 * 60

    api_gateway_timeout = 29

    # The service's health cache lambda makes an HTTP request to the service's
    # REST API, so the timeout for the health cache lambda must be greater
    # than or equal to that of the API Gateway fronting the service's REST API
    # lambda, plus some more time for the other health checks performed by the
    # service's health cache lambda. Since we apply the same timeout to the
    # indexer's health cache lambda, we blindly assume that this timeout is
    # also sufficient for the health checks performed by that lambda.
    #
    health_cache_lambda_timeout = api_gateway_timeout + 10

    # The number of seconds to extend the timeout of a Lambda fronted by
    # API Gateway so that API Gateway times out before the Lambda. We pad the
    # Lambda timeout so we get consistent behaviour. Without this padding we'd
    # have a race between the Lambda being killed and API Gateway timing out.
    #
    api_gateway_timeout_padding = 2

    @property
    def api_gateway_lambda_timeout(self) -> int:
        return self.api_gateway_timeout + self.api_gateway_timeout_padding

    # This attribute is set dynamically at runtime
    lambda_is_handling_api_gateway_request: bool = False

    # The length limit is more or less arbitrary. It was determined a few years
    # ago by looking at the resource name length limits for various types of AWS
    # resources. We've since increased the length limit from 30 to 40 and will
    # deal with any API errors resulting from situations which we generate a
    # qualified resource name that is too long for AWS.
    term_re = re.compile(r'[a-z][a-z0-9_]{1,38}[a-z0-9]')

    def _term_from_env(self, env_var_name: str, optional=False) -> str:
        value = self.environ.get(env_var_name, default='')
        if value == '' and optional:
            return value
        else:
            self._validate_term(value, name=env_var_name)
            return value

    @classmethod
    def _validate_term(cls, term: str, name: str = 'Term') -> None:
        require(cls._is_valid_term(term),
                f"{name} is either too short, too long or contains invalid characters: '{term}'")

    @classmethod
    def _is_valid_term(cls, term):
        return cls.term_re.fullmatch(term) is not None

    @classmethod
    def validate_qualifier(cls, qualifier: str) -> None:
        cls._validate_term(qualifier, name='qualifier')

    def secrets_manager_secret_name(self, *args):
        return '/'.join(['dcp', 'azul', self.deployment_stage, *args])

    def enable_gcp(self):
        return self.google_project() is not None

    def google_project(self) -> Optional[str]:
        return self.environ.get('GOOGLE_PROJECT')

    class ServiceAccount(Enum):
        indexer = ''
        public = '_public'
        unregistered = '_unregistered'

        def id(self, config: 'Config') -> str:
            return config.environ['AZUL_GOOGLE_SERVICE_ACCOUNT' + self.value.upper()]

        @property
        def secret_name(self) -> str:
            return 'google_service_account' + self.value

    manifest_sfn = 'manifest'

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
        return self._boolean(self.environ['AZUL_BIGQUERY_BATCH_MODE'])

    def notifications_queue_name(self, *, retry=False, fail=False) -> str:
        name = self.unqual_notifications_queue_name(retry=retry, fail=fail)
        return self.qualified_resource_name(name)

    def unqual_notifications_queue_name(self, *, retry=False, fail=False):
        return self._unqual_queue_name('notifications', retry, fail)

    def tallies_queue_name(self, *, retry=False, fail=False) -> str:
        name = self.unqual_tallies_queue_name(retry=retry, fail=fail)
        return self.qualified_resource_name(name, suffix='.fifo')

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
    def all_queue_names(self) -> list[str]:
        return self.work_queue_names + self.fail_queue_names

    @property
    def fail_queue_names(self) -> list[str]:
        return [
            self.tallies_queue_name(fail=True),
            self.notifications_queue_name(fail=True)
        ]

    @property
    def work_queue_names(self) -> list[str]:
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

    @property
    def terraform_keep_unused(self):
        return self._boolean(self.environ['azul_terraform_keep_unused'])

    permissions_boundary_name = 'azul-boundary'

    @property
    def github_project(self) -> str:
        return self.environ['azul_github_project']

    @property
    def github_access_token(self) -> str:
        return self.environ['azul_github_access_token']

    @property
    def gitlab_access_token(self) -> Optional[str]:
        return self.environ.get('azul_gitlab_access_token')

    def portal_db_object_key(self, catalog_source: str) -> str:
        return f'azul/{self.deployment_stage}/portals/{catalog_source}-db.json'

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
    def current_sources(self) -> list[str]:
        sources = self.environ.get('azul_current_sources', '*')
        sources = shlex.split(sources)
        require(bool(sources), 'Sources cannot be empty', sources)
        return sources

    terms_aggregation_size = 99999

    precision_threshold = 40000

    minimum_compression_size = 0

    @property
    def google_oauth2_client_id(self) -> Optional[str]:
        return self.environ.get('AZUL_GOOGLE_OAUTH2_CLIENT_ID')

    @property
    def monitoring_email(self) -> str:
        return self.environ['AZUL_MONITORING_EMAIL']

    @property
    def cloudwatch_dashboard_template(self) -> str:
        return f'{config.project_root}/terraform/cloudwatch_dashboard.template.json.py'

    class SecurityContact(TypedDict):
        name: str
        title: str
        email_address: str
        phone_number: str

    @property
    def security_contact(self) -> SecurityContact | None:
        value = self.environ.get('azul_security_contact')
        if value is None:
            return None
        else:
            # FIXME: Eliminate local import
            #        https://github.com/DataBiosphere/azul/issues/3133
            import json
            return json.loads(value)

    @attr.s(frozen=True, kw_only=True, auto_attribs=True)
    class SlackIntegration:
        workspace_id: str
        channel_id: str

    @property
    def slack_integration(self) -> Optional[SlackIntegration]:

        # FIXME: Eliminate local import
        #        https://github.com/DataBiosphere/azul/issues/3133
        import json
        slack_integration = self.environ.get('azul_slack_integration')
        if slack_integration is None:
            return None
        else:
            return self.SlackIntegration(**json.loads(slack_integration))

    manifest_column_joiner = '||'

    @property
    def docker_registry(self) -> str:
        return self.environ['azul_docker_registry']

    @property
    def terraform_version(self) -> str:
        return self.environ['azul_terraform_version']

    class ImageSpec(TypedDict):
        """
        Captures key information about a Docker image used in Azul
        """
        #: Fully qualified image reference, registry/repository/user/name:tag
        ref: str

        #: URL of a human-readable description of the image
        url: str

        #: True, if we build the image ourselves
        is_custom: NotRequired[bool]

    @property
    def docker_images(self) -> dict[str, ImageSpec]:
        import json
        return json.loads(self.environ['azul_docker_images'])

    docker_platforms = [
        'linux/arm64',
        'linux/amd64'
    ]

    @property
    def docker_image_gists_path(self) -> Path:
        return Path(config.project_root) / 'image_manifests.json'

    blocked_v4_ips_term = 'blocked_v4_ips'

    allowed_v4_ips_term = 'allowed_v4_ips'

    waf_rate_rule_name = 'RateRule'

    waf_rate_alarm_rule_name = 'RateAlarmRule'

    waf_rate_rule_period = 300  # seconds; this value is fixed by AWS

    waf_rate_rule_retry_after = 30  # seconds

    waf_rate_rule_limit = 1000

    assert 100 <= waf_rate_rule_limit <= 2_000_000_000  # mandated by AWS

    @property
    def vpc_cidr(self) -> str:
        return self.environ['azul_vpc_cidr']

    @property
    def vpn_subnet(self) -> str:
        return self.environ['azul_vpn_subnet']


config: Config = Config()  # yes, the type hint does help PyCharm


class RequirementError(RuntimeError):
    """
    Unlike assertions, unsatisfied requirements do not constitute a bug in the program.
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


absent = object()

T = TypeVar('T')
E = TypeVar('E')


def iif(condition: bool, then: T, otherwise: E = absent) -> Union[T, E]:
    """
    An alternative to ``if`` expressions, that, in certain situations, might
    be more convenient or readable, such as when the ``else`` branch
    evaluates to the zero value of a given type. Example zero values are
    ``0`` for ``int``, ``[]`` for ``list``, ``()`` for ``tuple``, ``{}`` for
    ``dict`` and ``''`` for ``str``.

    Specifically, if the ``then`` and ``else`` branches of an ``if``
    expression yield values of the same type, and the ``else`` branch yields
    the zero value of that type, the ``if`` expression can be replaced with a
    call to ``iif`` that omits the 3rd argument. If the first argument in
    those calls evaluates to ``False``, ``iif`` returns a zero value, which
    is created by calling, without arguments, the constructor for the type of
    the 2nd argument.

    >>> iif(True, 42)
    42

    >>> iif(False, 42)
    0

    >>> iif(True, 42, None)
    42

    >>> iif(False, 42, None)

    >>> iif(False, [42])
    []

    Do not use ``iif`` as a replacement for an ``if`` expression whose
    branches are expensive to evaluate. ``if`` expressions are lazy, ``iif``
    is not:

    >>> 42 if True else 42/0
    42

    >>> iif(True, 42, 42/0)
    Traceback (most recent call last):
    ...
    ZeroDivisionError: division by zero
    """
    if condition:
        return then
    else:
        if otherwise is absent:
            return type(then)()
        else:
            return otherwise
