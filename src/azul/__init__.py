import logging
import os
import re
from typing import (
    ClassVar,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Tuple,
    Union,
)

import attr
import boltons.cacheutils
from more_itertools import (
    first,
)

log = logging.getLogger(__name__)

Netloc = Tuple[str, int]

CatalogName = str

cached_property = boltons.cacheutils.cachedproperty


class Config:
    """
    See `environment` for documentation of these settings.
    """

    @property
    def owner(self):
        return os.environ['AZUL_OWNER']

    def _boolean(self, value: str) -> bool:
        if value == "0":
            return False
        elif value == "1":
            return True
        else:
            raise ValueError('Expected "0" or "1"', value)

    @property
    def debug(self) -> int:
        debug = int(os.environ['AZUL_DEBUG'])
        self._validate_debug(debug)
        return debug

    @debug.setter
    def debug(self, debug: int):
        self._validate_debug(debug)
        os.environ['AZUL_DEBUG'] = str(debug)

    def _validate_debug(self, debug):
        require(debug in (0, 1, 2), "AZUL_DEBUG must be either 0, 1 or 2")

    _es_endpoint_env_name = 'AZUL_ES_ENDPOINT'

    @property
    def es_endpoint(self) -> Optional[Netloc]:
        try:
            es_endpoint = os.environ[self._es_endpoint_env_name]
        except KeyError:
            return None
        else:
            host, _, port = es_endpoint.partition(':')
            return host, int(port)

    def es_endpoint_env(self, es_endpoint: Netloc, es_instance_count: Union[int, str]) -> Mapping[str, str]:
        host, port = es_endpoint
        return {
            self._es_endpoint_env_name: f'{host}:{port}',
            self._es_instance_count_env_name: str(es_instance_count)
        }

    @property
    def aws_account_id(self) -> str:
        return os.environ['AZUL_AWS_ACCOUNT_ID']

    @property
    def project_root(self) -> str:
        return os.environ['project_root']

    @property
    def es_domain(self) -> str:
        return os.environ['AZUL_ES_DOMAIN']

    @property
    def share_es_domain(self) -> bool:
        return self._boolean(os.environ['AZUL_SHARE_ES_DOMAIN'])

    @property
    def disable_multipart_manifests(self) -> bool:
        return self._boolean(os.environ['AZUL_DISABLE_MULTIPART_MANIFESTS'])

    @property
    def s3_bucket(self) -> str:
        return os.environ['AZUL_S3_BUCKET']

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
        return os.environ['AZUL_URL_REDIRECT_FULL_DOMAIN_NAME']

    @property
    def url_redirect_base_domain_name(self) -> str:
        return os.environ['AZUL_URL_REDIRECT_BASE_DOMAIN_NAME']

    @property
    def es_timeout(self) -> int:
        return int(os.environ['AZUL_ES_TIMEOUT'])

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
        return f'{self._resource_prefix}-data-browser-{self.deployment_stage}'

    @property
    def data_portal_name(self):
        return f'{self._resource_prefix}-data-portal-{self.deployment_stage}'

    @property
    def dss_endpoint(self) -> str:
        return os.environ['AZUL_DSS_ENDPOINT']

    def tdr_source(self, catalog: CatalogName) -> str:
        try:
            return os.environ[f'AZUL_TDR_{catalog.upper()}_SOURCE']
        except KeyError:
            return os.environ['AZUL_TDR_SOURCE']

    @property
    def tdr_service_url(self) -> str:
        return os.environ['AZUL_TDR_SERVICE_URL']

    @property
    def sam_service_url(self):
        return os.environ['AZUL_SAM_SERVICE_URL']

    @property
    def dss_query_prefix(self) -> str:
        return os.environ.get('azul_dss_query_prefix', '')

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
        return self._boolean(os.environ['AZUL_DSS_DIRECT_ACCESS'])

    def dss_direct_access_role(self, lambda_name: str, stage: Optional[str] = None) -> Optional[str]:
        key = 'AZUL_DSS_DIRECT_ACCESS_ROLE'
        try:
            role_arn = os.environ[key]
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
    def num_repo_workers(self) -> int:
        return int(os.environ['AZUL_REPO_WORKERS'])

    @property
    def external_lambda_role_assumptors(self) -> MutableMapping[str, List[str]]:
        try:
            accounts = os.environ['AZUL_EXTERNAL_LAMBDA_ROLE_ASSUMPTORS']
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
    def _resource_prefix(self):
        prefix = os.environ['AZUL_RESOURCE_PREFIX']
        self.validate_prefix(prefix)
        return prefix

    def qualified_resource_name(self, resource_name, suffix='', stage=None):
        self._validate_term(resource_name)
        if stage is None:
            stage = self.deployment_stage
        return f"{self._resource_prefix}-{resource_name}-{stage}{suffix}"

    def unqualified_resource_name(self, qualified_resource_name: str, suffix: str = '') -> tuple:
        """
        >>> config.unqualified_resource_name('azul-foo-dev')
        ('foo', 'dev')

        >>> config.unqualified_resource_name('azul-foo')
        Traceback (most recent call last):
        ...
        azul.RequirementError

        >>> config.unqualified_resource_name('azul-object_versions-dev')
        ('object_versions', 'dev')

        >>> config.unqualified_resource_name('azul-object-versions-dev')
        Traceback (most recent call last):
        ...
        azul.RequirementError

        :param qualified_resource_name:
        :param suffix:
        :return:
        """
        require(qualified_resource_name.endswith(suffix))
        if len(suffix) > 0:
            qualified_resource_name = qualified_resource_name[:-len(suffix)]
        components = qualified_resource_name.split('-')
        require(len(components) == 3)
        prefix, resource_name, deployment_stage = components
        require(prefix == self._resource_prefix)
        return resource_name, deployment_stage

    def unqualified_resource_name_or_none(self, qualified_resource_name: str, suffix: str = '') -> tuple:
        """
        >>> config.unqualified_resource_name_or_none('azul-foo-dev')
        ('foo', 'dev')

        >>> config.unqualified_resource_name_or_none('invalid-foo-dev')
        (None, None)

        :param qualified_resource_name:
        :param suffix:
        :return:
        """
        try:
            return self.unqualified_resource_name(qualified_resource_name, suffix=suffix)
        except RequirementError:
            return None, None

    def subdomain(self, lambda_name):
        return os.environ['AZUL_SUBDOMAIN_TEMPLATE'].replace('*', lambda_name)

    def api_lambda_domain(self, lambda_name: str) -> str:
        return self.subdomain(lambda_name) + "." + self.domain_name

    @property
    def drs_domain(self):
        return os.environ['AZUL_DRS_DOMAIN_NAME']

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
        return self.qualified_resource_name('indexer')

    @property
    def service_name(self) -> str:
        return self.qualified_resource_name('service')

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
        deployment_name = os.environ['AZUL_DEPLOYMENT_STAGE']
        self.validate_deployment_name(deployment_name)
        return deployment_name

    @property
    def region(self) -> str:
        return os.environ['AWS_DEFAULT_REGION']

    @property
    def terraform_backend_bucket(self) -> str:
        return self.versioned_bucket

    @property
    def versioned_bucket(self):
        return os.environ['AZUL_VERSIONED_BUCKET']

    @property
    def enable_monitoring(self) -> bool:
        return self._boolean(os.environ['AZUL_ENABLE_MONITORING'])

    @property
    def disable_monitoring(self) -> bool:
        return not self.enable_monitoring

    @property
    def es_instance_type(self) -> str:
        return os.environ['AZUL_ES_INSTANCE_TYPE']

    _es_instance_count_env_name = 'AZUL_ES_INSTANCE_COUNT'

    @property
    def es_instance_count(self) -> int:
        return int(os.environ[self._es_instance_count_env_name])

    @property
    def es_volume_size(self) -> int:
        return int(os.environ['AZUL_ES_VOLUME_SIZE'])

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

    @cached_property
    def catalogs(self) -> Mapping[CatalogName, Mapping[str, str]]:
        """
        A mapping from catalog name to a mapping from plugin type to plugin
        package name.
        """
        catalogs = os.environ['AZUL_CATALOGS']
        result = {}
        for catalog in catalogs.split(','):
            catalog_name, *plugins = catalog.split(':')
            result[catalog_name] = (catalog := {})
            for plugin in plugins:
                plugin_type, plugin_name = plugin.split('/')
                catalog[plugin_type] = plugin_name
        require(bool(result), 'No catalogs configured', catalogs)
        return result

    @cached_property
    def default_catalog(self) -> CatalogName:
        return first(self.catalogs)

    @cached_property
    def integration_test_catalogs(self) -> Mapping[CatalogName, Mapping[str, str]]:
        it_catalog_re = re.compile(r'it[\d]+')
        return {
            name: catalog
            for name, catalog in self.catalogs.items()
            if it_catalog_re.fullmatch(name)
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
        return os.environ['AZUL_DOMAIN_NAME']

    main_deployments_by_branch = {
        'develop': 'dev',
        'integration': 'integration',
        'staging': 'staging',
        'prod': 'prod'
    }

    def is_main_deployment(self, stage: str = None) -> bool:
        if stage is None:
            stage = self.deployment_stage
        return stage in self.main_deployments_by_branch.values()

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
            'commit': os.environ['azul_git_commit'],
            'dirty': str_to_bool(os.environ['azul_git_dirty'])
        }

    @property
    def lambda_env(self):
        """
        A dictionary with the environment variables to be used by a deployed AWS
        Lambda function or `chalice local`
        """
        return {
            **{k: v for k, v in os.environ.items() if k.startswith('AZUL_')},
            **self._git_status,
            'XDG_CONFIG_HOME': '/tmp'  # The DSS CLI caches downloaded Swagger definitions there
        }

    contribution_lambda_timeout = 5 * 60

    def aggregation_lambda_timeout(self, *, retry: bool) -> int:
        return (5 if retry else 1) * 60

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

    term_re = re.compile("[a-z][a-z0-9_]{1,28}[a-z0-9]")

    def _term_from_env(self, env_var_name: str, optional=False) -> str:
        value = os.environ.get(env_var_name, default='')
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
        return 'GOOGLE_PROJECT' in os.environ

    @property
    def indexer_google_service_account(self):
        try:
            return os.environ['AZUL_INDEXER_GOOGLE_SERVICE_ACCOUNT']
        except KeyError:
            return self.qualified_resource_name('indexer')

    def plugin_name(self, catalog_name: CatalogName, plugin_type: str) -> str:
        return self.catalogs[catalog_name][plugin_type]

    @property
    def subscribe_to_dss(self):
        return self._boolean(os.environ['AZUL_SUBSCRIBE_TO_DSS'])

    service_cache_health_lambda_basename = 'servicecachehealth'
    indexer_cache_health_lambda_basename = 'indexercachehealth'

    @property
    def indexer_concurrency(self):
        return int(os.environ['AZUL_INDEXER_CONCURRENCY'])

    def notifications_queue_name(self, *, fail=False) -> str:
        name = self.unqual_notifications_queue_name(fail=fail)
        return self.qualified_resource_name(name)

    def unqual_notifications_queue_name(self, *, fail=False):
        parts = ['notifications']
        if fail:
            parts.append('fail')
        return '_'.join(parts)

    def tallies_queue_name(self, *, retry=False, fail=False) -> str:
        name = self.unqual_tallies_queue_name(retry=retry, fail=fail)
        return config.qualified_resource_name(name, suffix='.fifo')

    def unqual_tallies_queue_name(self, *, retry=False, fail=False):
        parts = ['tallies']
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
            self.notifications_queue_name(),
            *(self.tallies_queue_name(retry=retry) for retry in (False, True))
        ]

    manifest_lambda_basename = 'manifest'

    @property
    def manifest_state_machine_name(self):
        return config.qualified_resource_name('manifest')

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

    @property
    def dynamo_user_table_name(self):
        return self.qualified_resource_name('users')

    @property
    def dynamo_cart_table_name(self):
        return self.qualified_resource_name('carts')

    @property
    def dynamo_cart_item_table_name(self):
        return self.qualified_resource_name('cartitems')

    cart_item_write_lambda_basename = 'cartitemwrite'

    @property
    def cart_item_state_machine_name(self):
        return self.qualified_resource_name('cartitems')

    @property
    def cart_export_max_batch_size(self):
        return int(os.environ['AZUL_CART_EXPORT_MAX_BATCH_SIZE'])

    @property
    def cart_export_min_access_token_ttl(self):
        return int(os.environ['AZUL_CART_EXPORT_MIN_ACCESS_TOKEN_TTL'])

    @property
    def cart_export_state_machine_name(self):
        return self.qualified_resource_name('cartexport')

    cart_export_dss_push_lambda_basename = 'cartexportpush'

    access_token_issuer = "https://humancellatlas.auth0.com"

    @property
    def access_token_audience_list(self):
        return [
            f"https://{self.deployment_stage}.data.humancellatlas.org/",
            f"{self.access_token_issuer}/userinfo"
        ]

    @property
    def fusillade_endpoint(self) -> str:
        return os.environ['AZUL_FUSILLADE_ENDPOINT']

    @property
    def grafana_user(self):
        return os.environ['azul_grafana_user']

    @property
    def grafana_password(self):
        return os.environ['azul_grafana_password']

    @property
    def grafana_endpoint(self):
        return os.environ['azul_grafana_endpoint']

    @property
    def terraform_component(self):
        return self._term_from_env('azul_terraform_component', optional=True)

    permissions_boundary_name = 'azul-boundary'

    @property
    def github_project(self) -> str:
        return os.environ['azul_github_project']

    @property
    def github_access_token(self) -> str:
        return os.environ['azul_github_access_token']

    @property
    def portal_db_bucket(self) -> str:
        return self.versioned_bucket

    @property
    def portal_db_object_key(self) -> str:
        return f'azul/{self.deployment_stage}/portals/{self.dss_deployment_stage(self.dss_endpoint)}-db.json'

    @property
    def lambda_layer_bucket(self) -> str:
        return self.versioned_bucket

    @property
    def lambda_layer_key(self) -> str:
        return 'azul/lambda_layer'

    @property
    def dynamo_object_version_table_name(self) -> str:
        return self.qualified_resource_name('object_versions')

    terms_aggregation_size = 99999


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

    catalog_name_re: ClassVar[re.Pattern] = re.compile(r'[a-z0-9]{1,64}')

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
        azul.RequirementError: Catalog name '_' contains invalid characters.

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
            self.validate_catalog_name(self.catalog)
        Config.validate_entity_type(self.entity_type)
        assert '_' not in self.prefix, self.prefix
        assert '_' not in self.deployment, self.deployment
        assert self.catalog is None or '_' not in self.catalog, self.catalog

    @classmethod
    def validate_catalog_name(cls, catalog, **kwargs):
        reject(cls.catalog_name_re.fullmatch(catalog) is None,
               f'Catalog name {catalog!r} contains invalid characters.',
               **kwargs)

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
    Raise a RequirementError, or an instance of the given exception class, if the given condition is False.

    :param condition: the boolean condition to be required

    :param args: optional positional arguments to be passed to the exception constructor. Typically only one such
                 argument should be provided: a string containing a textual description of the requirement.

    :param exception: a custom exception class to be instantiated and raised if the condition does not hold
    """
    reject(not condition, *args, exception=exception)


def reject(condition: bool, *args, exception: type = RequirementError):
    """
    Raise a RequirementError, or an instance of the given exception class, if the given condition is True.

    :param condition: the boolean condition to be rejected

    :param args: optional positional arguments to be passed to the exception constructor. Typically only one such
                 argument should be provided: a string containing a textual description of the rejected condition.

    :param exception: a custom exception class to be instantiated and raised if the condition occurs
    """
    if condition:
        raise exception(*args)


def str_to_bool(string: str):
    if string == 'True':
        return True
    elif string == 'False':
        return False
    else:
        raise ValueError(string)
