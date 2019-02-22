import functools
import os
import re
import time
from typing import List, Mapping, Tuple

from hca.dss import DSSClient
from urllib3 import Timeout

from azul.deployment import aws


class Config:
    """
    See `environment` for documentation of these settings.
    """

    def _boolean(self, value: str) -> bool:
        if value == "0":
            return False
        elif value == "1":
            return True
        else:
            raise ValueError('Expected "0" or "1"', value)

    @property
    def es_endpoint(self) -> Tuple[str, int]:
        try:
            es_endpoint = os.environ['AZUL_ES_ENDPOINT']
        except KeyError:
            return aws.es_endpoint(self.es_domain)
        else:
            host, _, port = es_endpoint.partition(':')
            return host, int(port)

    @property
    def project_root(self) -> str:
        return os.environ['azul_home']

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
        # TODO: Work around for obtaining data-browser domain.
        if self.deployment_stage not in ['dev', 'integration', 'staging', "prod"]:
            return 'dev.data.humancellatlas.org'
        else:
            return f'{self.deployment_stage}.data.humancellatlas.org'

    @property
    def data_browser_name(self):
        return f'{self._resource_prefix}-data-browser-{self.deployment_stage}'

    @property
    def dss_endpoint(self) -> str:
        return os.environ['AZUL_DSS_ENDPOINT']

    @property
    def dss_query_prefix(self) -> str:
        return os.environ.get('azul_dss_query_prefix', '')

    # Remove once https://github.com/HumanCellAtlas/data-store/issues/1837 is resolved

    @property
    def dss_deployment_stage(self):
        return self._dss_deployment_stage(self.dss_endpoint)

    def _dss_deployment_stage(self, dss_endpoint):
        """
        >>> config._dss_deployment_stage('https://dss.staging.data.humancellatlas.org/v1')
        'staging'
        >>> config._dss_deployment_stage('https://dss.data.humancellatlas.org/v1')
        'prod'
        """
        from urllib.parse import urlparse
        user, _, domain = urlparse(dss_endpoint).netloc.rpartition('@')
        domain = domain.split('.')
        require(domain[-3:] == ['data', 'humancellatlas', 'org'])
        require(domain[0] == 'dss')
        stage = domain[1:-3]
        assert len(stage) < 2
        return 'prod' if stage == [] else stage[0]

    # Remove once https://github.com/HumanCellAtlas/data-store/issues/1837 is resolved

    @property
    def dss_checkout_bucket(self):
        stage = self.dss_deployment_stage
        # For domain_part, DSS went from `humancellatlas` to `hca` in 9/2018 and started reverting back to
        # `humancellatlas` in 12/2018. As I write this, only `dev` is back on `humancellatlas`
        domain_part = 'humancellatlas' if stage == 'dev' else 'hca'
        return f'org-{domain_part}-dss-checkout-{stage}'

    @property
    def num_dss_workers(self) -> int:
        return int(os.environ['AZUL_DSS_WORKERS'])

    @property
    def _resource_prefix(self):
        return self._term_from_env('AZUL_RESOURCE_PREFIX')

    def qualified_resource_name(self, resource_name, suffix=''):
        self._validate_term(resource_name)
        return f"{self._resource_prefix}-{resource_name}-{self.deployment_stage}{suffix}"

    def unqualified_resource_name(self, qualified_resource_name: str, suffix: str = '') -> tuple:
        """
        >>> config.unqualified_resource_name('azul-foo-dev')
        ('foo', 'dev')

        >>> config.unqualified_resource_name('azul-foo')
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
        require(prefix == 'azul')
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
        return os.environ['AZUL_SUBDOMAIN_TEMPLATE'].format(lambda_name=lambda_name)

    def api_lambda_domain(self, lambda_name: str) -> str:
        return self.subdomain(lambda_name) + "." + self.domain_name

    def lambda_endpoint(self, lambda_name: str) -> str:
        return "https://" + self.api_lambda_domain(lambda_name)

    def indexer_endpoint(self) -> str:
        return self.lambda_endpoint('indexer')

    def service_endpoint(self) -> str:
        return self.lambda_endpoint('service')

    def lambda_names(self) -> List[str]:
        return ['indexer', 'service']

    def indexer_endpoint(self):
        return "https://" + config.api_lambda_domain('indexer')

    @property
    def indexer_name(self) -> str:
        return self.qualified_resource_name('indexer')

    @property
    def service_name(self) -> str:
        return self.qualified_resource_name('service')

    @property
    def deployment_stage(self) -> str:
        return self._term_from_env('AZUL_DEPLOYMENT_STAGE')

    @property
    def terraform_backend_bucket(self) -> str:
        return os.environ['AZUL_TERRAFORM_BACKEND_BUCKET']

    @property
    def enable_monitoring(self) -> bool:
        return self._boolean(os.environ['AZUL_ENABLE_MONITORING'])

    @property
    def es_instance_type(self) -> str:
        return os.environ['AZUL_ES_INSTANCE_TYPE']

    @property
    def es_instance_count(self) -> int:
        return int(os.environ['AZUL_ES_INSTANCE_COUNT'])

    @property
    def es_volume_size(self) -> int:
        return int(os.environ['AZUL_ES_VOLUME_SIZE'])

    @property
    def _index_prefix(self) -> str:
        return self._term_from_env('AZUL_INDEX_PREFIX')

    def es_index_name(self, entity_type, aggregate=False) -> str:
        self._validate_term(entity_type)
        return f"{self._index_prefix}_{entity_type}{'_aggregate' if aggregate else ''}_{self.deployment_stage}"

    def parse_es_index_name(self, index_name: str) -> Tuple[str, bool]:
        prefix, deployment_stage, entity_type, aggregate = self.parse_foreign_es_index_name(index_name)
        assert prefix == self._index_prefix
        assert deployment_stage == self.deployment_stage
        return entity_type, aggregate

    def parse_foreign_es_index_name(self, index_name) -> Tuple[str, str, str, bool]:
        index_name = index_name.split('_')
        if len(index_name) == 3:
            aggregate = False
        elif len(index_name) == 4:
            assert index_name.pop(2) == 'aggregate'
            aggregate = True
        else:
            assert False
        prefix, entity_type, deployment_stage = index_name
        return prefix, deployment_stage, entity_type, aggregate

    @property
    def domain_name(self) -> str:
        return os.environ['AZUL_DOMAIN_NAME']

    main_deployments_by_branch = {
        'develop': 'dev',
        'integration': 'integration',
        'staging': 'staging',
        'prod': 'prod'
    }

    @property
    def git_status(self):
        return {
            'commit': os.environ['azul_git_commit'],
            'dirty': str_to_bool(os.environ['azul_git_dirty'])
        }

    @property
    def lambda_env(self) -> Mapping[str, str]:
        """
        A dictionary with the enviroment variables to be used by a deployed AWS Lambda function or `chalice local`
        """
        import git
        repo = git.Repo(self.project_root)
        host, port = self.es_endpoint
        return {
            **{k: v for k, v in os.environ.items() if k.startswith('AZUL_')},
            # Hard-wire the ES endpoint, so we don't need to look it up at run-time, for every request/invocation
            'AZUL_ES_ENDPOINT': f"{host}:{port}",
            'azul_git_commit': repo.head.object.hexsha,
            'azul_git_dirty': str(repo.is_dirty()),
            'XDG_CONFIG_HOME': '/tmp'  # The DSS CLI caches downloaded Swagger definitions there
        }

    def get_lambda_arn(self, function_name, suffix):
        return f"arn:aws:lambda:{aws.region_name}:{aws.account}:function:{function_name}-{suffix}"

    lambda_timeout = 300

    term_re = re.compile("[a-z][a-z0-9]{2,29}")

    def _term_from_env(self, env_var_name: str) -> str:
        value = os.environ[env_var_name]
        self._validate_term(value, name=env_var_name)
        return value

    def _validate_term(self, term: str, name: str = 'Term'):
        require(self.term_re.fullmatch(term) is not None,
                f"{name} is either too short, too long or contains invalid characters: '{term}'")

    def google_service_account(self, lambda_name):
        return f"dcp/azul/{self.deployment_stage}/{lambda_name}/google_service_account"

    def enable_gcp(self):
        return 'GOOGLE_PROJECT' in os.environ

    @property
    def plugin_name(self) -> str:
        return 'azul.project.' + os.environ.get('AZUL_PROJECT', 'hca')

    @property
    def subscribe_to_dss(self):
        return self._boolean(os.environ['AZUL_SUBSCRIBE_TO_DSS'])

    def dss_client(self, dss_endpoint: str = None) -> DSSClient:
        swagger_url = (dss_endpoint or self.dss_endpoint) + '/swagger.json'
        client = DSSClient(swagger_url=swagger_url)
        client.timeout_policy = Timeout(connect=10, read=40)
        return client

    @property
    def indexer_concurrency(self):
        return int(os.environ['AZUL_INDEXER_CONCURRENCY'])

    @property
    def notify_queue_name(self):
        return self.qualified_resource_name('notify')

    @property
    def token_queue_name(self):
        return config.qualified_resource_name('documents')

    @property
    def document_queue_name(self):
        return config.qualified_resource_name('documents', suffix='.fifo')

    @property
    def fail_queue_name(self):
        return config.qualified_resource_name('fail')

    @property
    def fail_fifo_queue_name(self):
        return config.qualified_resource_name('fail', suffix='.fifo')

    @property
    def all_queue_names(self):
        return (self.token_queue_name, self.document_queue_name, self.fail_queue_name,
                self.fail_fifo_queue_name, self.notify_queue_name)

    @property
    def manifest_lambda_basename(self):
        return 'manifest'

    @property
    def manifest_state_machine_name(self):
        return config.qualified_resource_name('manifest')

    @property
    def test_mode(self) -> bool:
        return self._boolean(os.environ.get('TEST_MODE', '0'))

    @property
    def url_shortener_whitelist(self):
        return [r'.*humancellatlas\.org']

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

    @property
    def cart_item_write_lambda_basename(self):
        return 'cartitemwrite'

    @property
    def cart_item_state_machine_name(self):
        return self.qualified_resource_name('cartitems')

    @property
    def access_token_issuer(self):
        return "https://humancellatlas.auth0.com"

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
    def cart_export_max_batch_size(self):
        return int(os.environ['AZUL_CART_EXPORT_MAX_BATCH_SIZE'])

    @property
    def cart_export_min_access_token_ttl(self):
        return int(os.environ['AZUL_CART_EXPORT_MIN_ACCESS_TOKEN_TTL'])

    @property
    def cart_export_state_machine_name(self):
        return self.qualified_resource_name('cartexport')

    @property
    def cart_export_dss_push_lambda_basename(self):
        return 'cartexportpush'

    @property
    def access_token_issuer(self):
        return "https://humancellatlas.auth0.com"

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


config = Config()


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


# Taken from
# https://github.com/HumanCellAtlas/data-store/blob/90ffc8fccd2591dc21dab48ccfbba6e9ac29a063/tests/__init__.py

# noinspection PyUnusedLocal
# (see below)
def eventually(timeout: float, interval: float, errors: set = frozenset((AssertionError,))):
    """
    Runs a test until all assertions are satisfied or a timeout is reached.

    :param timeout: time until the test fails
    :param interval: time between attempts of the test
    :param errors: the exceptions to catch and retry on
    :return: the result of the function or a raised assertion error
    """

    def decorate(func):
        @functools.wraps(func)
        def call(*args, **kwargs):
            """
            This timeout eliminates the retry feature of eventually.
            The reason for this change is described here:
            https://github.com/DataBiosphere/azul/issues/233
            """
            timeout = 0

            timeout_time = time.time() + timeout
            error_tuple = tuple(errors)
            while True:
                try:
                    return func(*args, **kwargs)
                except error_tuple:
                    if time.time() >= timeout_time:
                        raise
                    time.sleep(interval)

        return call

    return decorate


def str_to_bool(string: str):
    if string == 'True':
        return True
    elif string == 'False':
        return False
    else:
        raise ValueError(string)
