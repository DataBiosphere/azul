import functools
import importlib
import os
import re
import time

from typing import Tuple, Mapping

from hca import HCAConfig
from hca.dss import DSSClient
from urllib3 import Timeout

from azul.deployment import aws


# FIXME: This class collides conceptually with the plugin config classes derived from BaseIndexerConfig.

class Config:
    """
    See `environment` for documentation of these settings.
    """

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
        return os.environ['AZUL_HOME']

    @property
    def es_domain(self) -> str:
        return os.environ['AZUL_ES_DOMAIN']

    @property
    def share_es_domain(self) -> bool:
        return 0 != int(os.environ['AZUL_SHARE_ES_DOMAIN'])

    @property
    def es_timeout(self) -> int:
        return int(os.environ['AZUL_ES_TIMEOUT'])

    @property
    def dss_endpoint(self) -> str:
        return os.environ['AZUL_DSS_ENDPOINT']

    @property
    def num_dss_workers(self) -> int:
        return int(os.environ['AZUL_DSS_WORKERS'])

    @property
    def _resource_prefix(self):
        return self._term_from_env('AZUL_RESOURCE_PREFIX')

    def qualified_resource_name(self, resource_name):
        self._validate_term(resource_name)
        return f"{self._resource_prefix}-{resource_name}-{self.deployment_stage}"

    def subdomain(self, lambda_name):
        return os.environ['AZUL_SUBDOMAIN_TEMPLATE'].format(lambda_name=lambda_name)

    def api_lambda_domain(self, lambda_name):
        return config.subdomain(lambda_name) + "." + config.domain_name

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
    def terraform_backend_bucket_template(self) -> str:
        return os.environ['AZUL_TERRAFORM_BACKEND_BUCKET_TEMPLATE']

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

    def es_index_name(self, entity_type) -> str:
        self._validate_term(entity_type)
        return f"{self._index_prefix}_{entity_type}_{self.deployment_stage}"

    def entity_type_for_es_index(self, index_name) -> str:
        prefix, entity_type, deployment_stage = index_name.split('_')
        assert prefix == self._index_prefix
        assert deployment_stage == self.deployment_stage
        return entity_type

    @property
    def domain_name(self) -> str:
        return os.environ['AZUL_DOMAIN_NAME']

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
            **{k: v for k, v in os.environ.items() if k.startswith('AZUL_') and k != 'AZUL_HOME'},
            # Hard-wire the ES endpoint, so we don't need to look it up at run-time, for every request/invocation
            'AZUL_ES_ENDPOINT': f"{host}:{port}",
            'azul_git_commit': repo.head.object.hexsha,
            'azul_git_dirty': str(repo.is_dirty()),
            'XDG_CONFIG_HOME': '/tmp'  # The DSS CLI caches downloaded Swagger definitions there
        }

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

    # FIXME: type hint return value

    def plugin(self):
        from azul.base_config import BaseIndexProperties
        from azul.indexer import BaseIndexer
        plugin_name = 'azul.project.' + os.environ.get('AZUL_PROJECT', 'hca')
        plugin = importlib.import_module(plugin_name)
        assert issubclass(plugin.Indexer, BaseIndexer)
        assert issubclass(plugin.IndexProperties, BaseIndexProperties)
        return plugin

    @property
    def subscribe_to_dss(self):
        return 0 != int(os.environ['AZUL_SUBSCRIBE_TO_DSS'])

    def dss_client(self, dss_endpoint: str = None) -> DSSClient:
        swagger_url = (dss_endpoint or self.dss_endpoint) + '/swagger.json'
        client = DSSClient(swagger_url=swagger_url)
        client.timeout_policy = Timeout(connect=10, read=40)
        return client

    @property
    def indexer_concurrency(self):
        return int(os.environ['AZUL_INDEXER_CONCURRENCY'])


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
