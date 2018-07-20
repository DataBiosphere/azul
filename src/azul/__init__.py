import functools
import importlib
import os
import time

from typing import Tuple

from hca import HCAConfig
from hca.dss import DSSClient
from urllib3 import Timeout

from azul.deployment import aws


class Config:
    """
    See `environment` for documentation of these settings.
    """
    # FIXME: This collides with the plugin config classes derived from BaseIndexerConfig, they should be consolidated
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
    def es_domain(self):
        return os.environ['AZUL_ES_DOMAIN']

    @property
    def dss_endpoint(self) -> str:
        return os.environ['AZUL_DSS_ENDPOINT']

    @property
    def num_workers(self) -> int:
        return int(os.environ['AZUL_INDEX_WORKERS'])

    @property
    def num_dss_workers(self) -> int:
        return int(os.environ['AZUL_DSS_WORKERS'])

    def resource_name(self, lambda_name):
        prefix = os.environ['AZUL_RESOURCE_PREFIX']
        return f"{prefix}{lambda_name}-{self.deployment_stage}"

    def subdomain(self, lambda_name):
        return os.environ['AZUL_SUBDOMAIN_TEMPLATE'].format(lambda_name=lambda_name)

    def api_lambda_domain(self, lambda_name):
        return config.subdomain(lambda_name) + "." + config.domain_name

    @property
    def indexer_name(self) -> str:
        return self.resource_name('indexer')

    @property
    def service_name(self) -> str:
        return self.resource_name('service')

    @property
    def deployment_stage(self) -> str:
        return os.environ['AZUL_DEPLOYMENT_STAGE']

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
    def es_index(self) -> str:
        return os.environ['AZUL_ES_INDEX']

    @property
    def domain_name(self) -> str:
        return os.environ['AZUL_DOMAIN_NAME']

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
        # Work around https://github.com/HumanCellAtlas/dcp-cli/issues/142
        hca_config = HCAConfig("hca")
        hca_config['DSSClient'].swagger_url = (dss_endpoint or self.dss_endpoint) + '/swagger.json'
        client = DSSClient(config=hca_config)
        client.timeout_policy = Timeout(connect=10, read=40)
        return client


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


# Taken from:
# https://github.com/HumanCellAtlas/data-store/blob/90ffc8fccd2591dc21dab48ccfbba6e9ac29a063/tests/__init__.py
def eventually(timeout: float, interval: float, errors: set={AssertionError}):
    """
    @eventually runs a test until all assertions are satisfied or a timeout is reached.
    :param timeout: time until the test fails
    :param interval: time between attempts of the test
    :param errors: the exceptions to catch and retry on
    :return: the result of the function or a raised assertion error
    """
    def decorate(func):
        @functools.wraps(func)
        def call(*args, **kwargs):
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
