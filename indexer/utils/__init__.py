import os

from typing import Tuple

from utils.deployment import aws


# FIXME: Once indexer has a `src` directory this should be moved such that it is importable as from azul import config

# FIXME: This class collides with the plugin config classes derived from BaseIndexerConfig, they should be consolidated

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

    @property
    def indexer_name(self) -> str:
        return os.environ['AZUL_INDEXER_NAME']

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
