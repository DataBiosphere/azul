from moto import (
    mock_sts,
)

from azul.logging import (
    configure_test_logging,
)
from azul.service.dynamo_data_access import (
    DynamoDataAccessor,
)
from docker_container_test_case import (
    DockerContainerTestCase,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class DynamoTestCase(DockerContainerTestCase):
    dynamo_accessor = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        host, port = cls._create_container('docker.io/amazon/dynamodb-local:1.11.477', container_port=8000)
        try:
            endpoint = f'http://{host}:{port}'
            with mock_sts():
                cls.dynamo_accessor = DynamoDataAccessor(endpoint)
        except BaseException:  # no coverage
            cls._kill_containers()
            raise
