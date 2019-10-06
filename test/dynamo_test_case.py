from moto import mock_sts

from azul.service.responseobjects.dynamo_data_access import DynamoDataAccessor
from docker_container_test_case import DockerContainerTestCase


class DynamoTestCase(DockerContainerTestCase):
    dynamo_accessor = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        host, port = cls._create_container('amazon/dynamodb-local', container_port=8000)
        try:
            endpoint = f'http://{host}:{port}'
            with mock_sts():
                cls.dynamo_accessor = DynamoDataAccessor(endpoint, 'us-east-1')
        except BaseException:  # no coverage
            cls._kill_containers()
            raise
