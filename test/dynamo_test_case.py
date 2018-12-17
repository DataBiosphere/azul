from moto import mock_sts

from azul.service.responseobjects.dynamo_data_access import DynamoDataAccessor
from docker_container_test_case import DockerContainerTestCase


class DynamoTestCase(DockerContainerTestCase):

    _dynamo_docker_container = None

    dynamo_accessor = None

    dynamo_url = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        api_container_port = '8000/tcp'

        cls._dynamo_docker_container = cls._docker_client.containers.run(
            'amazon/dynamodb-local',
            detach=True,
            auto_remove=True,
            ports={api_container_port: ('127.0.0.1', None)})
        cls.dynamo_url = f'http://{cls.get_container_address(cls._dynamo_docker_container, api_container_port)}'

        with mock_sts():
            cls.dynamo_accessor = DynamoDataAccessor(cls.dynamo_url, 'us-east-1')

    @classmethod
    def tearDownClass(cls):
        cls._dynamo_docker_container.kill()
        super().tearDownClass()
