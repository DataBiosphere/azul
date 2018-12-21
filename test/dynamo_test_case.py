from moto import mock_sts

from azul.service.responseobjects.dynamo_data_access import DynamoDataAccessor
from docker_container_test_case import DockerContainerTestCase


class DynamoTestCase(DockerContainerTestCase):

    dynamo_accessor = None

    dynamo_url = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        container_host = cls.create_container('amazon/dynamodb-local', '8000/tcp')
        cls.dynamo_url = f'http://{container_host}'

        with mock_sts():
            cls.dynamo_accessor = DynamoDataAccessor(cls.dynamo_url, 'us-east-1')
