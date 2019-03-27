from unittest import mock

import boto3
from mock import PropertyMock
from moto import mock_sts

from azul.deployment import aws
from azul.service.responseobjects.dynamo_data_access import DynamoDataAccessor
from docker_container_test_case import DockerContainerTestCase


class DynamoTestCase(DockerContainerTestCase):
    dynamo_accessor: DynamoDataAccessor = None
    _aws_dynamodb_resource = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        host, port = cls._create_container('amazon/dynamodb-local', container_port=8000)
        try:
            with mock_sts():
                dynamodb_resource = boto3.resource('dynamodb',
                                                   endpoint_url=f'http://{host}:{port}',
                                                   region_name='us-east-1')
                cls._aws_dynamodb_resource = mock.patch.object(target=aws.__class__,
                                                               attribute='dynamodb_resource',
                                                               new_callable=PropertyMock,
                                                               return_value=dynamodb_resource)
                cls._aws_dynamodb_resource.__enter__()
                try:
                    cls.dynamo_accessor = DynamoDataAccessor()
                except:
                    cls._aws_dynamodb_resource.__exit__()
        except:  # no coverage
            cls._kill_containers()
            raise

    @classmethod
    def tearDownClass(cls):
        cls._aws_dynamodb_resource.__exit__()
        super().tearDownClass()
