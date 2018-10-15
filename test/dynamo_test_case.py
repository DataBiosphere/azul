from unittest import mock, TestCase

import boto3
import docker
from moto import mock_sts

from azul.service.responseobjects.dynamo_data_access import DynamoDataAccessor


class DynamoTestCase(TestCase):

    _dynamo_docker_container = None

    dynamo_accessor = None

    dynamo_url = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        docker_client = docker.from_env()
        api_container_port = '8000/tcp'

        cls._dynamo_docker_container = docker_client.containers.run('amazon/dynamodb-local',
                                                                    detach=True,
                                                                    auto_remove=True,
                                                                    ports={api_container_port: ('127.0.0.1', None)},)
        container_info = docker_client.api.inspect_container(cls._dynamo_docker_container.name)
        container_ports = container_info['NetworkSettings']['Ports']
        container_port = container_ports[api_container_port][0]
        host_port = int(container_port['HostPort'])
        host_ip = container_port['HostIp']
        cls.dynamo_url = f'http://{host_ip}:{host_port}'

        with mock_sts():
            cls.dynamo_accessor = DynamoDataAccessor(cls.dynamo_url, 'us-east-1')

    @classmethod
    def tearDownClass(cls):
        cls._dynamo_docker_container.kill()
        super().tearDownClass()
