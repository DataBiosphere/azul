from unittest import TestCase

import docker


class DockerContainerTestCase(TestCase):

    _docker_client = docker.from_env()

    @classmethod
    def get_container_address(cls, docker_container, api_container_port):
        container_info = cls._docker_client.api.inspect_container(docker_container.name)
        container_ports = container_info['NetworkSettings']['Ports']
        container_port = container_ports[api_container_port][0]
        return f'{container_port["HostIp"]}:{int(container_port["HostPort"])}'
