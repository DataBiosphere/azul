from unittest import TestCase

import docker


class DockerContainerTestCase(TestCase):

    _docker_client = docker.from_env()

    _containers = []

    @classmethod
    def create_container(cls, image, api_container_port, **kwargs):
        container = cls._docker_client.containers.run(
            image,
            detach=True,
            auto_remove=True,  # Automatically remove an ES container upon stop/kill
            ports={api_container_port: ('127.0.0.1', None)},
            **kwargs)

        cls._containers.append(container)

        container_info = cls._docker_client.api.inspect_container(container.name)
        container_ports = container_info['NetworkSettings']['Ports']
        container_port = container_ports[api_container_port][0]

        return f'{container_port["HostIp"]}:{int(container_port["HostPort"])}'

    @classmethod
    def tearDownClass(cls):
        for container in cls._containers:
            container.kill()
        cls._containers.clear()
        super().tearDownClass()
