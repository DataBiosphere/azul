import logging
import os
import time
import unittest
from unittest.mock import patch

import docker

from azul.es import ESClientFactory

logger = logging.getLogger(__name__)


class ElasticsearchTestCase(unittest.TestCase):

    es_client = None

    _es_docker_container = None
    _old_es_endpoint = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        docker_client = docker.from_env()
        api_container_port = '9200/tcp'
        cls._es_docker_container = docker_client.containers.run("docker.elastic.co/elasticsearch/elasticsearch:5.5.3",
                                                                detach=True,
                                                                ports={api_container_port: ('127.0.0.1', None)},
                                                                environment=["xpack.security.enabled=false",
                                                                             "discovery.type=single-node"])
        container_info = docker_client.api.inspect_container(cls._es_docker_container.name)
        container_ports = container_info['NetworkSettings']['Ports']
        container_port = container_ports[api_container_port][0]
        es_host_port = int(container_port['HostPort'])
        es_host_ip = container_port['HostIp']
        es_host = f'{es_host_ip}:{es_host_port}'
        cls._old_es_endpoint = os.environ.get('AZUL_ES_ENDPOINT')
        os.environ['AZUL_ES_ENDPOINT'] = es_host
        cls.es_client = ESClientFactory.get()
        cls._wait_for_es()

    @classmethod
    def _wait_for_es(cls):
        patched_log_level = logging.WARNING if logger.getEffectiveLevel() <= logging.DEBUG else logging.ERROR
        start_time = time.time()
        with patch.object(logging.getLogger('elasticsearch'), 'level', new=patched_log_level):
            while not cls.es_client.ping():
                if time.time() - start_time > 60:
                    raise AssertionError('Docker container took more than a minute to set up')
                logger.info('Could not ping Elasticsearch. Retrying...')
                time.sleep(1)
        logger.info('Elasticsearch appears to be up.')

    @classmethod
    def tearDownClass(cls):
        if cls._old_es_endpoint is None:
            del os.environ['AZUL_ES_ENDPOINT']
        else:
            os.environ['AZUL_ES_ENDPOINT'] = cls._old_es_endpoint
        cls._es_docker_container.kill()
        cls._es_docker_container = None
        super().tearDownClass()
