import logging
import os
import time
import unittest
from unittest.mock import patch

import docker
from elasticsearch5 import Elasticsearch

logger = logging.getLogger(__name__)


class AzulTestCase(unittest.TestCase):
    es_docker_container = None
    es_host_port = None
    es_host_ip = None
    es_host = None
    _es_client = None

    @classmethod
    def get_es_client(cls):
        return cls._es_client

    def make_fake_notification(self, uuid: str, version: str) -> Mapping[str, Any]:
        return {
            "query": {
                "match_all": {}
            },
            "subscription_id": str(uuid4()),
            "transaction_id": str(uuid4()),
            "match": {
                "bundle_uuid": uuid,
                "bundle_version": version
            }
        }

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        docker_client = docker.from_env()
        api_container_port = '9200/tcp'
        cls.es_docker_container = docker_client.containers.run("docker.elastic.co/elasticsearch/elasticsearch:5.5.3",
                                                               detach=True,
                                                               ports={api_container_port: ('127.0.0.1', None)},
                                                               environment=["xpack.security.enabled=false",
                                                                      "discovery.type=single-node"])
        container_info = docker_client.api.inspect_container(cls.es_docker_container.name)
        container_ports = container_info['NetworkSettings']['Ports']
        container_port = container_ports[api_container_port][0]
        cls.es_host_port = int(container_port['HostPort'])
        cls.es_host_ip = container_port['HostIp']
        cls.es_host = f'{cls.es_host_ip}: {cls.es_host_port}'
        os.environ['AZUL_ES_ENDPOINT'] = cls.es_host
        cls._es_client = Elasticsearch(hosts=[cls.es_host], use_ssl=False)
        # FIXME: https://github.com/DataBiosphere/azul/issues/134
        # deprecate use of production server in favor of local, farm-to-table data files

        # try wait here for the elasticsearch container
        patched_log_level = logging.WARNING if logger.getEffectiveLevel() <= logging.DEBUG else logging.ERROR

        start_time = time.time()
        with patch.object(logging.getLogger('elasticsearch'), 'level', new=patched_log_level):
            while not cls.get_es_client().ping():
                if time.time() - start_time > 60:
                    logger.error('Docker container took more than a minute to set up')
                    raise AssertionError
                logger.info('Could not ping Elasticsearch. Retrying...')
                time.sleep(1)
        logger.info('Elasticsearch appears to be up.')


    @classmethod
    def tearDownClass(cls):
        cls.es_docker_container.kill()
        super().tearDownClass()
