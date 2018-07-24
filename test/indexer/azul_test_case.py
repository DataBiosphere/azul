import docker
import logging
import os
import time
import unittest

from typing import Mapping, Any
from unittest.mock import patch
from uuid import uuid4

from azul import config
from azul.project.hca.indexer import Indexer
from azul.project.hca.config import IndexProperties

logger = logging.getLogger(__name__)


class AzulTestCase(unittest.TestCase):
    @classmethod
    def get_es_client(cls):
        return cls.index_properties.elastic_search_client

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
        cls.container_obj = docker_client.containers.run("docker.elastic.co/elasticsearch/elasticsearch:5.5.3",
                                                         detach=True,
                                                         ports={api_container_port: ('127.0.0.1', None)},
                                                         environment=["xpack.security.enabled=false",
                                                                      "discovery.type=single-node"])
        container_info = docker_client.api.inspect_container(cls.container_obj.name)
        container_ports = container_info['NetworkSettings']['Ports']
        container_port = container_ports[api_container_port][0]
        host_port, host_ip = int(container_port['HostPort']), container_port['HostIp']

        # FIXME: https://github.com/DataBiosphere/azul/issues/134
        # deprecate use of production server in favor of local, farm-to-table data files
        cls.old_dss_endpoint = os.environ.get('AZUL_DSS_ENDPOINT')
        os.environ['AZUL_DSS_ENDPOINT'] = "https://dss.data.humancellatlas.org/v1"
        cls.index_properties = IndexProperties(dss_url=config.dss_endpoint,
                                               es_endpoint=(host_ip, host_port))

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

        cls.hca_indexer = Indexer(cls.index_properties)

    @classmethod
    def tearDownClass(cls):
        cls.container_obj.kill()
        # remove patched endpoint
        os.environ['AZUL_DSS_ENDPOINT'] = cls.old_dss_endpoint
        super().tearDownClass()
