import logging
import os
import time
from unittest.mock import patch

from azul.es import ESClientFactory
from docker_container_test_case import DockerContainerTestCase

logger = logging.getLogger(__name__)


class ElasticsearchTestCase(DockerContainerTestCase):

    es_client = None

    _old_es_endpoint = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        es_host = cls.create_container('docker.elastic.co/elasticsearch/elasticsearch:5.5.3',
                                       '9200/tcp',
                                       environment=["xpack.security.enabled=false", "discovery.type=single-node"])
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
        logger.info(f'Took {time.time() - start_time:.3f}s to have ES reachable')
        logger.info('Elasticsearch appears to be up.')

    @classmethod
    def tearDownClass(cls):
        if cls._old_es_endpoint is None:
            del os.environ['AZUL_ES_ENDPOINT']
        else:
            os.environ['AZUL_ES_ENDPOINT'] = cls._old_es_endpoint
        super().tearDownClass()
