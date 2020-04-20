import logging
import os
import time
from unittest import mock
from unittest.mock import patch

from azul import config
from azul.es import ESClientFactory
from azul.json_freeze import (
    freeze,
    sort_frozen,
)
from docker_container_test_case import DockerContainerTestCase

logger = logging.getLogger(__name__)


class ElasticsearchTestCase(DockerContainerTestCase):
    """
    A test case that uses an Elasticsearch instance running in a container. The same Elasticsearch instance will be
    shared by all tests in the class.
    """
    es_client = None
    _env_patch = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        es_endpoint = cls._create_container('docker.elastic.co/elasticsearch/elasticsearch:6.8.0',
                                            container_port=9200,
                                            environment=['xpack.security.enabled=false',
                                                         'discovery.type=single-node',
                                                         'ES_JAVA_OPTS=-Xms512m -Xmx512m'])
        try:
            new_env = config.es_endpoint_env(es_endpoint=es_endpoint, es_instance_count=2)
            cls._env_patch = mock.patch.dict(os.environ, **new_env)
            cls._env_patch.__enter__()
            cls.es_client = ESClientFactory.get()
            cls._wait_for_es()
        except BaseException:  # no coverage
            cls._kill_containers()
            raise

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

    def assertElasticsearchResultsEqual(self, first, second):
        """
        The ordering of list items in our Elasticsearch responses typically doesn't matter.
        The comparison done by this method is insensitive to ordering differences in lists.

        For details see the doc string for sort_frozen() and freeze()
        """
        self.assertEqual(sort_frozen(freeze(first)), sort_frozen(freeze(second)))

    @classmethod
    def tearDownClass(cls):
        cls._env_patch.__exit__(None, None, None)
        super().tearDownClass()
