import copy
import json
import os
from typing import (
    List,
)
from unittest import (
    TestCase,
    mock,
)
import uuid

from more_itertools import (
    first,
    flatten,
    one,
)

from app_test_case import (
    LocalAppTestCase,
)
from azul import (
    config,
)
from azul.indexer import (
    BundleFQID,
)
from indexer import (
    IndexerTestCase,
)


class WebServiceTestCase(IndexerTestCase, LocalAppTestCase):
    """
    Although it seems weird for the webservice to inherit the testing mechanisms for the indexer,
    we need them in order to send live indexer output to the webservice.
    """

    @classmethod
    def bundles(cls) -> List[BundleFQID]:
        return [
            BundleFQID('aaa96233-bf27-44c7-82df-b4dc15ad4d9d', '2018-11-02T113344.698028Z')
        ]

    @classmethod
    def lambda_name(cls) -> str:
        return "service"

    @classmethod
    def _setup_indices(cls):
        cls.index_service.create_indices(cls.catalog)
        for bundle in cls.bundles():
            cls._index_canned_bundle(bundle)

    @classmethod
    def _teardown_indices(cls):
        cls.index_service.delete_indices(cls.catalog)

    @classmethod
    def _get_doc(cls):
        body = {
            "query": {
                "match_all": {}
            }
        }
        return cls.es_client.search(index=config.es_index_name(catalog=cls.catalog,
                                                               entity_type='files',
                                                               aggregate=True),
                                    body=body)['hits']['hits']

    @classmethod
    def _duplicate_es_doc(cls, doc):
        """
        Duplicate the given `files` document with a new entity ID
        """
        new_doc = copy.deepcopy(doc)
        new_id = str(uuid.uuid4())
        new_doc['entity_id'] = new_id
        one(new_doc['contents']['files'])['document_id'] = new_id
        return new_doc

    @classmethod
    def _fill_index(cls, num_docs=1000):
        """
        Makes a bunch of copies of the first doc found in the files index and then bulk uploads them
        """
        existing_docs = cls._get_doc()
        template_doc = first(existing_docs)['_source']
        docs = [cls._duplicate_es_doc(template_doc) for _ in range(num_docs - len(existing_docs))]
        fake_data_body = '\n'.join(flatten(
            (json.dumps({"create": {"_type": "doc", "_id": doc['entity_id']}}),
             json.dumps(doc))
            for doc in docs))
        cls.es_client.bulk(fake_data_body,
                           index=config.es_index_name(catalog=cls.catalog,
                                                      entity_type='files',
                                                      aggregate=True),
                           doc_type='meta',
                           refresh='wait_for')


class DSSUnitTestCase(TestCase):
    """
    A mixin for test cases that depend on certain DSS-related environment
    variables.
    """

    _dss_mock = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._dss_mock = mock.patch.dict(os.environ,
                                        AZUL_DSS_ENDPOINT='https://dss.data.humancellatlas.org/v1')
        cls._dss_mock.start()

    @classmethod
    def tearDownClass(cls):
        cls._dss_mock.stop()
        super().tearDownClass()
