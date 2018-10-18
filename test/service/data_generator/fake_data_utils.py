#!/usr/bin/python
from copy import deepcopy
import json
from faker import Faker
from elasticsearch.exceptions import NotFoundError
import logging
import os

from more_itertools import flatten

from azul import config
from azul.es import ESClientFactory

logger = logging.getLogger(__name__)


class FakerSchemaGenerator(object):
    def __init__(self, faker=None, locale=None, providers=None, includes=None, seed=None):
        self._faker = faker or Faker(locale=locale, providers=providers, includes=includes)
        if seed:
            self._faker.seed(seed)

    def generate_fake(self, schema, iterations=1):
        result = [self._generate_one_fake(schema) for _ in range(iterations)]
        return result[0] if len(result) == 1 else result

    def _generate_one_fake(self, schema):
        """
        Recursively traverse schema dictionary and for each "leaf node", evaluate the fake
        value
        Implementation:
        For each key-value pair:
        1) If value is not an iterable (i.e. dict or list), evaluate the fake data (base case)
        2) If value is a dictionary, recurse
        3) If value is a list, iteratively recurse over each item
        """
        data = {}
        for k, v in schema.items():
            if isinstance(v, dict):
                data[k] = self._generate_one_fake(v)
            elif isinstance(v, list):
                if isinstance(v[0], dict):
                    data[k] = [self._generate_one_fake(item) for item in v]
                else:
                    data[k] = [getattr(self._faker, a)() for a in v]
            else:
                data[k] = getattr(self._faker, v)()
        return data


class ElasticsearchFakeDataLoader(object):
    entity_types = ['files', 'specimens', 'projects']

    def __init__(self, number_of_documents=1000):
        service_tests_folder = os.path.dirname(os.path.realpath(__file__))
        fake_data_template_file = open(os.path.join(service_tests_folder, 'fake_data_template.json'), 'r')
        with fake_data_template_file as template_file:
            self.doc_template = json.load(template_file)

        self.elasticsearch_client = ESClientFactory.get()
        self.number_of_documents = number_of_documents

    def load_data(self, seed=None):
        self.clean_up()
        try:
            for entity_type in self.entity_types:
                index = config.es_index_name(entity_type, aggregate=True)
                logger.log(logging.INFO, f"Creating new test index '{index}'.")
                self.elasticsearch_client.indices.create(index)
                logger.log(logging.INFO, f"Loading data into test index '{index}'.")
                faker = FakerSchemaGenerator(seed=seed)
                documents = [self.fix_canned_document(entity_type, faker.generate_fake(self.doc_template))
                             for _ in range(self.number_of_documents)]
                fake_data_body = '\n'.join(flatten(
                    (json.dumps({"index": {"_type": "doc", "_id": document['entity_id']}}),
                     json.dumps(document))
                    for document in documents))
                self.elasticsearch_client.bulk(fake_data_body, index=index, doc_type='meta', refresh='wait_for')
        except NotFoundError:
            logger.log(logging.DEBUG, f"The index {index} doesn't exist yet.")

    @classmethod
    def fix_canned_document(cls, entity_type, doc):
        """
        This function fixes the canned document so that it satisfies the following invariants:

        1) In a response where each hit represents a file, 'hits.content.specimens.some_field` is a list because
        hits.content.specimens is the result of the indexer aggregating over more than one specimen. In a response
        where each hit represents a specimen, that same field is a single value because hits.content.specimens is a
        singleton. There are a two exceptions to that rule: if the metadata already specifies that field as a list,
        the field will be a list in either case. If the field is a numeric aggregate, the aggregation is done via
        sum() rather than set() and so the field remains an int, too.

        2) doc.entity_id == doc.contents.$entity_type[0].document_id in every document representing $entity_type
        """
        doc = deepcopy(doc)
        doc['contents'] = {
            inner_entity_type: [
                {
                    field: value if (
                        inner_entity_type == entity_type
                        or field in ('total_estimated_cells', 'size')
                        or isinstance(value, list)
                    ) else [value]
                    for field, value in inner_entity.items()
                } for inner_entity in inner_entities
            ] for inner_entity_type, inner_entities in doc['contents'].items()
        }
        for inner_entity_type, inner_entities in doc['contents'].items():
            if inner_entity_type == entity_type:
                assert len(inner_entities) == 1
                inner_entity = inner_entities[0]
                inner_entity['document_id'] = doc['entity_id']
        return doc

    def clean_up(self):
        logger.log(logging.INFO, "Deleting leftover data in test indices")
        try:
            for index in self.entity_types:
                self.elasticsearch_client.indices.delete(index=index)
        except NotFoundError:
            logger.log(logging.DEBUG, f"The index {index} doesn't exist yet.")
