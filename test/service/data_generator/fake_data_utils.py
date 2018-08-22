#!/usr/bin/python
import json
from faker import Faker
import elasticsearch5
import logging
import os

logger = logging.getLogger(__name__)


class FakerSchemaGenerator(object):
    def __init__(self, faker=None, locale=None, providers=None, includes=None):
        self._faker = faker or Faker(locale=locale, providers=providers, includes=includes)

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
                if (isinstance(v[0], dict)):
                    data[k] = [self._generate_one_fake(item) for item in v]
                else:
                    data[k] = [getattr(self._faker, a)() for a in v]
            else:
                data[k] = getattr(self._faker, v)()
        return data


class ElasticsearchFakeDataLoader(object):
    test_index_name = 'browser_files_dev'

    def __init__(self, number_of_documents=1000, azul_es_endpoint=None):

        service_tests_folder = os.path.dirname(os.path.realpath(__file__))
        fake_data_template_file = open(os.path.join(service_tests_folder, 'fake_data_template.json'), 'r')
        with fake_data_template_file as template_file:
            self.doc_template = json.load(template_file)

        self.azul_es_url = azul_es_endpoint or os.environ['AZUL_ES_ENDPOINT']
        self.elasticsearch_client = elasticsearch5.Elasticsearch(hosts=[self.azul_es_url], port=9200)
        self.number_of_documents = number_of_documents

    def load_data(self, will_clean_up=True):
        logger.log(logging.INFO, f"Deleting leftover data in test index "
                                 f"'{self.test_index_name}' at\n{self.azul_es_url}.")

        if will_clean_up:
            self.clean_up()

        try:
            self.elasticsearch_client.indices.delete(index=self.test_index_name)
        except elasticsearch5.exceptions.NotFoundError:
            logger.log(logging.DEBUG, f"The index {self.test_index_name} doesn't exist yet. Skipping clean up.")

        logger.log(logging.INFO, f"Creating new test index '{self.test_index_name}' at\n{self.azul_es_url}.")
        self.elasticsearch_client.indices.create(self.test_index_name)

        logger.log(logging.INFO, f"Loading data into test index '{self.test_index_name}' at\n{self.azul_es_url}.")
        faker = FakerSchemaGenerator()
        fake_data_body = ""
        for i in range(self.number_of_documents):
            fake_data_body += json.dumps({"index": {"_type": "meta", "_id": i}}) + "\n"
            fake_data_body += json.dumps(faker.generate_fake(self.doc_template)) + "\n"
        self.elasticsearch_client.bulk(fake_data_body, index=self.test_index_name, doc_type='meta', refresh='wait_for')

    def clean_up(self):
        try:
            self.elasticsearch_client.indices.delete(index=self.test_index_name)
        except elasticsearch5.exceptions.NotFoundError:
            logger.log(logging.DEBUG, f"The index {self.test_index_name} doesn't exist yet. Skipping clean up.")
