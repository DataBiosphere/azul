import os
import logging

from shared import AzulTestCase
from service.data_generator.fake_data_utils import ElasticsearchFakeDataLoader


class setUpModule():
    logging.basicConfig(level=logging.INFO)


class WebServiceTestCase(AzulTestCase):
    data_loader = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.data_loader = ElasticsearchFakeDataLoader(
            faker_settings_filepath=os.path.join('data_generator', 'fake_data_template.json'),
            settings_filepath=os.path.join('data_generator', 'td_settings.json'),
            mapping_filepath=os.path.join('data_generator', 'td_mapping.json'),
            number_of_documents=1000)
        cls.data_loader.load_data()

    @classmethod
    def tearDownClass(cls):
        cls.data_loader.clean_up()
