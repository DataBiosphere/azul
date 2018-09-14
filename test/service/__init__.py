from app_test_case import LocalAppTestCase
from es_test_case import ElasticsearchTestCase
from service.data_generator.fake_data_utils import ElasticsearchFakeDataLoader
import os


class WebServiceTestCase(ElasticsearchTestCase, LocalAppTestCase):
    data_directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')

    @classmethod
    def lambda_name(cls) -> str:
        return "service"

    _data_loader = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._data_loader = ElasticsearchFakeDataLoader()
        cls._data_loader.load_data()

    @classmethod
    def tearDownClass(cls):
        cls._data_loader.clean_up()
        super().tearDownClass()
