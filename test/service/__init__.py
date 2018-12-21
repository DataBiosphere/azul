import os

from app_test_case import LocalAppTestCase
from es_test_case import ElasticsearchTestCase
from service.data_generator.fake_data_utils import ElasticsearchFakeDataLoader


class WebServiceTestCase(ElasticsearchTestCase, LocalAppTestCase):
    data_directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')

    @classmethod
    def lambda_name(cls) -> str:
        return "service"

    _data_loader = None
    seed = None  # seed is used to set a seed for the fake data loader so we can reproduce tests if needed
    number_of_documents = 1000

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._data_loader = ElasticsearchFakeDataLoader(cls.number_of_documents)
        cls._data_loader.load_data(seed=cls.seed)

    @classmethod
    def tearDownClass(cls):
        cls._data_loader.clean_up()
        super().tearDownClass()
