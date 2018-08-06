from shared import AzulTestCase
from service.data_generator.fake_data_utils import ElasticsearchFakeDataLoader


class WebServiceTestCase(AzulTestCase):
    data_loader = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.data_loader = ElasticsearchFakeDataLoader()
        cls.data_loader.load_data()

    @classmethod
    def tearDownClass(cls):
        cls.data_loader.clean_up()
