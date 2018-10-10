import os

from azul import config
from app_test_case import LocalAppTestCase
from es_test_case import ElasticsearchTestCase
from service.data_generator.fake_data_utils import ElasticsearchFakeDataLoader
from s3_test_case_mixin import S3TestCaseHelper


class WebServiceTestCase(ElasticsearchTestCase, LocalAppTestCase):
    data_directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')
    _default_s3_client = None

    @classmethod
    def lambda_name(cls) -> str:
        return "service"

    _data_loader = None
    seed = None  # seed is used to set a seed for the fake data loader so we can reproduce tests if needed

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._data_loader = ElasticsearchFakeDataLoader()
        cls._data_loader.load_data(seed=cls.seed)

        # noinspection PyUnresolvedReferences, PyPackageRequirements
        from app import storage_service
        S3TestCaseHelper.start_s3_server()
        S3TestCaseHelper.s3_client().create_bucket(Bucket=config.s3_bucket)
        cls._default_s3_client = storage_service.client
        storage_service.set_client(S3TestCaseHelper.s3_client())

    @classmethod
    def tearDownClass(cls):
        # noinspection PyUnresolvedReferences, PyPackageRequirements
        from app import storage_service
        storage_service.set_client(cls._default_s3_client)
        S3TestCaseHelper.stop_s3_server()

        cls._data_loader.clean_up()
        super().tearDownClass()
