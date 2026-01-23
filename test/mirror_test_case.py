from unittest.mock import (
    PropertyMock,
    patch,
)

from azul import (
    config,
)
from azul_test_case import (
    AzulUnitTestCase,
)
from s3_test_case import (
    S3TestCase,
)


class MirroringEnabledTestCase(AzulUnitTestCase):
    mirror_bucket = 'test-mirror-bucket'

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.addClassPatch(patch.object(type(config),
                                       'enable_mirroring',
                                       new=PropertyMock(return_value=True)))
        cls.addClassPatch(patch.object(type(config),
                                       'mirror_bucket',
                                       new=PropertyMock(return_value=cls.mirror_bucket)))


class MirrorFilesTestCase(MirroringEnabledTestCase, S3TestCase):

    def setUp(self):
        super().setUp()
        self._create_test_bucket(self.mirror_bucket)
