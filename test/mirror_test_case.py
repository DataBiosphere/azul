from unittest.mock import (
    PropertyMock,
    patch,
)

from azul import (
    config,
)
from s3_test_case import (
    S3TestCase,
)


class MirrorTestCase(S3TestCase):
    mirror_bucket = 'test-mirror-bucket'

    def setUp(self):
        super().setUp()
        self.addPatch(patch.object(type(config),
                                   'enable_mirroring',
                                   new=PropertyMock(return_value=True)))
        self.addPatch(patch.object(type(config),
                                   'mirror_bucket',
                                   new=PropertyMock(return_value=self.mirror_bucket)))
        self._create_test_bucket(self.mirror_bucket)
