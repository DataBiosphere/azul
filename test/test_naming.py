from unittest.mock import (
    patch,
)

from azul import (
    RequirementError,
)
from azul.deployment import (
    aws,
)
from azul_test_case import (
    AzulTestCase,
)


class TestNaming(AzulTestCase):

    @patch.object(type(aws), 'account_name', 'platform-foo-dev')
    @patch.object(type(aws), 'region_name', 'xx-east-1')
    def test_qualified_bucket_name(self):
        self.assertEqual('edu-ucsc-gi-platform-foo-dev-foo.xx-east-1',
                         aws.qualified_bucket_name('foo'))
        with self.assertRaises(RequirementError):
            aws.qualified_bucket_name('')
