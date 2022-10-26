from moto import (
    mock_iam,
)

from azul import (
    RequirementError,
)
from azul.deployment import (
    aws,
)
from azul_test_case import (
    AzulUnitTestCase,
)


class TestDeploymentAWS(AzulUnitTestCase):

    @mock_iam
    def test_qualified_bucket_name(self):
        aws.iam.create_account_alias(AccountAlias='platform-foo-dev')
        self.assertEqual('edu-ucsc-gi-platform-foo-dev-foo.us-gov-west-1',
                         aws.qualified_bucket_name('foo'))
        for invalid in ['', 'x', '1foo']:
            with self.subTest(invalid=invalid):
                with self.assertRaises(RequirementError):
                    aws.qualified_bucket_name(invalid)
