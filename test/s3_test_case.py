from typing import (
    cast,
    get_args,
)

from moto import (
    mock_aws,
)
from mypy_boto3_s3.client import (
    S3Client,
)
from mypy_boto3_s3.literals import (
    BucketLocationConstraintType,
)

from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul_test_case import (
    AzulUnitTestCase,
)


class S3TestCase(AzulUnitTestCase):

    @property
    def _s3(self) -> S3Client:
        return aws.s3

    def setUp(self) -> None:
        super().setUp()
        self.addPatch(mock_aws())

    def _create_test_bucket(self, bucket_name: str):
        assert config.region in get_args(BucketLocationConstraintType)
        location = cast(BucketLocationConstraintType, config.region)
        self._s3.create_bucket(Bucket=bucket_name,
                               CreateBucketConfiguration={'LocationConstraint': location})
