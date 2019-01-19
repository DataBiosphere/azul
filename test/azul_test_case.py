import os
from unittest import TestCase

import boto3.session


class AzulTestCase(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.aws_profile = os.environ.pop('AWS_PROFILE', None)
        assert boto3.session.Session().get_credentials() is None, (
            "This test does not work while there are configured AWS credentials available. Make sure "
            "that neither AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY or AWS_SESSION_TOKEN are set. If you "
            "have credentials configured in ~/.aws/config or ~/.aws/credentials, make sure that they are "
            "configured in a separate [profile] so they are not active by default.")

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.aws_profile is not None:
            os.environ['AWS_PROFILE'] = cls.aws_profile
        super().tearDownClass()
