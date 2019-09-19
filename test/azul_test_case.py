from unittest import TestCase

import boto3.session


class AzulTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.get_credentials_orig = boto3.session.Session.get_credentials
        # This ensures that we don't accidentally use actual cloud resources in unit tests. Furthermore,
        # Boto3/botocore cache credentials which can lead to credentials from an unmocked use of boto3 in one test to
        # leak into a mocked use of boto3. The latter was the reason for #668.
        boto3.session.Session.get_credentials = lambda self: None
        assert boto3.session.Session().get_credentials() is None

    @classmethod
    def tearDownClass(cls) -> None:
        boto3.session.Session.get_credentials = cls.get_credentials_orig
        super().tearDownClass()
