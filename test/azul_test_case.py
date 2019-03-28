import os
from typing import List, Optional, ContextManager
from unittest import TestCase, mock

import boto3.session


class AzulTestCase(TestCase):
    _patches: List[ContextManager]

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._patches = []
        try:
            cls._setUpClassPatches()
        except:
            cls._tearDownClassPatches()

    @classmethod
    def _setUpClassPatches(cls):
        """
        Override this method if you want to add a patch to the class setup. Call _addPatch to
        """
        # This patch ensures that we don't accidentally use actual cloud resources in unit tests. Furthermore,
        # Boto3/botocore cache credentials which can lead to credentials from an unmocked use of boto3 in one test to
        # leak into a mocked use of boto3. The latter was the reason for #668.
        cls._addClassPatch(mock.patch.object(boto3.session.Session, 'get_credentials', return_value=None))
        assert boto3.session.Session().get_credentials() is None

        # This patch ensures that unit tests use a consistent region
        cls._addClassPatch(mock.patch.dict(os.environ, AWS_DEFAULT_REGION='us-east-1'))

    @classmethod
    def tearDownClass(cls) -> None:
        cls._tearDownClassPatches()
        super().tearDownClass()

    @classmethod
    def _tearDownClassPatches(cls):
        while cls._patches:
            patch = cls._patches.pop(-1)  # consume elements in reverse order
            patch.__exit__(None, None, None)

    @classmethod
    def _addClassPatch(cls, patch):
        patch.__enter__()
        cls._patches.append(patch)
