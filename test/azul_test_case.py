import os
from typing import List
from unittest import TestCase
from unittest.mock import (
    patch,
)
import warnings

import boto3.session
from botocore.credentials import Credentials
import botocore.session

from azul import (
    CatalogName,
)


class AzulTestCase(TestCase):
    _catch_warnings = None
    _caught_warnings: List[warnings.WarningMessage] = []

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        catch_warnings = warnings.catch_warnings(record=True)
        # Use tuple assignment to modify state atomically
        cls._catch_warnings, cls._caught_warnings = catch_warnings, catch_warnings.__enter__()
        permitted_warnings_ = {
            ResourceWarning: [
                '.*<ssl.SSLSocket.*>',
                '.*<socket.socket.*>'
            ],
            DeprecationWarning: {
                '.*Call to deprecated method fetch_bundle_manifest.*',
                'ProjectContact.contact_name is deprecated',
                'File.file_format is deprecated',
                'ProjectPublication.publication_title is deprecated',
                'ProjectPublication.publication_url is deprecated',
                'CellLine.cell_line_type is deprecated',
                '.*humancellatlas.data.metadata.api.DissociationProcess',
                '.*humancellatlas.data.metadata.api.EnrichmentProcess',
                '.+humancellatlas.data.metadata.api.LibraryPreparationProcess',
                '.*humancellatlas.data.metadata.api.SequencingProcess',
                # FIXME: Upgrade tenacity
                #        https://github.com/DataBiosphere/azul/issues/2070
                '"@coroutine" decorator is deprecated since Python 3.8, use "async def" instead'
            }
        }
        for warning_class, message_patterns in permitted_warnings_.items():
            for message_pattern in message_patterns:
                warnings.filterwarnings('ignore', message_pattern, warning_class)

    @classmethod
    def tearDownClass(cls) -> None:
        if cls._catch_warnings is not None:
            cls._catch_warnings.__exit__()
            caught_warnings = cls._caught_warnings
            # Use tuple assignment to modify state atomically
            cls._catch_warnings, cls._caught_warnings = None, []
            assert not caught_warnings, list(map(str, caught_warnings))
        super().tearDownClass()


class AlwaysTearDownTestCase(TestCase):
    """
    AlwaysTearDownTestCase makes sure that tearDown / cleanup methods are always
    run when they should be.

    This means that

    - if a KeyboardInterrupt is raised in a test, then tearDown, tearDownClass,
      and tearDownModule will all still run
    - if any exception is raised in a setUp, then tearDown, tearDownClass,
      and tearDownModule will all still run

    Caveats:

    - All tearDown methods should pass even if their corresponding setUps don't
      run at all, as in the case of a KeyboardInterrupt or other exception.
    - If an exception is raised in setUpClass or setUpModule, the corresponding
      tearDown will not be run.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setUp = self._cleanup_wrapper(self.setUp, BaseException)

    def run(self, result=None):
        test_method = getattr(self, self._testMethodName)
        wrapped_test = self._cleanup_wrapper(test_method, KeyboardInterrupt)
        setattr(self, self._testMethodName, wrapped_test)
        return super().run(result)

    def _cleanup_wrapper(self, method, exception_cls):
        def wrapped(*args, **kwargs):
            try:
                return method(*args, **kwargs)
            except exception_cls:
                self.tearDown()
                self.doCleanups()
                raise

        return wrapped


class AzulUnitTestCase(AzulTestCase):
    catalog: CatalogName = 'test'
    get_credentials_botocore = None
    get_credentials_boto3 = None
    _saved_boto3_default_session = None
    aws_account_id = None
    # We almost certainly won't have access to this region
    _aws_test_region = 'us-gov-west-1'
    _aws_region_mock = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()

        # Set AZUL_AWS_ACCOUNT_ID to what the Moto is using. This circumvents
        # assertion errors in azul.deployment.aws.account.
        cls.aws_account_id = os.environ['AZUL_AWS_ACCOUNT_ID']
        # The fake Moto account ID is defined as a constant in a Moto module
        # but we cannot import any Moto modules since doing so has a bad side
        # effect: https://github.com/spulec/moto/issues/2058.
        # FIXME: Switch to overriding MOTO_ACCOUNT_ID as part of
        #        https://github.com/DataBiosphere/azul/issues/1718
        os.environ['AZUL_AWS_ACCOUNT_ID'] = '123456789012'

        # Save and then reset the default boto3session. This overrides any
        # session customizations such as those performed by envhook.py which
        # interfere with moto patchers, rendering them ineffective.
        cls._saved_boto3_default_session = boto3.DEFAULT_SESSION
        boto3.DEFAULT_SESSION = None

        cls.get_credentials_botocore = botocore.session.Session.get_credentials
        cls.get_credentials_boto3 = boto3.session.Session.get_credentials

        # This ensures that we don't accidentally use actual cloud resources in
        # unit tests. Furthermore, `boto3` and `botocore` cache credentials
        # which can lead to credentials from an unmocked use of boto3 in one
        # test to leak into a mocked use of boto3. The latter was the reason for
        # https://github.com/DataBiosphere/azul/issues/668.

        def dummy_get_credentials(_self):
            # These must match what `moto` uses to mock the instance metadata
            # response (see InstanceMetadataResponse.metadata_response() in
            # moto.instance_metadata.responses).
            return Credentials(access_key='test-key',
                               secret_key='test-secret-key',
                               token='test-session-token')

        botocore.session.Session.get_credentials = dummy_get_credentials
        boto3.session.Session.get_credentials = dummy_get_credentials

        # Ensure that mock leakages fail by targeting a region we don't have acces to.
        # Subclasses can override the selected region if moto rejects the default one.
        cls._aws_region_mock = patch.dict(os.environ, AWS_DEFAULT_REGION=cls._aws_test_region)
        cls._aws_region_mock.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls._aws_region_mock.stop()
        boto3.session.Session.get_credentials = cls.get_credentials_boto3
        botocore.session.Session.get_credentials = cls.get_credentials_botocore
        boto3.DEFAULT_SESSION = cls._saved_boto3_default_session
        os.environ['AZUL_AWS_ACCOUNT_ID'] = cls.aws_account_id
        super().tearDownClass()


class Hidden:
    # Keep this test case out of the main namespace

    class TracingTestCase:
        """A test case which traces its calls."""

        def __init__(self, events):
            super().__init__('test')
            self.events = events

        # noinspection PyPep8Naming
        def setUp(self):
            self.events.append('setUp')

        def test(self):
            self.events.append('test')

        # noinspection PyPep8Naming
        def tearDown(self):
            self.events.append('tearDown')


class TestAlwaysTearDownTestCase(TestCase):

    def test_regular_execution_order(self):
        expected = ['setUp', 'test', 'tearDown', 'setUp', 'test', 'tearDown']
        for test_class, expected_events in [(TestCase, expected),
                                            (AlwaysTearDownTestCase, expected)]:
            with self.subTest(test_class=test_class, expected_events=expected_events):
                events = []

                class TC(Hidden.TracingTestCase, test_class):
                    pass

                TC(events).run()
                TC(events).run()
                self.assertEqual(events, expected_events)

    def test_keyboard_interrupt_in_test(self):
        expected = ['setUp', 'test']
        for test_class, expected_events in [(TestCase, expected),
                                            (AlwaysTearDownTestCase, expected + ['tearDown'])]:
            with self.subTest(test_class=test_class, expected_events=expected_events):
                events = []

                class TC(Hidden.TracingTestCase, test_class):

                    def test(self):
                        super().test()
                        raise KeyboardInterrupt()

                with self.assertRaises(KeyboardInterrupt):
                    TC(events).run()
                self.assertEqual(events, expected_events)
                self.assertEqual(events, expected_events)

    def test_exception_in_setup(self):
        expected = ['setUp']
        for test_class, expected_events in [(TestCase, expected),
                                            (AlwaysTearDownTestCase, expected + ['tearDown'])]:
            with self.subTest(test_class=test_class, expected_events=expected_events):
                events = []

                class TC(Hidden.TracingTestCase, test_class):

                    def setUp(self):
                        super().setUp()
                        raise RuntimeError('Exception in setUp')

                TC(events).run()
                self.assertEqual(events, expected_events)

    def test_keyboard_interrupt_in_setup(self):
        expected = ['setUp']
        for test_class, expected_events in [(TestCase, expected),
                                            (AlwaysTearDownTestCase, expected + ['tearDown'])]:
            with self.subTest(test_class=test_class, expected_events=expected_events):
                events = []

                class TC(Hidden.TracingTestCase, test_class):

                    def setUp(self):
                        super().setUp()
                        raise KeyboardInterrupt()

                with self.assertRaises(KeyboardInterrupt):
                    TC(events).run()
                self.assertEqual(events, expected_events)
