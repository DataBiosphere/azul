from unittest import TestCase

import boto3.session
from botocore.credentials import Credentials
import botocore.session


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


class AzulTestCase(TestCase):
    get_credentials_botocore = None
    get_credentials_boto3 = None
    _saved_boto3_default_session = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()

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

        def dummy_get_credentials(self):
            # These must match what `moto` uses to mock the instance metadata
            # response (see InstanceMetadataResponse.metadata_response() in
            # moto.instance_metadata.responses).
            return Credentials(access_key='test-key',
                               secret_key='test-secret-key',
                               token='test-session-token')

        botocore.session.Session.get_credentials = dummy_get_credentials
        boto3.session.Session.get_credentials = dummy_get_credentials

    @classmethod
    def tearDownClass(cls) -> None:
        boto3.session.Session.get_credentials = cls.get_credentials_boto3
        botocore.session.Session.get_credentials = cls.get_credentials_botocore
        boto3.DEFAULT_SESSION = cls._saved_boto3_default_session
        super().tearDownClass()


class Hidden:
    # Keep this test case out of the main namespace

    class TracingTestCase:
        """A test case which traces its calls."""

        def __init__(self, events):
            super().__init__('test')
            self.events = events

        def setUp(self):
            self.events.append('setUp')

        def test(self):
            self.events.append('test')

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
