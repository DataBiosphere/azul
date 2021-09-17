import os
from re import (
    escape,
)
from typing import (
    List,
)
from unittest import (
    TestCase,
)
from unittest.mock import (
    PropertyMock,
    patch,
)
import warnings

import boto3.session
from botocore.credentials import (
    Credentials,
)
import botocore.session
import moto.backends
import moto.core.models

from azul import (
    CatalogName,
    config,
)
from azul.deployment import (
    aws,
)


class AzulTestCase(TestCase):
    _catch_warnings = None
    _caught_warnings: List[warnings.WarningMessage] = []

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()

        class RE(str):
            pass

        catch_warnings = warnings.catch_warnings(record=True)
        # Use tuple assignment to modify state atomically
        cls._catch_warnings, cls._caught_warnings = catch_warnings, catch_warnings.__enter__()
        permitted_warnings_ = {
            ResourceWarning: {
                RE(r'.*<ssl.SSLSocket.*>'),
                RE(r'.*<socket.socket.*>')
            },
            DeprecationWarning: {
                RE(r'Call to deprecated method .*\. \(DOS support will be removed\)'),

                'Call to deprecated method fetch_bundle_manifest',
                'ProjectContact.contact_name is deprecated',
                'File.file_format is deprecated',
                'ProjectPublication.publication_title is deprecated',
                'ProjectPublication.publication_url is deprecated',
                'CellLine.cell_line_type is deprecated',

                RE(r'.*humancellatlas\.data\.metadata\.api\.DissociationProcess'),
                RE(r'.*humancellatlas\.data\.metadata\.api\.EnrichmentProcess'),
                RE(r'.+humancellatlas\.data\.metadata\.api\.LibraryPreparationProcess'),
                RE(r'.*humancellatlas\.data\.metadata\.api\.SequencingProcess'),

                # FIXME: Upgrade tenacity
                #        https://github.com/DataBiosphere/azul/issues/2070
                '"@coroutine" decorator is deprecated since Python 3.8, use "async def" instead',
                # FIXME: https://github.com/DataBiosphere/azul/issues/2758
                'OpenJDK 64-Bit Server VM warning: Option UseConcMarkSweepGC was deprecated',

                RE(r'.*Fielddata access on the _uid field is deprecated, use _id instead'),
                RE(r'.*Accessing variable \[_aggs\]'),
                RE(r'.*Accessing variable \[_agg\]'),

                # FIXME: furl.fragmentstr raises deprecation warning
                #        https://github.com/DataBiosphere/azul/issues/2848
                'furl.fragmentstr is deprecated',

                (
                    "Using or importing the ABCs from 'collections' instead of from "
                    "'collections.abc' is deprecated since Python 3.3, and in 3.9 "
                    "it will stop working"
                )
            },
            UserWarning: {
                'https://github.com/DataBiosphere/azul/issues/2114',
            }
        }
        for warning_class, message_patterns in permitted_warnings_.items():
            for message_pattern in message_patterns:
                if not isinstance(message_pattern, RE):
                    message_pattern = escape(message_pattern)
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

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._mock_catalogs()
        cls._mock_aws_account_id()
        cls._mock_aws_credentials()
        cls._mock_aws_region()
        cls._mock_partition_prefix_length()

    @classmethod
    def tearDownClass(cls) -> None:
        cls._restore_aws_region()
        cls._restore_aws_credentials()
        cls._restore_aws_account()
        cls._restore_catalogs()
        cls._restore_partition_prefix_length()
        super().tearDownClass()

    def setUp(self) -> None:
        super().setUp()
        # Moto backends are reset to ensure no mock resources are left over if
        # a test fails to clean up after itself.
        self._reset_moto()

    def _reset_moto(self):
        # Note that we don't use moto.core.models.moto_api_backend.reset() here
        # because it resets all backends and therefore requires that all Moto
        # extras are installed. The backends listed here need to match the
        #  extras specified for the `moto` dependency in `requirements.dev.txt`.
        for name in ('s3', 'sqs', 'sns', 'dynamodb2'):
            backends = moto.backends.get_backend(name)
            for region_name, backend in backends.items():
                backend.reset()

    catalog: CatalogName = 'test'
    catalog_config = {
        catalog: config.Catalog(name=catalog,
                                atlas='hca',
                                internal=False,
                                plugins=dict(metadata=config.Catalog.Plugin(name='hca'),
                                             repository=config.Catalog.Plugin(name='dss')))
    }
    _catalog_mock = None

    @classmethod
    def _mock_catalogs(cls):
        # Reset the cached properties
        try:
            # noinspection PyPropertyAccess
            del config.catalogs
        except AttributeError:
            pass
        try:
            # noinspection PyPropertyAccess
            del config.default_catalog
        except AttributeError:
            pass
        try:
            # noinspection PyPropertyAccess
            del config.integration_test_catalogs
        except AttributeError:
            pass
        # Patch the catalog property to use a single fake test catalog.
        cls._catalog_mock = patch.object(target=type(config),
                                         attribute='catalogs',
                                         new_callable=PropertyMock,
                                         return_value=cls.catalog_config)
        cls._catalog_mock.start()
        assert cls.catalog_config[cls.catalog]
        # Ensure that derived cached properties are affected
        assert config.default_catalog == cls.catalog
        assert config.integration_test_catalogs == {}

    @classmethod
    def _restore_catalogs(cls):
        cls._catalog_mock.stop()

    _aws_account_id = None

    @classmethod
    def _mock_aws_account_id(cls):
        # Set AZUL_AWS_ACCOUNT_ID to what the Moto is using. This circumvents
        # assertion errors in azul.deployment.aws.account.
        cls._aws_account_id = os.environ['AZUL_AWS_ACCOUNT_ID']
        os.environ['AZUL_AWS_ACCOUNT_ID'] = moto.core.models.ACCOUNT_ID

    @classmethod
    def _restore_aws_account(cls):
        os.environ['AZUL_AWS_ACCOUNT_ID'] = cls._aws_account_id

    get_credentials_botocore = None
    get_credentials_boto3 = None
    _saved_boto3_default_session = None

    @classmethod
    def _mock_aws_credentials(cls):
        # Discard cached Boto3/botocore clients, resources and sessions
        aws.clear_caches()
        aws.discard_all_sessions()

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

    @classmethod
    def _restore_aws_credentials(cls):
        boto3.session.Session.get_credentials = cls.get_credentials_boto3
        botocore.session.Session.get_credentials = cls.get_credentials_botocore
        boto3.DEFAULT_SESSION = cls._saved_boto3_default_session
        # Discard cached Boto3/botocore clients, resources and sessions
        aws.clear_caches()
        aws.discard_all_sessions()

    # We almost certainly won't have access to this region
    _aws_test_region = 'us-gov-west-1'
    _aws_region_mock = None

    @classmethod
    def _mock_aws_region(cls):
        # Ensure that mock leakages fail by targeting a region we don't have access to.
        # Subclasses can override the selected region if moto rejects the default one.
        cls._aws_region_mock = patch.dict(os.environ, AWS_DEFAULT_REGION=cls._aws_test_region)
        cls._aws_region_mock.start()

    @classmethod
    def _restore_aws_region(cls):
        cls._aws_region_mock.stop()

    partition_prefix_length = 2
    _partition_mock = None

    @classmethod
    def _mock_partition_prefix_length(cls):
        cls._partition_mock = patch.object(target=type(config),
                                           attribute='partition_prefix_length',
                                           new_callable=PropertyMock,
                                           return_value=cls.partition_prefix_length)
        cls._partition_mock.start()

    @classmethod
    def _restore_partition_prefix_length(cls):
        cls._partition_mock.stop()


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
                        raise KeyboardInterrupt

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
                        raise KeyboardInterrupt

                with self.assertRaises(KeyboardInterrupt):
                    TC(events).run()
                self.assertEqual(events, expected_events)
