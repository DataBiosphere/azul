import json
import time
from typing import (
    Mapping,
)
from unittest import (
    mock,
)
from unittest.mock import (
    MagicMock,
    PropertyMock,
    patch,
)

from furl import (
    furl,
)
from moto import (
    mock_aws,
)
from mypy_boto3_dynamodb.literals import (
    ScalarAttributeTypeType,
)

from app_test_case import (
    LocalAppTestCase,
)
from azul.http import (
    http_client,
)
from azul.logging import (
    configure_test_logging,
    get_test_logger,
)
from azul.resources import (
    NotInLambdaContextException,
)
from azul.service.source_service import (
    Expired,
    NotFound,
    SourceService,
)
from azul.source import (
    Source,
    SourceConfig,
)
from azul.terra import (
    TDRClient,
    TDRSourceRef,
    TDRSourceSpec,
)
from azul_test_case import (
    DCP2TestCase,
)
from dynamodb_test_case import (
    DynamoDBTestCase,
)

log = get_test_logger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(log)


@mock_aws
class TestSourceCache(DynamoDBTestCase):

    def _dynamodb_table_name(self) -> str:
        return SourceService.table_name

    def _dynamodb_attributes(self) -> Mapping[str, ScalarAttributeTypeType]:
        return {SourceService.key_attribute: 'S'}

    def _dynamodb_hash_key(self) -> str:
        return SourceService.key_attribute

    wait = 2

    @mock.patch.object(SourceService, attribute='expiration', new=wait)
    def test_source_cache(self):
        key = 'foo'
        value = [{'bar': 'baz'}]
        service = SourceService()
        with self.assertRaises(NotFound):
            service._get('nil')
        service._put(key, value)
        self.assertEqual(service._get(key), value)
        time.sleep(self.wait + 1)
        with self.assertRaises(Expired):
            service._get(key)


class TestPublicSources(DCP2TestCase):

    @classmethod
    def _patch_public_sources(cls):
        pass  # don't call super so that code under test isn't patched

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()

        class MockPlugin:

            @property
            def sources(self):
                return {source.ref.spec: source.config for source in cls._sources()}

            def list_sources(self, authentication):
                assert authentication is None, authentication
                return [cls.source.ref]

        cls.addClassPatch(mock.patch.object(SourceService,
                                            'repository_plugin',
                                            return_value=MockPlugin()))

    def test(self):
        actuals, outsourced = [], []

        def test():
            service = SourceService()
            actuals.append(service._public_sources)
            outsourced.append(service.public_sources_for_outsourcing)
            mock_open_resource.assert_called_once()

        target = SourceService.__module__ + '.open_resource'

        with mock.patch(target,
                        side_effect=NotInLambdaContextException('')
                        ) as mock_open_resource:
            test()

        with mock.patch(target,
                        new_callable=mock.mock_open,
                        read_data=json.dumps(outsourced[0])
                        ) as mock_open_resource:
            test()

        self.assertEqual(*outsourced)
        self.assertEqual([{self.catalog: [self.source]}] * 2, actuals)


class TestListSources(DCP2TestCase, LocalAppTestCase):

    @classmethod
    def app_name(cls) -> str:
        return 'service'

    snapshot_names = ['mock_snapshot_1', 'mock_snapshot_2']
    make_spec_str = 'tdr:bigquery:gcp:mock-project:{}'.format

    mock_list_snapshots_response = {
        str(i): {
            'id': str(i),
            'dataProject': 'mock-project',
            'name': name
        }
        # Include extra sources to check that the endpoint only returns results
        # for the current catalog
        for i, name in enumerate(snapshot_names + ['foo', 'bar'])
    }

    @classmethod
    def _sources(cls):
        return [
            Source(
                config=SourceConfig(mirror=True),
                ref=TDRSourceRef(id=id,
                                 spec=TDRSourceSpec.parse(cls.make_spec_str(snapshot['name'])),
                                 prefix=None)
            )
            for id, snapshot in cls.mock_list_snapshots_response.items()
            if snapshot['name'] in cls.snapshot_names
        ]

    @classmethod
    def _patch_public_sources(cls):
        cls.addClassPatch(
            patch.object(SourceService,
                         '_public_sources',
                         new_callable=PropertyMock,
                         return_value={cls.catalog: cls._sources()})
        )

    @patch.object(SourceService, '_get')
    @patch.object(TDRClient, 'list_snapshots')
    @patch.object(TDRClient, 'validate', new=MagicMock())
    def test(self, mock_list_snapshots, mock_source_cache_get):
        mock_list_snapshots.return_value = self.mock_list_snapshots_response
        client = http_client(log)
        azul_url = furl(url=self.base_url,
                        path='/repository/sources',
                        args=dict(catalog=self.catalog))

        def _test(*, authenticate: bool, cache: bool):
            with self.subTest(authenticate=authenticate, cache=cache):
                headers = {'Authorization': 'Bearer foo_token'} if authenticate else {}
                response = client.request('GET', str(azul_url), headers=headers)
                self.assertEqual(response.status, 200)
                actual = json.loads(response.data)
                expected = {
                    'sources': [
                        {
                            'sourceId': id,
                            'sourceSpec': self.make_spec_str(snapshot['name']),
                            'sourceConfig': {'mirror': True}
                        }
                        for id, snapshot in self.mock_list_snapshots_response.items()
                        if snapshot['name'] in self.snapshot_names
                    ]
                }
                self.assertEqual(expected, actual)

        mock_source_cache_get.return_value = list(self.mock_list_snapshots_response.keys())
        _test(authenticate=True, cache=True)
        _test(authenticate=False, cache=True)
        mock_source_cache_get.return_value = None
        mock_source_cache_get.side_effect = NotFound('foo_token')
        with patch('azul.terra.TDRClient.list_snapshot_ids',
                   return_value=self.mock_list_snapshots_response.keys() | {'not_indexed'}):
            _test(authenticate=True, cache=False)
            _test(authenticate=False, cache=False)
