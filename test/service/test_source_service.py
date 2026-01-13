import json
import time
from typing import (
    Mapping,
)
from unittest import (
    mock,
)

from moto import (
    mock_aws,
)
from mypy_boto3_dynamodb.literals import (
    ScalarAttributeTypeType,
)

from azul import (
    NotInLambdaContextException,
)
from azul.plugins.repository.dss import (
    DSSSourceRef,
)
from azul.service.source_service import (
    Expired,
    NotFound,
    SourceService,
)
from azul_test_case import (
    AzulUnitTestCase,
)
from dynamodb_test_case import (
    DynamoDBTestCase,
)


@mock_aws
class TestSourceCache(DynamoDBTestCase):

    def _dynamodb_table_name(self) -> str:
        return SourceService.table_name

    def _dynamodb_atttributes(self) -> Mapping[str, ScalarAttributeTypeType]:
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


class TestConfiguredSources(AzulUnitTestCase):
    public_sources = {DSSSourceRef.for_dss_source('foo', '/0')}
    all_sources = {*public_sources, DSSSourceRef.for_dss_source('bar', '/1')}
    configured_sources_for_outsourcing = {
        'all': [s.to_json() for s in all_sources],
        'public': [s.to_json() for s in public_sources],
    }

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()

        class MockPlugin:

            def list_accessible_sources(self, authentication):
                assert authentication is None, authentication
                return TestConfiguredSources.public_sources

            def list_sources(self):
                return TestConfiguredSources.all_sources

        cls.addClassPatch(mock.patch.object(SourceService,
                                            'repository_plugin',
                                            return_value=MockPlugin()))

    @mock.patch('azul.service.source_service.open_resource',
                side_effect=NotInLambdaContextException(''))
    def test_outside_lambda(self, open_resource):
        self._test()
        open_resource.assert_called_once()

    @mock.patch('azul.service.source_service.open_resource',
                new_callable=mock.mock_open,
                read_data=json.dumps(configured_sources_for_outsourcing))
    def test_inside_lambda(self, open_resource):
        self._test()
        open_resource.assert_called_once()

    def _test(self):
        service = SourceService()
        self.assertSetEqual(self.all_sources, service.configured_sources)
        self.assertSetEqual(self.public_sources, service.configured_public_sources)
