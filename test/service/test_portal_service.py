import json
import unittest

import boto3
from botocore.exceptions import (
    ClientError,
)
from moto import (
    mock_s3,
    mock_sts,
)

from azul import (
    cached_property,
    config,
)
from azul.logging import (
    configure_test_logging,
)
from azul.plugins import (
    RepositoryPlugin,
)
from azul.portal_service import (
    PortalService,
)
from azul.types import (
    JSONs,
)
from azul.version_service import (
    NoSuchObjectVersion,
)
from version_table_test_case import (
    VersionTableTestCase,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


@mock_s3
@mock_sts
class TestPortalService(VersionTableTestCase):
    dummy_db = [
        {
            "spam": "eggs"
        }
    ]

    @cached_property
    def plugin_db(self) -> JSONs:
        # Must be lazy so the mock catalog's repository plugin is used
        catalog = config.default_catalog
        plugin = RepositoryPlugin.load(catalog).create(catalog)
        return plugin.portal_db()

    multiplex_db = [
        {
            "integrations": [
                # this should be flattened
                {
                    "entity_ids": {
                        config.dss_deployment_stage: ["good"],
                        "other": ["bad"],
                    }
                },
                # this should be removed (entity_ids defined but missing for current stage)
                {
                    "entity_ids": {
                        config.dss_deployment_stage: [],
                        "other": ["whatever"]
                    }
                },
                # this should be present but still empty (missing entity_ids field is ignored)
                {

                }
            ]
        }
    ]

    demultiplex_db = [
        {
            "integrations": [
                {"entity_ids": ["good"]},
                {}
            ]
        }
    ]

    def setUp(self):
        super().setUp()

        self.s3_client = boto3.client('s3')
        self.s3_client.create_bucket(Bucket=config.portal_db_bucket)
        self.s3_client.put_bucket_versioning(Bucket=config.portal_db_bucket,
                                             VersioningConfiguration={
                                                 'Status': 'Enabled',
                                                 'MFADelete': 'Disabled'
                                             })
        self.portal_service = PortalService()

    def tearDown(self):
        super().tearDown()

        # To ensure that the bucket is cleared between tests, all versions
        # must be deleted. The most convenient way to do this is just to
        # disabling versioning and perform a single delete.
        self.s3_client.put_bucket_versioning(Bucket=config.portal_db_bucket,
                                             VersioningConfiguration={
                                                 'Status': 'Disabled',
                                                 'MFADelete': 'Disabled'
                                             })
        self.s3_client.delete_object(Bucket=config.portal_db_bucket,
                                     Key=config.portal_db_object_key)
        self.s3_client.delete_bucket(Bucket=config.portal_db_bucket)

    def download_db(self) -> JSONs:
        response = self.s3_client.get_object(Bucket=config.portal_db_bucket,
                                             Key=config.portal_db_object_key)
        return json.loads(response['Body'].read().decode())

    def test_demultiplex(self):
        result = self.portal_service.demultiplex(self.multiplex_db)
        self.assertNotEqual(result, self.multiplex_db)
        self.assertEqual(result, self.demultiplex_db)

    def test_internal_crud(self):
        self.assertRaises(ClientError, self.download_db)

        # These tests all ignore the issue of eventual consistency, which may be
        # a non-issue when mocking.

        with self.subTest('create'):
            create_db, version = self.portal_service._create_db()
            download_db = self.download_db()  # Grabs latest version
            self.assertEqual(create_db, download_db)
            self.assertEqual(create_db, self.portal_service.demultiplex(self.plugin_db))

        with self.subTest('read'):
            read_db = self.portal_service._read_db(version)
            self.assertEqual(read_db, download_db)
            self.assertRaises(NoSuchObjectVersion, self.portal_service._read_db, 'fake_version')

        with self.subTest('update'):
            version = self.portal_service._write_db(self.dummy_db, version)
            read_db = self.portal_service._read_db(version)
            download_db = self.download_db()
            self.assertEqual(read_db, download_db)
            self.assertEqual(read_db, self.dummy_db)

        with self.subTest('delete'):
            self.portal_service._delete_db(version)
            self.assertRaises(NoSuchObjectVersion, self.portal_service._read_db, version)

    def test_crud(self):
        # DB not initially present in mock S3
        self.assertRaises(ClientError, self.download_db)

        def test(callback, expected):
            self.portal_service._crud(callback)
            self.portal_service._crud(lambda db: self.assertEqual(db, expected))
            self.portal_service._crud(lambda db: self.assertEqual(db, self.download_db()))

        # It would be cool if we could force version conflicts but I'm not sure how
        test_cases = [
            ('create', (lambda db: None), self.portal_service.demultiplex(self.plugin_db)),
            ('update', (lambda db: self.dummy_db), self.dummy_db),
            ('read', (lambda db: None), self.dummy_db)
        ]

        # Note that bucket is not re-emptied between sub-tests
        for op, callback, expected in test_cases:
            with self.subTest(operation=op):
                test(callback, expected)


if __name__ == '__main__':
    unittest.main()
