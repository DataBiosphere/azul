import json
import unittest

import boto3
from botocore.exceptions import ClientError
from moto import (
    mock_sts,
    mock_s3,
)

from azul import config
from azul.logging import configure_test_logging
from azul.plugin import Plugin
from azul.portal_service import PortalService
from azul_test_case import AzulTestCase


def setUpModule():
    configure_test_logging()


class TestPortalService(AzulTestCase):

    def setUp(self) -> None:
        self.portal_service = PortalService()

        self.multiplex_db = [
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

        self.demultiplexed_db = [
            {
                "integrations": [
                    {"entity_ids": ["good"]},
                    {}
                ]
            }
        ]

    def test_staging_transform(self):
        result = self.portal_service.demultiplex(self.multiplex_db)
        self.assertNotEqual(result, self.multiplex_db)
        self.assertEqual(result, self.demultiplexed_db)

    @mock_s3
    @mock_sts
    def test_integration_portal_db_rollout(self):

        s3 = boto3.resource('s3')
        s3.create_bucket(Bucket=config.portal_integrations_db_bucket)
        obj = s3.Object(config.portal_integrations_db_bucket, config.portal_integrations_db_object)

        # DB not initially present in mock S3
        self.assertRaises(ClientError, obj.get)

        default_db = Plugin.load().portal_integrations_db()
        test_cases = [
            # Check that local copy is used when remote is missing
            (None, self.portal_service.demultiplex(default_db)),
            # Check that local copy is ignored when remote is present
            (self.multiplex_db, self.multiplex_db)
        ]

        # Note that bucket is not re-emptied between sub-tests
        for upload, expect in test_cases:
            with self.subTest(upload=upload, expect=expect):
                if upload is not None:
                    obj.put(Body=json.dumps(upload).encode(),
                            ContentType='application/json')
                db = self.portal_service.get_portal_integrations_db()
                self.assertEqual(db, expect)
                s3_db = json.loads(obj.get()['Body'].read().decode('UTF-8'))
                self.assertEqual(s3_db, expect)


if __name__ == '__main__':
    unittest.main()
