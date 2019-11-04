from copy import deepcopy
from typing import (
    Sequence,
)
import json
import logging

import boto3

from azul import config
from azul.plugin import Plugin
from azul.types import JSON

log = logging.getLogger(__name__)


class PortalService:

    def get_portal_integrations_db(self) -> Sequence[JSON]:
        """
        Retrieve portal DB from S3.

        If no object is present at the location specified in azul.config, then
        a default hardcoded DB is obtained from the current plugin, uploaded to
        S3, and returned.
        """

        client = boto3.client('s3')
        try:
            response = client.get_object(Bucket=config.portal_integrations_db_bucket,
                                         Key=config.portal_integrations_db_object)
        except client.exceptions.NoSuchKey:
            plugin = Plugin.load()
            db = self.demultiplex(plugin.portal_integrations_db())
            log.info('Portal integration DB not found in S3; uploading hard-coded DB.')
            json_bytes = json.dumps(db).encode()
            client.put_object(Bucket=config.portal_integrations_db_bucket,
                              Key=config.portal_integrations_db_object,
                              Body=json_bytes,
                              ContentType='application/json')
        else:
            json_bytes = response['Body'].read()
            db = json.loads(json_bytes.decode('UTF-8'))
        return db

    def demultiplex(self, db: Sequence[JSON]) -> Sequence[JSON]:
        """
        Transform portal integrations database to only contain entity_ids from
        the current DSS deployment stage, leaving the original unmodified.

        :param db: portal DB where the `entity_ids` fields  are dictionaries
        whose keys correspond to DSS deployment stages.

        :return: deep copy of that DB where the `entity_ids` fields have been
        replaced by the entry associated with the current DSS deployment stage.
        If the `entity_ids` field is present but no entity ids are specified for
        the current deployment stages, the integration is removed. Portals,
        however, are not removed even if they have no remaining associated
        integrations.
        """

        def transform_integrations(integrations):
            for integration in integrations:
                if 'entity_ids' in integration:
                    staged_entity_ids = integration['entity_ids'][config.dss_deployment_stage]
                    if staged_entity_ids:
                        yield {
                            k: deepcopy(v if k != 'entity_ids' else staged_entity_ids)
                            for k, v in integration.items()
                        }
                else:
                    yield deepcopy(integration)

        def transform_portal(portal):
            return {
                k: deepcopy(v) if k != 'integrations' else list(transform_integrations(v))
                for k, v in portal.items()
            }

        return [transform_portal(portal) for portal in db]
