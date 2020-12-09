from copy import (
    deepcopy,
)
import json
import logging
from typing import (
    Callable,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
)

from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.plugins import (
    RepositoryPlugin,
)
from azul.types import (
    JSON,
    JSONs,
)
from azul.version_service import (
    NoSuchObjectVersion,
    VersionConflict,
    VersionService,
)

log = logging.getLogger(__name__)


class PortalService:

    def __init__(self):
        self.client = aws.client('s3')
        self.version_service = VersionService()

    def list_integrations(self, entity_type: str, integration_type: str, entity_ids: Optional[Set[str]]) -> JSONs:
        """
        Return matching portal integrations.

        :param entity_type: The type of the entity to which an integration applies (e.g. project, file, bundle)
        :param integration_type: The kind of integration (e.g. get, get_entity, get_entities, get_manifest)
        :param entity_ids: If given results will be limited to this set of entity UUIDs
        :return: A list of portals that have one or more matching integrations
        """
        result = []

        def callback(portal_db):
            for portal in portal_db:
                integrations = [
                    integration
                    for integration in cast(Sequence[JSON], portal['integrations'])
                    if (integration['entity_type'] == entity_type
                        and integration['integration_type'] == integration_type
                        and (entity_ids is None
                             or 'entity_ids' not in integration
                             or not entity_ids.isdisjoint(integration['entity_ids'])))
                ]
                if len(integrations) > 0:
                    portal = {k: v if k != 'integrations' else integrations for k, v in portal.items()}
                    result.append(portal)

        self._crud(callback)
        return result

    def read(self):
        return self._crud(lambda db: db)

    def overwrite(self, new_db):
        return self._crud(lambda _: new_db)

    def demultiplex(self, db: JSONs) -> JSONs:
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
                try:
                    current_entity_ids = integration['entity_ids'].get(config.dss_deployment_stage)
                    if current_entity_ids:
                        yield {
                            k: deepcopy(v if k != 'entity_ids' else current_entity_ids)
                            for k, v in integration.items()
                        }
                except KeyError:
                    yield deepcopy(integration)

        def transform_portal(portal):
            return {
                k: deepcopy(v) if k != 'integrations' else list(transform_integrations(v))
                for k, v in portal.items()
            }

        return list(map(transform_portal, db))

    def _crud(self, operation: Callable[[JSONs], Optional[JSONs]]) -> Optional[JSONs]:
        """
        Perform a concurrent read/write operation on the portal integrations DB.

        :param operation: Callable that accepts the latest version of the portal
        DB and optionally returns an updated version to be uploaded.
        :return: the result of the operation.
        """
        db = None
        while True:
            version = self.version_service.get(self._db_url)
            try:
                if version is None:
                    log.info('Portal integration DB not found in S3; uploading hard-coded DB.')
                    db, version = self._create_db()
                else:
                    try:
                        db = self._read_db(version)
                    except NoSuchObjectVersion:
                        # Wait for latest version to appear in S3.
                        continue
                db = operation(db)
                if db is None:
                    # Operation is read-only; we're done.
                    break
                else:
                    self._write_db(db, version)
            except VersionConflict:
                # Retry with up-to-date DB
                continue
            else:
                break

        return db

    def _create_db(self) -> Tuple[JSONs, str]:
        """
        Write hardcoded portal integrations DB to S3.
        :return: Newly created DB and accompanying version.
        """
        catalog = config.default_catalog
        plugin = RepositoryPlugin.load(catalog).create(catalog)
        db = self.demultiplex(plugin.portal_db())
        version = self._write_db(db, None)
        return db, version

    def _read_db(self, version: str) -> JSONs:
        """
        Retrieve specified version of portal DB from S3.
        Raises `NoSuchObjectVersion` if the version is not found.
        """
        try:
            response = self.client.get_object(Bucket=config.portal_db_bucket,
                                              Key=config.portal_db_object_key,
                                              VersionId=version)
        except self.client.exceptions.NoSuchKey:
            raise NoSuchObjectVersion(version)
        else:
            json_bytes = response['Body'].read()
            return json.loads(json_bytes.decode())

    def _write_db(self, db: JSONs, version: Optional[str]) -> str:
        """
        Try to write portal integrations database to S3 and update version.
        Update is rejected with `VersionConflict` if `version` is not current.
        :param db: the DB to be written to S3.
        :param version: the version of the DB this write is intended to replace.
        :return: version of the newly written DB.
        """
        json_bytes = json.dumps(db).encode()
        response = self.client.put_object(Bucket=config.portal_db_bucket,
                                          Key=config.portal_db_object_key,
                                          Body=json_bytes,
                                          ContentType='application/json')
        new_version = response['VersionId']
        try:
            self.version_service.put(self._db_url, version, new_version)
        except VersionConflict:
            # Operation was performed on outdated DB and now erroneously exists
            # in S3.
            self._delete_db(new_version)
            raise
        else:
            return new_version

    def _delete_db(self, version: str) -> None:
        """
        Delete the specified version of the portal integrations DB from S3.
        Failures are logged and ignored.
        """
        try:
            self.client.delete_object(Bucket=config.portal_db_bucket,
                                      Key=config.portal_db_object_key,
                                      VersionId=version)
        except self.client.exceptions.NoSuchKey:
            log.info(f'Failed to delete version {version} of portal DB from S3.')

    @classmethod
    def validate(cls, portal_text: Union[str, bytes]) -> None:
        portal = json.loads(portal_text)
        for required_field in ('portal_id', 'integrations'):
            assert required_field in portal

    @property
    def _db_url(self) -> str:
        return f's3:/{config.portal_db_bucket}/{config.portal_db_object_key}'
