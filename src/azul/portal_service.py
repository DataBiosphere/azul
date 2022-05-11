from copy import (
    deepcopy,
)
import hashlib
import json
import logging
from typing import (
    Callable,
    Optional,
    Sequence,
    Set,
    Union,
    cast,
)

from botocore.exceptions import (
    ClientError,
)

from azul import (
    cached_property,
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

    @property
    def client(self):
        return aws.s3

    @cached_property
    def version_service(self) -> VersionService:
        return VersionService()

    @property
    def bucket(self):
        return config.portal_db_bucket

    @property
    def object_key(self):
        return config.portal_db_object_key(self.catalog_source)

    @cached_property
    def catalog_source(self):
        # FIXME: Parameterize PortalService instances with current catalog
        #        https://github.com/DataBiosphere/azul/issues/2716
        catalog = config.default_catalog
        md5 = hashlib.md5()
        for source in sorted(config.sources(catalog)):
            md5.update(source.encode())
        return md5.hexdigest()

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
            return portal_db

        self._crud(callback)
        return result

    @cached_property
    def default_db(self) -> JSONs:
        # FIXME: Parameterize PortalService instances with current catalog
        #        https://github.com/DataBiosphere/azul/issues/2716
        catalog = config.default_catalog
        plugin = RepositoryPlugin.load(catalog).create(catalog)
        return self.demultiplex(plugin.portal_db())

    def read(self):
        return self._crud(lambda db: db)

    def overwrite(self, new_db):
        return self._crud(lambda _: new_db)

    def demultiplex(self, db: JSONs) -> JSONs:
        """
        Transform portal integrations database to only contain entity_ids from
        the current catalog source, leaving the original unmodified.

        :param db: portal DB where the `entity_ids` fields  are dictionaries
        whose keys correspond to catalog sources (either the DSS deployment
        stage or a hash of the TDR source).

        :return: deep copy of that DB where the `entity_ids` fields have been
        replaced by the entry associated with the current catalog source.
        If the `entity_ids` field is present but no entity ids are specified for
        the current deployment stages, the integration is removed. Portals,
        however, are not removed even if they have no remaining associated
        integrations.
        """

        def transform_integrations(integrations):
            for integration in integrations:
                try:
                    entity_ids = integration['entity_ids']
                except KeyError:
                    yield deepcopy(integration)
                else:
                    current_entity_ids = entity_ids.get(self.catalog_source)
                    if current_entity_ids:
                        yield {
                            k: deepcopy(v if k != 'entity_ids' else current_entity_ids)
                            for k, v in integration.items()
                        }

        def transform_portal(portal):
            return {
                k: deepcopy(v) if k != 'integrations' else list(transform_integrations(v))
                for k, v in portal.items()
            }

        return list(map(transform_portal, db))

    def _crud(self, operation: Callable[[JSONs], JSONs]) -> JSONs:
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
                new_db = operation(db)
                if db == new_db:
                    # Operation didn't modify DB; we're done.
                    break
                elif new_db is None:
                    # FIXME: Add delete logic for crud
                    #        https://github.com/DataBiosphere/azul/issues/3484
                    assert False
                else:
                    self._write_db(new_db, version)
                    db = new_db
            except VersionConflict:
                # Retry with up-to-date DB
                continue
            else:
                break

        return db

    def _create_db(self) -> tuple[JSONs, str]:
        """
        Write hardcoded portal integrations DB to S3.
        :return: Newly created DB and accompanying version.
        """
        db = self.default_db
        version = self._write_db(db, None)
        return db, version

    def _read_db(self, version: str) -> JSONs:
        """
        Retrieve specified version of portal DB from S3.
        Raises `NoSuchObjectVersion` if the version is not found.
        """
        try:
            response = self.client.get_object(Bucket=self.bucket,
                                              Key=self.object_key,
                                              VersionId=version)
        except self.client.exceptions.NoSuchKey:
            # We hypothesize that when S3 was only eventually consistent,
            # NoSuchKey would have been raised when an object had been
            # created but hadn't materialized yet …
            raise NoSuchObjectVersion(version)
        except ClientError as e:
            # … and that NoSuchVersion would have been raised when the object had
            # been overwritten but the overwrite hadn't materialized yet.
            if e.response['Error']['Code'] == 'NoSuchVersion':
                raise NoSuchObjectVersion(version)
            else:
                raise
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
        response = self.client.put_object(Bucket=self.bucket,
                                          Key=self.object_key,
                                          Body=json_bytes,
                                          ContentType='application/json',
                                          Tagging='='.join(self._expiration_tag))
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
            self.client.delete_object(Bucket=self.bucket,
                                      Key=self.object_key,
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
        return f's3:/{self.bucket}/{self.object_key}'

    @property
    def _expiration_tag(self) -> tuple[str, str]:
        return 'expires', str(not config.is_main_deployment()).lower()
