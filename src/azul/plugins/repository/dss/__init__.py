import logging
import time
from typing import (
    NoReturn,
    Self,
)
import urllib
import urllib.parse
from uuid import (
    UUID,
    uuid5,
)

import attrs
from furl import (
    furl,
)
from more_itertools import (
    one,
)
import requests

from azul import (
    config,
)
from azul.auth import (
    Authentication,
)
from azul.collections import (
    adict,
)
from azul.deployment import (
    aws,
)
from azul.http import (
    HasCachedHttpClient,
)
from azul.indexer import (
    Prefix,
    SimpleSourceSpec,
    SourceRef,
    SourcedBundleFQID,
)
from azul.plugins import (
    RepositoryFileDownload,
    RepositoryPlugin,
)
from azul.plugins.metadata.hca.bundle import (
    HCABundle,
)
from azul.time import (
    parse_dcp2_version,
)
from azul.types import (
    JSON,
)

log = logging.getLogger(__name__)


class DSSSourceRef(SourceRef[SimpleSourceSpec]):
    """
    Subclass of `Source` to create new namespace for source IDs.
    """
    namespace: UUID = UUID('6925391e-6519-41d9-879f-c6307eb83c1c')

    @classmethod
    def for_dss_source(cls, source: str, prefix: str) -> Self:
        # We hash the endpoint instead of using it verbatim to distinguish them
        # within a document, which is helpful for testing.
        spec = SimpleSourceSpec.parse(source)
        prefix = Prefix.parse(prefix)
        return cls(id=cls.id_from_spec(spec), spec=spec, prefix=prefix)

    @classmethod
    def id_from_spec(cls, spec: SimpleSourceSpec) -> str:
        return str(uuid5(cls.namespace, spec.name))


class DSSBundleFQID(SourcedBundleFQID[DSSSourceRef]):
    pass


class DSSBundle(HCABundle[DSSBundleFQID]):

    @classmethod
    def canning_qualifier(cls) -> str:
        return 'dss.hca'

    def drs_uri(self, manifest_entry: JSON) -> str:
        file_uuid = manifest_entry['uuid']
        file_version = manifest_entry['version']
        netloc = config.drs_domain or config.api_lambda_domain('service')
        return str(furl(scheme='drs',
                        netloc=netloc,
                        path=(file_uuid,),
                        args={'version': file_version}))


class Plugin(RepositoryPlugin[
                 DSSBundle,
                 SimpleSourceSpec,
                 DSSSourceRef,
                 DSSBundleFQID
             ],
             HasCachedHttpClient):

    def _lookup_source_id(self, spec: SimpleSourceSpec) -> str:
        return DSSSourceRef.id_from_spec(spec)

    def count_bundles(self, source: DSSSourceRef) -> NoReturn:
        assert False, 'DSS is EOL'

    def count_files(self, source: DSSSourceRef) -> NoReturn:
        assert False, 'DSS is EOL'

    def list_accessible_sources(self,
                                authentication: Authentication | None
                                ) -> list[DSSSourceRef]:
        return self.list_sources()

    def list_sources(self) -> list[DSSSourceRef]:
        return [
            DSSSourceRef(id=self._lookup_source_id(spec), spec=spec, prefix=None)
            for spec in self.sources
        ]

    def list_bundles(self,
                     source: DSSSourceRef,
                     prefix: str
                     ) -> NoReturn:
        assert False, 'DSS is EOL'

    def fetch_bundle(self, bundle_fqid: DSSBundleFQID) -> NoReturn:
        assert False, 'DSS is EOL'

    def list_files(self, source: DSSSourceRef, prefix: str) -> NoReturn:
        assert False, 'DSS is EOL'

    def dss_subscription_query(self, prefix: str) -> JSON:
        return {
            "query": {
                "bool": {
                    "must_not": [
                        {
                            "term": {
                                "admin_deleted": True
                            }
                        }
                    ],
                    "must": [
                        {
                            "exists": {
                                "field": "files.project_json"
                            }
                        },
                        *self._prefix_clause(prefix)
                    ]
                }
            }
        }

    def dss_deletion_subscription_query(self, prefix: str) -> JSON:
        return {
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "admin_deleted": True
                            }
                        },
                        *self._prefix_clause(prefix)
                    ]
                }
            }
        }

    def _prefix_clause(self, prefix):
        return [
            {
                'prefix': {
                    'uuid': prefix
                }
            }
        ] if prefix else []

    def _direct_file_url(self,
                         file_uuid: str,
                         *,
                         file_version: str | None = None,
                         replica: str | None = None,
                         token: str | None = None,
                         ) -> str | None:
        dss_endpoint = one(self.sources).name
        url = furl(dss_endpoint)
        url.path.add(['files', file_uuid])
        url.query.add(adict(version=file_version, replica=replica, token=token))
        return str(url)

    def file_download_class(self) -> type[RepositoryFileDownload]:
        return DSSFileDownload

    def validate_version(self, version: str) -> None:
        # Note that this validates against the DCP2 format instead of the DSS
        # format (azul.dss.version_format). This is necessary due to commit
        # 48ef9388 which manually updated all the canned DSS bundles to use
        # DCP/2 version format.
        parse_dcp2_version(version)


class DSSFileDownload(RepositoryFileDownload):
    _location: str | None = None
    _retry_after: int | None = None

    def update(self,
               plugin: RepositoryPlugin,
               authentication: Authentication | None
               ) -> None:
        self.file = attrs.evolve(self.file, drs_uri=None)  # to shorten the retry URLs
        if self.replica is None:
            self.replica = 'aws'
        assert isinstance(plugin, Plugin)
        # noinspection PyProtectedMember
        dss_url = plugin._direct_file_url(file_uuid=self.file.uuid,
                                          file_version=self.file.version,
                                          replica=self.replica,
                                          token=self.token)
        dss_response = requests.get(dss_url, allow_redirects=False)
        if dss_response.status_code == 301:
            retry_after = int(dss_response.headers.get('Retry-After'))
            location = dss_response.headers['Location']

            location = urllib.parse.urlparse(location)
            query = urllib.parse.parse_qs(location.query, strict_parsing=True)
            self.token = one(query['token'])
            self.replica = one(query['replica'])
            self.file = attrs.evolve(self.file, version=one(query['version']))
            self._retry_after = retry_after
        elif dss_response.status_code == 302:
            location = dss_response.headers['Location']
            # Remove once https://github.com/HumanCellAtlas/data-store/issues/1837 is resolved
            if True:
                location = urllib.parse.urlparse(location)
                query = urllib.parse.parse_qs(location.query, strict_parsing=True)
                expires = int(one(query['Expires']))
                bucket = location.netloc.partition('.')[0]
                dss_endpoint = one(plugin.sources).name
                assert bucket == aws.dss_checkout_bucket(dss_endpoint), bucket
                with aws.direct_access_credentials(dss_endpoint, lambda_name='service'):
                    # FIXME: make region configurable (https://github.com/DataBiosphere/azul/issues/1560)
                    s3 = aws.client('s3', region_name='us-east-1')
                    params = {
                        'Bucket': bucket,
                        'Key': location.path[1:],
                        'ResponseContentDisposition': 'attachment;filename=' + self.file.name,
                    }
                    location = s3.generate_presigned_url(ClientMethod=s3.get_object.__name__,
                                                         ExpiresIn=round(expires - time.time()),
                                                         Params=params)
            self._location = location
        else:
            dss_response.raise_for_status()
            assert False

    @property
    def location(self) -> str | None:
        return self._location

    @property
    def retry_after(self) -> int | None:
        return self._retry_after
