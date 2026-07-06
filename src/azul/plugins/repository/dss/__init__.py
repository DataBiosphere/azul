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

from furl import (
    furl,
)
from more_itertools import (
    one,
)

from azul import (
    config,
)
from azul.auth import (
    Authentication,
)
from azul.deployment import (
    aws,
)
from azul.http import (
    HasCachedHttpClient,
    raise_on_status,
)
from azul.indexer import (
    SourcedBundleFQID,
)
from azul.lib.collections import (
    adict,
)
from azul.lib.time import (
    parse_dcp2_version,
)
from azul.lib.types import (
    JSON,
)
from azul.plugins import (
    File,
    RepositoryPlugin,
)
from azul.plugins.metadata.hca.bundle import (
    HCABundle,
)
from azul.source import (
    Prefix,
    SimpleSourceSpec,
    SourceRef,
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

    def list_sources(self,
                     authentication: Authentication | None
                     ) -> list[DSSSourceRef]:
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
                         ) -> str | None:
        dss_endpoint = one(self.sources).name
        url = furl(dss_endpoint)
        url.path.add(['files', file_uuid])
        url.query.add(adict(version=file_version, replica=replica))
        return str(url)

    def file_download_url(self,
                          file: File,
                          authentication: Authentication | None,
                          replica: str | None = None
                          ) -> str | None:
        if replica is None:
            replica = 'aws'
        dss_url = self._direct_file_url(file_uuid=file.uuid,
                                        file_version=file.version,
                                        replica=replica)
        dss_response = self._http_client.request('GET', dss_url, redirect=False)
        if dss_response.status == 301:
            assert False, 'DSS plugin no longer supports Retry-After'
        elif dss_response.status == 302:
            location = dss_response.headers['Location']
            # Remove once https://github.com/HumanCellAtlas/data-store/issues/1837 is resolved
            if True:
                location = urllib.parse.urlparse(location)
                query = urllib.parse.parse_qs(location.query, strict_parsing=True)
                expires = int(one(query['Expires']))
                bucket = location.netloc.partition('.')[0]
                dss_endpoint = one(self.sources).name
                assert bucket == aws.dss_checkout_bucket(dss_endpoint), bucket
                with aws.direct_access_credentials(dss_endpoint, lambda_name='service'):
                    # FIXME: make region configurable (https://github.com/DataBiosphere/azul/issues/1560)
                    s3 = aws.client('s3', region_name='us-east-1')
                    params = {
                        'Bucket': bucket,
                        'Key': location.path[1:],
                        'ResponseContentDisposition': 'attachment;filename=' + file.name,
                    }
                    location = s3.generate_presigned_url(ClientMethod=s3.get_object.__name__,
                                                         ExpiresIn=round(expires - time.time()),
                                                         Params=params)
            return location
        else:
            raise_on_status(dss_response)
            assert False

    def validate_version(self, version: str) -> None:
        # Note that this validates against the DCP2 format instead of the DSS
        # format (azul.dss.version_format). This is necessary due to commit
        # 48ef9388 which manually updated all the canned DSS bundles to use
        # DCP/2 version format.
        parse_dcp2_version(version)
