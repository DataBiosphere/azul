from functools import (
    lru_cache,
)
from time import (
    time,
)
from typing import (
    Optional,
)

from chalice import (
    NotFoundError,
)
from elasticsearch_dsl.response import (
    Hit,
)
from furl import (
    furl,
)
# noinspection PyPackageRequirements
import google.cloud.storage as gcs
from more_itertools import (
    one,
)

from azul import (
    CatalogName,
)
from azul.drs import (
    DRSClient,
)
from azul.dss import (
    shared_credentials,
)
from azul.plugins import (
    RepositoryPlugin,
)
from azul.service.elasticsearch_service import (
    ElasticsearchService,
)
from azul.tdr import (
    TDRClient,
)
from azul.types import (
    JSON,
)


class RepositoryFileService(ElasticsearchService):

    # FIXME: rename class to RepositoryService
    #        https://github.com/DataBiosphere/azul/issues/2277

    def __init__(self) -> None:
        super().__init__()

    @lru_cache(maxsize=None)
    def repository_plugin(self, catalog: CatalogName) -> RepositoryPlugin:
        return RepositoryPlugin.load(catalog).create(catalog)

    def _hit_to_doc(self, catalog: CatalogName, hit: Hit) -> JSON:
        return self.translate_fields(catalog, hit.to_dict(), forward=False)

    def _get_file_document(self,
                           catalog: CatalogName,
                           file_uuid: str,
                           file_version: Optional[str]) -> JSON:
        """
        Return a file document from Elasticsearch.
        """
        filters = {
            "fileId": {"is": [file_uuid]},
            **({"fileVersion": {"is": [file_version]}}
               if file_version else {})
        }
        es_search = self._create_request(catalog=catalog,
                                         filters=filters,
                                         post_filter=True,
                                         enable_aggregation=False,
                                         entity_type='files')
        hit = one(self._hit_to_doc(catalog, hit) for hit in es_search.scan())
        if not hit:
            raise NotFoundError(f'Unable to find file {catalog}/{file_uuid}/{file_version}')
        file = one(file for file in hit['contents']['files'])
        if file_version:
            assert file_version == file['version']
        if file['drs_path'] is None:
            raise NotFoundError(f'No DRS path found for file {catalog}/{file_uuid}/{file_version}')
        return file

    def _get_blob(self, bucket_name: str, blob_name: str) -> gcs.Blob:
        """
        Get a Blob object by name.
        """
        with shared_credentials():
            client = gcs.Client()
        bucket = gcs.Bucket(client, bucket_name)
        return bucket.get_blob(blob_name)

    def get_signed_url(self,
                       catalog: CatalogName,
                       file_uuid: str,
                       file_version: Optional[str],
                       expiration_seconds: int = 3600) -> str:
        """
        Return a signed URL for the specified file.
        """
        file = self._get_file_document(catalog, file_uuid, file_version)
        file_name = file['name'].replace('"', r'\"')
        assert all(0x1f < ord(c) < 0x80 for c in file_name)
        drs_uri = self.repository_plugin(catalog).drs_uri(file['drs_path'])
        drs_client = DRSClient()
        drs_url = drs_client.drs_uri_to_url(drs_uri)
        tdr_client = TDRClient()
        access_url = tdr_client.get_access_url(drs_url, method_type='gs')
        url_parts = furl(access_url['url'])
        blob = self._get_blob(bucket_name=url_parts.netloc,
                              blob_name='/'.join(url_parts.path.segments))
        expiration = int(time() + expiration_seconds)
        disposition = f"attachment; filename={file_name}"
        return blob.generate_signed_url(expiration=expiration,
                                        response_disposition=disposition)
