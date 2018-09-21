from concurrent.futures import ThreadPoolExecutor
import logging
from typing import List, Optional, Tuple

from hca import HCAConfig
from hca.dss import DSSClient
from urllib3 import Timeout

from humancellatlas.data.metadata.api import JSON

logger = logging.getLogger(__name__)


def download_bundle_metadata(client: DSSClient,
                             replica: str,
                             uuid: str,
                             version: Optional[str] = None,
                             directurls: bool = False,
                             presignedurls: bool = False,
                             num_workers: Optional[int] = None) -> Tuple[str, List[JSON], JSON]:
    """
    Download the metadata for a given bundle from the HCA data store (DSS).

    :param client: A DSS API client instance

    :param replica: The name of the DSS replica to use

    :param uuid: The UUID of the bundle in DSS

    :param version: The version of the bundle. if None, the most recent version of the bundle will be downloaded.

    :param directurls: Whether to include direct-access URLs in the response. This is mutually
                       exclusive with the presignedurls parameter. Note: including `directurls` and `presignedurls` in
                       the function call will cause the DSS to copy metadata and data files in the bundle to another
                       bucket first. That could be time-consuming and/or inefficient for users who only want to work
                       with the metadata instead of the files. It is very likely `directurls` and `presignedurls` will
                       be removed or changed in the future.

    :param presignedurls: A boolean controls whether to include presigned URLs in the response. This is mutually
                          exclusive with the directurls parameter. Note this parameter, similar to the `directurls`,
                          is a temporary parameter, and it's not guaranteed to stay in this place in the future.

    :param num_workers: The size of the thread pool to use for downloading metadata files in parallel. If None, the
                        default pool size will be used, typically a small multiple of the number of cores on the system
                        executing this function. If 0, no thread pool will be used and all files will be downloaded
                        sequentially by the current thread.

    :return: A tuple consisting of the version of the downloaded bundle, a list of the manifest entries for all files
             in the bundle (data and metadata) and a dictionary mapping the file name of each metadata file in the
             bundle to the JSON contents of that file.
    """
    if directurls or presignedurls:
        logger.warning("PendingDeprecationWarning: `directurls` and `presignedurls` are temporary parameters and not"
                       " guaranteed to stay in the code base in the future!")

    logger.debug("Getting bundle %s.%s from DSS.", uuid, version)
    # noinspection PyUnresolvedReferences
    response = client.get_bundle(uuid=uuid,
                                 version=version,
                                 replica=replica,
                                 directurls=directurls,
                                 presignedurls=presignedurls)
    bundle = response['bundle']
    manifest = bundle['files']
    metadata_files = {f["name"]: f for f in manifest if f["indexed"]}

    def download_file(item):
        file_name, manifest_entry = item
        file_uuid = manifest_entry['uuid']
        file_version = manifest_entry['version']
        logger.debug("Getting file '%s' (%s.%s) from DSS.", file_name, file_uuid, file_version)
        # noinspection PyUnresolvedReferences
        return file_name, client.get_file(uuid=file_uuid, version=file_version, replica='aws')

    if num_workers == 0:
        metadata_files = map(download_file, metadata_files.items())
    else:
        with ThreadPoolExecutor(num_workers) as tpe:
            metadata_files = tpe.map(download_file, metadata_files.items())

    return bundle['version'], manifest, dict(metadata_files)


def dss_client(deployment: Optional[str] = None) -> DSSClient:
    """
    Return a DSS client to DSS production or the specified DSS deployment.

    :param deployment: The name of a DSS deployment like `dev`, `integration` or `staging`. If None, the production
                       deployment (`prod`) will be used.
    """
    # Work around https://github.com/HumanCellAtlas/dcp-cli/issues/142
    hca_config = HCAConfig()
    deployment = deployment + "." if deployment else ""
    hca_config['DSSClient'].swagger_url = f'https://dss.{deployment}data.humancellatlas.org/v1/swagger.json'
    # Clear the cached swagger specs that may come from a different deployment. This work-around isn't thread safe but
    # neither is the caching iteself.
    DSSClient._swagger_spec = None
    client = DSSClient(config=hca_config)
    client.timeout_policy = Timeout(connect=10, read=40)
    return client
