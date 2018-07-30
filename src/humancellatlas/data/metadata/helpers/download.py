from concurrent.futures import ThreadPoolExecutor
import logging
from typing import Tuple, List, Optional
from hca.dss import DSSClient

from humancellatlas.data.metadata import JSON

logger = logging.getLogger(__name__)


def download_bundle_metadata(client: DSSClient,
                             uuid: str,
                             version: str,
                             replica: str,
                             num_workers: Optional[int] = None
                             ) -> Tuple[List[JSON], JSON]:
    """
    Download the metadata for a given bundle.

    :param client: the DSS API client object to use

    :param uuid: the UUID of the bundle

    :param version: the version of the bundle

    :param replica: the name of the replica to use

    :param num_workers: the size of the thread pool to use for downloading metadata files in parallel. If None, the
                        default pool size will be used, typically a small multiple of the number of cores on the system
                        executing this function. If 0, no thread pool will be used and all files will be downloaded
                        sequentially by the current thread.

    :return: a list of the manifest entries for all files (data and metadata) in the bundle and a dictionary mapping
             the file name of each metadata file to its contents.
    """
    logger.debug("Getting bundle %s.%s from DSS.", uuid, version)
    # noinspection PyUnresolvedReferences
    bundle = client.get_bundle(uuid=uuid, version=version, replica=replica)
    manifest = bundle['bundle']['files']
    metadata_files = {f["name"]: f for f in manifest if f["indexed"]}

    def download_file(item):
        file_name, manifest_entry = item
        # noinspection PyUnresolvedReferences
        file_uuid = manifest_entry['uuid']
        file_version = manifest_entry['version']
        logger.debug("Getting file '%s' (%s.%s) from DSS.", file_name, file_uuid, file_version)
        return file_name, client.get_file(uuid=file_uuid, version=file_version, replica='aws')

    if num_workers == 0:
        metadata_files = map(download_file, metadata_files.items())
    else:
        with ThreadPoolExecutor(num_workers) as tpe:
            metadata_files = tpe.map(download_file, metadata_files.items())

    return manifest, dict(metadata_files)
