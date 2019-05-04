from concurrent.futures import ThreadPoolExecutor
import logging
from typing import List, Optional, Tuple

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
    kwargs = dict(uuid=uuid,
                  version=version,
                  replica=replica,
                  directurls=directurls,
                  presignedurls=presignedurls)
    url = None
    manifest = []
    while True:
        # We can't use get_file.iterate because it only returns the `bundle.files` part of the response and swallows
        # the `bundle.version`. See https://github.com/HumanCellAtlas/dcp-cli/issues/331
        # noinspection PyUnresolvedReferences,PyProtectedMember
        response = client.get_bundle._request(kwargs, url=url)
        bundle = response.json()['bundle']
        manifest.extend(bundle['files'])
        try:
            url = response.links['next']['url']
        except KeyError:
            break

    metadata_files = {f['name']: f for f in manifest if f['indexed']}

    for f in metadata_files.values():
        content_type, _, _ = f['content-type'].partition(';')
        expected_content_type = 'application/json'
        if not content_type.startswith(expected_content_type):
            raise NotImplementedError(f"Expecting file {f['uuid']}.{f['version']} "
                                      f"to have content type '{expected_content_type}', "
                                      f"not '{content_type}'")

    def download_file(item):
        file_name, manifest_entry = item
        file_uuid = manifest_entry['uuid']
        file_version = manifest_entry['version']
        logger.debug("Getting file '%s' (%s.%s) from DSS.", file_name, file_uuid, file_version)
        # noinspection PyUnresolvedReferences
        file_contents = client.get_file(uuid=file_uuid, version=file_version, replica=replica)

        # Work around https://github.com/HumanCellAtlas/data-store/issues/2073
        if replica == 'gcp' and isinstance(file_contents, bytes):  # pragma: no cover
            import json
            file_contents = json.loads(file_contents)

        if not isinstance(file_contents, dict):
            raise TypeError(f'Expecting file {file_uuid}.{file_version} '
                            f'to contain a JSON object ({dict}), '
                            f'not {type(file_contents)}')
        return file_name, file_contents

    if num_workers == 0:
        metadata_files = map(download_file, metadata_files.items())
    else:
        with ThreadPoolExecutor(num_workers) as tpe:
            metadata_files = tpe.map(download_file, metadata_files.items())

    return bundle['version'], manifest, dict(metadata_files)


def dss_client(deployment: str = 'prod') -> DSSClient:
    """
    Return a DSS client to DSS production or the specified DSS deployment.

    :param deployment: The name of a DSS deployment like `dev`, `integration`, `staging` or `prod`.
    """
    deployment = "" if deployment == "prod" else deployment + "."
    swagger_url = f'https://dss.{deployment}data.humancellatlas.org/v1/swagger.json'
    client = DSSClient(swagger_url=swagger_url)
    client.timeout_policy = Timeout(connect=10, read=40)
    return client
