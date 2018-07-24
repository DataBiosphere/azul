from concurrent.futures import ThreadPoolExecutor
from typing import Tuple, List
from hca.dss import DSSClient

from humancellatlas.data.metadata import JSON


def download_bundle_metadata(client: DSSClient,
                             uuid: str,
                             version: str,
                             replica: str,
                             num_workers: int = 8
                             ) -> Tuple[List[JSON], JSON]:
    bundle = client.get_bundle(uuid=uuid, version=version, replica=replica)
    manifest = bundle['bundle']['files']
    metadata_files = {f["name"]: f for f in manifest if f["indexed"]}
    with ThreadPoolExecutor(num_workers) as tpe:
        def get_metadata(item):
            file_name, manifest_entry = item
            return file_name, client.get_file(uuid=manifest_entry['uuid'],
                                              version=manifest_entry['version'],
                                              replica='aws')

        metadata_files = dict(tpe.map(get_metadata, metadata_files.items()))
    return manifest, metadata_files


