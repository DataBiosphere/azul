from dataclasses import (
    dataclass,
)
from typing import (
    NamedTuple,
)

from azul.types import (
    MutableJSON,
    MutableJSONs,
)

BundleUUID = str
BundleVersion = str


class BundleFQID(NamedTuple):
    uuid: BundleUUID
    version: BundleVersion


@dataclass
class Bundle:
    uuid: BundleUUID
    version: BundleVersion
    manifest: MutableJSONs
    """
    Each item of the `manifest` attribute's value has this shape:
    {
        'content-type': 'application/json; dcp-type="metadata/biomaterial"',
        'crc32c': 'fd239631',
        'indexed': True,
        'name': 'cell_suspension_0.json',
        's3_etag': 'aa31c093cc816edb1f3a42e577872ec6',
        'sha1': 'f413a9a7923dee616309e4f40752859195798a5d',
        'sha256': 'ea4c9ed9e53a3aa2ca4b7dffcacb6bbe9108a460e8e15d2b3d5e8e5261fb043e',
        'size': 1366,
        'uuid': '0136ebb4-1317-42a0-8826-502fae25c29f',
        'version': '2019-05-16T162155.020000Z'
    }
    """
    metadata_files: MutableJSON

    @classmethod
    def for_fqid(cls, fqid: BundleFQID, *, manifest: MutableJSONs, metadata_files: MutableJSON) -> 'Bundle':
        uuid, version = fqid
        return cls(uuid=uuid,
                   version=version,
                   manifest=manifest,
                   metadata_files=metadata_files)

    @property
    def fquid(self):
        return BundleFQID(self.uuid, self.version)
