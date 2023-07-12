from abc import (
    ABC,
)
import logging

import attr

from azul import (
    CatalogName,
)
from azul.indexer import (
    BUNDLE_FQID,
    Bundle,
)
from azul.types import (
    JSON,
    MutableJSON,
    MutableJSONs,
)

log = logging.getLogger(__name__)


@attr.s(auto_attribs=True, kw_only=True)
class HCABundle(Bundle[BUNDLE_FQID], ABC):
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

    def reject_joiner(self, catalog: CatalogName):
        # The dcp1 release has project metadata containing the character string
        # we use as a manifest column joiner, and since we won't be getting an
        # updated snapshot for dcp1 we skip the joiner check when using dcp1.
        if catalog == 'dcp1':
            pass
        else:
            self._reject_joiner(self.manifest)
            self._reject_joiner(self.metadata_files)

    def to_json(self) -> MutableJSON:
        return {
            'manifest': self.manifest,
            'metadata': self.metadata_files
        }

    @classmethod
    def from_json(cls, fqid: BUNDLE_FQID, json_: JSON) -> 'Bundle':
        manifest = json_['manifest']
        metadata = json_['metadata']
        assert isinstance(manifest, list), manifest
        assert isinstance(metadata, dict), metadata
        return cls(fqid=fqid, manifest=manifest, metadata_files=metadata)
