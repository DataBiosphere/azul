from typing import NamedTuple

from dataclasses import dataclass

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
    metadata_files: MutableJSON

    @classmethod
    def for_fqid(cls, fqid, manifest: MutableJSONs, metadata_files: MutableJSON) -> 'Bundle':
        uuid, version = fqid
        return cls(uuid=uuid,
                   version=version,
                   manifest=manifest,
                   metadata_files=metadata_files)
