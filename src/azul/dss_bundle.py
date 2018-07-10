from typing import List, MutableMapping, Any

from dataclasses import dataclass


@dataclass
class DSSBundle:
    uuid: str
    version: str
    manifest: List[MutableMapping[str, Any]]
    metadata_files: MutableMapping[str, Any]  # TODO: Change to JSON later