from collections.abc import (
    Mapping,
)
import hashlib
import re

import attrs

from azul.lib import (
    R,
)
from azul.resources import (
    list_resources,
    open_resource,
)


class NoSuchContractError(Exception):
    """
    This deployment offers no contract of the given name, or the contract has no
    version of the given number.
    """


@attrs.frozen(kw_only=True)
class Contract:
    name: str

    #: The number of the current version
    current: int

    _service: ContractService

    #: Maps the number of each version of the contract to the SHA-256 digest of
    #: that version's text, in ascending order of version
    versions: Mapping[int, str]

    def text(self, version: int) -> str:
        """
        The Markdown text of the given version of this contract
        """
        if version in self.versions:
            sha256 = self.versions[version]
            file_name = self._service._file_name(version, sha256)
            text = self._service._load(self.name, file_name)
            actual_sha256 = hashlib.sha256(text).hexdigest()
            assert actual_sha256 == sha256, R(
                'The SHA-256 digest of the contract does not match its file name',
                self.name, file_name, actual_sha256)
            return text.decode()
        else:
            raise NoSuchContractError(self.name, version)


@attrs.frozen(kw_only=True)
class ContractService:
    """
    The contracts a user can accept. Each version of a contract is a Markdown
    file named `v{version}-{sha256}.md` in a directory named after the
    contract, under `resources/static/contracts`, where `sha256` is the
    hexadecimal SHA-256 digest of the file's content. Embedding the digest in
    the file name lets the digests of all versions be listed without reading
    every file. A file named `current` in that directory contains the number of
    the current version. All versions are kept, since a user may have accepted
    any of them.
    """

    #: See :func:`azul.resources.open_resource`
    package_root: str

    def names(self) -> list[str]:
        """
        The names of all contracts, in sorted order
        """
        return sorted(self._list())

    _file_name_re = re.compile(r'v([1-9][0-9]*)-([0-9a-f]{64})\.md')

    def _file_name(self, version: int, sha256: str) -> str:
        return f'v{version}-{sha256}.md'

    def contract(self, name: str) -> Contract:
        if name in self.names():
            file_names = self._list(name)
            matches = (
                self._file_name_re.fullmatch(file_name)
                for file_name in file_names
            )
            versions = sorted(
                (int(match[1]), match[2])
                for match in matches
                if match is not None
            )
            numbers = [version for version, sha256 in versions]
            assert len(set(numbers)) == len(numbers), R(
                'The contract has more than one file per version',
                name, versions)
            current = int(self._load(name, 'current'))
            assert current in numbers, R(
                'The current version of the contract does not exist',
                name, current)
            return Contract(name=name,
                            current=current,
                            service=self,
                            versions=dict(versions))
        else:
            raise NoSuchContractError(name)

    def _list(self, *path: str) -> list[str]:
        return list_resources('static', 'contracts', *path,
                              package_root=self.package_root)

    def _load(self, name: str, file_name: str) -> bytes:
        with open_resource('static', 'contracts', name, file_name,
                           package_root=self.package_root,
                           binary=True) as f:
            return f.read()
