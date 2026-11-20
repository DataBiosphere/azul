from abc import (
    ABCMeta,
)
from collections.abc import (
    Mapping,
)
from contextlib import (
    contextmanager,
)
import hashlib
import io
from typing import (
    Any,
    IO,
)
from unittest.mock import (
    patch,
)

from app_test_case import (
    LocalAppTestCase,
)
from azul.lib.types import (
    JSON,
)
from azul.resources import (
    list_resources,
    open_resource,
)
from azul.service.contract_service import (
    ContractService,
)


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def contract_file(version: int, text: str) -> tuple[str, str]:
    """
    The name and content of the file containing the given version of a contract
    """
    return f'v{version}-{sha256_hex(text)}.md', text


class ContractTestCase(LocalAppTestCase, metaclass=ABCMeta):

    @classmethod
    def app_name(cls) -> str:
        return 'service'

    _contract = 'anvil_registered_access'

    @contextmanager
    def _mock_contract_files(self, files: Mapping[str, str]):
        """
        Replace the files in the directory of the contract with the given ones,
        mapping the name of each file to its content
        """
        directory = 'static', 'contracts', self._contract

        def mock_list_resources(*path: str, package_root: str) -> list[str]:
            if path == directory:
                return list(files)
            else:
                return list_resources(*path, package_root=package_root)

        def mock_open_resource(*path: str,
                               package_root: str,
                               binary: bool = False
                               ) -> IO[Any]:
            if path[:-1] == directory:
                content = files[path[-1]]
                return io.BytesIO(content.encode()) if binary else io.StringIO(content)
            elif binary:
                return open_resource(*path, package_root=package_root, binary=True)
            else:
                return open_resource(*path, package_root=package_root)

        with patch.multiple(ContractService.__module__,
                            list_resources=mock_list_resources,
                            open_resource=mock_open_resource):
            yield

    _contract_files = dict([
        ('current', '2\n'),
        # The order of the entries in a directory is arbitrary
        contract_file(4, 'Four'),
        contract_file(1, 'One'),
        contract_file(2, 'Two'),
        # None of these is a version of the contract
        contract_file(0, 'Zero'),
        (f'v03-{sha256_hex('Three')}.md', 'Three'),
        (f'v3-{sha256_hex('Three').upper()}.md', 'Three'),
        (f'v3-{sha256_hex('Three')[:-1]}.md', 'Three'),
        ('v3.md', 'Three'),
        (f'v5-{sha256_hex('Five')}.MD', 'Five'),
        (f'v6-{sha256_hex('Six')}', 'Six'),
        ('README.md', 'Read me')
    ])

    def _contract_sha256(self, version: int) -> str:
        """
        The SHA-256 digest of the given version of the contract
        """
        service = ContractService(package_root=self._app.package_root)
        return service.contract(self._contract).versions[version]

    def _versions(self, *versions: tuple[int, str]) -> list[JSON]:
        """
        The expected `versions` property for the given pairs of version number
        and text
        """
        return [
            {
                'version': version,
                'sha256': sha256_hex(text),
                'url': str(self.base_url.set(path=('contracts', self._contract, str(version))))
            }
            for version, text in versions
        ]
