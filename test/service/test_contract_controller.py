import base64
import hashlib
import json

from azul.logging import (
    configure_test_logging,
    get_test_logger,
)
from azul_test_case import (
    DCP2TestCase,
)
from service.contract_test_case import (
    ContractTestCase,
    contract_file,
    sha256_hex,
)

log = get_test_logger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(log)


def _content_digest(content: bytes) -> str:
    return f'sha-256=:{base64.b64encode(hashlib.sha256(content).digest()).decode()}:'


def _etag(version: int, content: bytes) -> str:
    return f'"{version}-{hashlib.sha256(content).hexdigest()}"'


class TestContractController(DCP2TestCase, ContractTestCase):

    def _get(self, path: str):
        return self._get_url(str(self.base_url.set(path=path)))

    def _get_url(self, url: str):
        return self._http_client.request('GET', url, redirect=False)

    def test_list_contracts(self):
        response = self._get('/contracts')
        self.assertEqual(200, response.status)
        url = self.base_url.set(path=('contracts', self._contract))
        expected = {'contracts': {self._contract: str(url)}}
        self.assertEqual(expected, json.loads(response.data))

    def test_list_contract_versions(self):
        response = self._get(f'/contracts/{self._contract}')
        self.assertEqual(200, response.status)
        body = json.loads(response.data)
        self.assertEqual([self._contract], list(body['contracts']))
        contract = body['contracts'][self._contract]
        self.assertEqual(1, contract['current'])
        self.assertEqual([1], [version['version'] for version in contract['versions']])
        url = self.base_url.set(path=('contracts', self._contract, '1'))
        self.assertEqual([str(url)], [version['url'] for version in contract['versions']])
        response = self._get('/contracts/no_such_contract')
        self.assertEqual(404, response.status)

    def test_get_contract(self):
        response = self._get(f'/contracts/{self._contract}/1')
        self.assertEqual(200, response.status)
        self.assertEqual('text/markdown; charset=utf-8',
                         response.headers['Content-Type'])
        text = response.data.decode()
        file_name = f'v1-{sha256_hex(text)}.md'
        path = 'static', 'contracts', self._contract, file_name
        self.assertEqual(self._app.load_resource(*path), text)
        self.assertEqual(_content_digest(response.data),
                         response.headers['Content-Digest'])
        self.assertEqual(_etag(1, response.data), response.headers['ETag'])

    def test_contract_digests(self):
        """
        The digest in the file name of every version of every contract must
        match the content of that file
        """
        contracts = json.loads(self._get('/contracts').data)['contracts']
        for name, url in contracts.items():
            response = self._get_url(url)
            versions = json.loads(response.data)['contracts'][name]['versions']
            for version in versions:
                with self.subTest(name=name, version=version):
                    response = self._get_url(version['url'])
                    self.assertEqual(200, response.status)
                    self.assertEqual(version['sha256'],
                                     hashlib.sha256(response.data).hexdigest())
                    self.assertEqual(_content_digest(response.data),
                                     response.headers['Content-Digest'])
                    self.assertEqual(_etag(version['version'], response.data),
                                     response.headers['ETag'])

    def test_get_current_contract(self):
        response = self._get(f'/contracts/{self._contract}/current')
        self.assertEqual(302, response.status)
        url = self.base_url.set(path=('contracts', self._contract, '1'))
        self.assertEqual(str(url), response.headers['Location'])

    def test_get_contract_invalid(self):
        for version, status in [
            ('2', 404),
            ('0', 400),
            ('01', 400),
            ('-1', 400),
            ('latest', 400)
        ]:
            with self.subTest(version=version):
                response = self._get(f'/contracts/{self._contract}/{version}')
                self.assertEqual(status, response.status)
        for version in ['1', 'current']:
            with self.subTest(version=version):
                response = self._get(f'/contracts/no_such_contract/{version}')
                self.assertEqual(404, response.status)

    def test_versions_from_files(self):
        with self._mock_contract_files(self._contract_files):
            response = self._get(f'/contracts/{self._contract}')
            self.assertEqual(200, response.status)
            expected = {
                'contracts': {
                    self._contract: {
                        'versions': self._versions((1, 'One'), (2, 'Two'), (4, 'Four')),
                        'current': 2
                    }
                }
            }
            self.assertEqual(expected, json.loads(response.data))

    def test_get_contract_from_files(self):
        with self._mock_contract_files(self._contract_files):
            for version, text in [(1, 'One'), (2, 'Two'), (4, 'Four')]:
                with self.subTest(version=version):
                    response = self._get(f'/contracts/{self._contract}/{version}')
                    self.assertEqual(200, response.status)
                    self.assertEqual(text, response.data.decode())
                    self.assertEqual(_content_digest(response.data),
                                     response.headers['Content-Digest'])
                    self.assertEqual(_etag(version, response.data),
                                     response.headers['ETag'])
            for version in ['3', '5', '6']:
                with self.subTest(version=version):
                    response = self._get(f'/contracts/{self._contract}/{version}')
                    self.assertEqual(404, response.status)
            response = self._get(f'/contracts/{self._contract}/current')
            self.assertEqual(302, response.status)
            url = self.base_url.set(path=('contracts', self._contract, '2'))
            self.assertEqual(str(url), response.headers['Location'])
            # The response to the redirect identifies the current version
            response = self._get_url(response.headers['Location'])
            self.assertEqual(_etag(2, response.data), response.headers['ETag'])

    def test_missing_current_version(self):
        files = {**self._contract_files, 'current': '3\n'}
        with self._mock_contract_files(files):
            for path in [
                f'/contracts/{self._contract}',
                f'/contracts/{self._contract}/current',
                f'/contracts/{self._contract}/1'
            ]:
                with self.subTest(path=path):
                    response = self._get(path)
                    self.assertEqual(500, response.status)

    def test_digest_mismatch(self):
        file_name, text = contract_file(1, 'One')
        files = {**self._contract_files, file_name: 'Uno'}
        with self._mock_contract_files(files):
            # Listing the versions doesn't read the files
            response = self._get(f'/contracts/{self._contract}')
            self.assertEqual(200, response.status)
            response = self._get(f'/contracts/{self._contract}/1')
            self.assertEqual(500, response.status)
            response = self._get(f'/contracts/{self._contract}/2')
            self.assertEqual(200, response.status)

    def test_duplicate_version(self):
        files = dict([*self._contract_files.items(), contract_file(1, 'Uno')])
        with self._mock_contract_files(files):
            for path in [
                f'/contracts/{self._contract}',
                f'/contracts/{self._contract}/current',
                f'/contracts/{self._contract}/2'
            ]:
                with self.subTest(path=path):
                    response = self._get(path)
                    self.assertEqual(500, response.status)
