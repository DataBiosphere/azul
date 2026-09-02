"""
Downloads the license files for each of the python packages locked in
`uv.lock`.

Recommended usage when updating the current set of license files:

1) Move the existing license files out of the destination path and into a
   temporary location.
2) Run this script to download fresh copies of the license files.
3) Using the old license files for reference, manually download the licenses
   for the python packages that this script failed to locate.
4) Delete the old license files.
"""
import argparse
import json
import logging
import sys
import time
import tomllib
from typing import (
    Sequence,
)

from furl import (
    furl,
)
from urllib3 import (
    HTTPResponse,
)

from azul import (
    config,
)
from azul.args import (
    AzulArgumentHelpFormatter,
)
from azul.http import (
    http_client,
)
from azul.lib import (
    cached_property,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)


class Main:
    destination_path = f'{config.project_root}/docs/licenses/python/'

    file_names = [
        'LICENSE',
        'LICENSE.txt',
        'LICENSE.rst',
        'LICENSE.md',
        'LICENSE.mit',
        'COPYING',
        'COPYING.BSD',
        'LICENCE',
        'LICENCE.md'
    ]

    @cached_property
    def http(self):
        return http_client()

    def main(self, argv: list[str]):
        parser = argparse.ArgumentParser(description=__doc__,
                                         formatter_class=AzulArgumentHelpFormatter)
        parser.add_argument('--package', '-p',
                            help='Optionally specify one or more packages to '
                                 'download from. If not specified, licenses from '
                                 'all Python dependencies will be downloaded.',
                            nargs='+',
                            metavar='PACKAGE',
                            )
        parser.add_argument('--debug',
                            action='store_true',
                            help='Log debugging information')
        args = parser.parse_args(argv)

        packages = []
        failures = []

        if args.package:
            packages = [p for p in args.package]
        else:
            with open(f'{config.project_root}/uv.lock', 'rb') as f:
                lock = tomllib.load(f)
            packages = [package['name'] for package in lock['package']]

        for package in packages:
            found = False
            pypi_url = f'https://pypi.org/pypi/{package}/json'
            response = self.fetch(pypi_url)
            assert isinstance(response, HTTPResponse)
            # Not all requirements are found on pypi (e.g. resumablehash)
            if response.status == 200:
                urls = json.loads(response.data)['info']['project_urls']
                urls = [] if urls is None else self.github_urls(urls.values())
                for url in urls:
                    url_raw = furl(url)
                    if len(url_raw.path.segments) > 2:
                        if url_raw.path.segments[2] in ('blob', 'tree'):
                            url_raw.path.segments[2] = 'raw'
                    else:
                        url_raw.path.segments.extend(['raw', 'HEAD'])
                    url_blob = url_raw.copy()
                    url_blob.path.segments[2] = 'blob'
                    for filename in self.file_names:
                        response = self.fetch(f'{url_raw}/{filename}')
                        assert isinstance(response, HTTPResponse)
                        if response.status == 200:
                            file_path = f'{self.destination_path}{package}.txt'
                            with open(file_path, 'wb') as f:
                                f.write(f'{url_blob}/{filename}\n\n'.encode('ascii'))
                                f.write(response.data)
                            log.info('%s... SUCCESS', package)
                            found = True
                            break
                    if found:
                        break
            if not found:
                failures.append(package)
                log.info('%s... FAIL (%s)', package, pypi_url)

        if failures:
            log.error('Failed to fetch licenses for packages: %s', failures)

    def github_urls(self, urls: Sequence[str]) -> list[str]:
        """
        Return URLs to GitHub project home directories found in the URLs given.
        """
        urls_: set[str] = set()
        for url in urls:
            url_ = furl(url).remove(args=True, fragment=True)
            if url_.netloc == 'github.com':
                if url_.path.segments and url_.path.segments[-1] == '':
                    url_.path.segments.pop()
                if url_.path.segments:
                    last_segment = url_.path.segments[-1]
                    if last_segment.endswith('.git'):
                        url_.path.segments[-1] = last_segment.removesuffix('.git')
                    elif last_segment.startswith('README'):
                        url_.path.segments.pop()
                    # Note we can't just chop segments at [:2] due to projects like:
                    # https://github.com/googleapis/google-cloud-python/blob/main/packages/google-cloud-bigquery-reservation
                    elif (
                        len(url_.path.segments) == 3
                        and last_segment in ('discussions', 'issues', 'pulls', 'wiki')
                    ):
                        url_.path.segments.pop()
                    urls_.add(str(url_))
        return sorted(urls_)

    def fetch(self, url: str) -> HTTPResponse:
        while True:
            response = self.http.request('GET', url)
            if response.status in [301, 302]:
                url = response.get_redirect_location()
                retry_after = response.headers.get('Retry-After')
                if retry_after is not None:
                    print('Sleeping %.3fs to honor Retry-After property' % retry_after)
                    log.info(f'Sleeping {retry_after:.3fs} to honor Retry-After property')
                    time.sleep(retry_after)
            else:
                return response


if __name__ == '__main__':
    configure_script_logging(log)
    Main().main(sys.argv[1:])
