"""
Downloads the license files for each of the python packages listed in
`requirements.all.txt`.

Recommended usage when updating the current set of license files:

1) Move the existing license files out of the destination path and into a
   temporary location.
2) Run this script to download fresh copies of the license files.
3) Using the old license files for reference, manually download the licenses
   for the python packages that this script failed to locate.
4) Delete the old license files.
"""
import json
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
    require,
)
from azul.http import (
    http_client,
)


def github_urls(urls: Sequence[str]) -> set[str]:
    """
    Return the GitHub URLs from the list of URLs given.
    """
    urls_ = set()
    for url in urls:
        url_ = furl(url.rstrip('/'))
        if url_.netloc == 'github.com':
            last_segment = url_.path.segments[-1] if url_.path.segments else ''
            if last_segment == 'issues':
                # https://github.com/USER/PACKAGE/issues
                url_.path.segments.pop()
            elif last_segment.endswith('.git'):
                # https://github.com/googleapis/proto-plus-python.git
                url_.path.segments[-1] = last_segment[:-4]
            urls_.add(str(url_))
    return urls_


destination_path = f'{config.project_root}/docs/licenses/python/'

license_file_names = [
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

http = http_client()

with open(f'{config.project_root}/requirements.all.txt', 'r') as f:
    lines = f.readlines()

failures = []
for line in lines:
    if line:
        package, version = line.split('==')
        pypi_url = f'https://pypi.org/pypi/{package}'
        response = http.request('GET', f'{pypi_url}/json')
        assert isinstance(response, HTTPResponse)
        require(response.status == 200, response)
        urls = json.loads(response.data)['info']['project_urls']
        found = False
        for url in github_urls(urls.values()):
            for filename in license_file_names:
                response = http.request('GET', f'{url}/raw/HEAD/{filename}')
                assert isinstance(response, HTTPResponse)
                if response.status == 200:
                    file_path = f'{destination_path}{package}.txt'
                    with open(file_path, 'wb') as f:
                        f.write(f'{url}/{filename}\n\n'.encode('ascii'))
                        f.write(response.data)
                    print(package, '... done.')
                    found = True
                    break
            if found:
                break
        else:
            failures.append(package)
            print(package, '... FAIL', pypi_url)

if failures:
    print()
    raise Exception(f'Python package license files not found: {failures}')
