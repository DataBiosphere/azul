import logging
from pathlib import (
    Path,
)

from furl import (
    furl,
)

from azul import (
    config,
)
from azul.http import (
    http_client,
)
from azul.lib import (
    R,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)
http = http_client(log)

repository_url = 'https://raw.githubusercontent.com/swagger-api/swagger-ui'
tag = 'v5.32.2'
files = [
    'index.html',
    'index.css',
    'swagger-ui.css',
    'swagger-ui-bundle.js',
    'swagger-ui-standalone-preset.js',
    'oauth2-redirect.js',
    'oauth2-redirect.html',
    # We don't directly serve these files, but we maintain verbatim copies from
    # the upstream distribution for reference.
    'swagger-initializer.js'
]

swagger_dir = Path(config.project_root) / 'resources/static/swagger'


def download_file(name: str):
    object_url = furl(repository_url) / tag / 'dist' / name
    response = http.request('GET', str(object_url))
    assert response.status == 200, R(name)
    with open(swagger_dir / name, 'wb') as f:
        f.write(response.data)


def main():
    for file_name in files:
        download_file(file_name)


if __name__ == '__main__':
    configure_script_logging(log)
    main()
