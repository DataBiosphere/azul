import os

from azul import (
    config,
    logging,
)
from azul.files import (
    write_file_atomically,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)


def main():
    path = 'environment.boot'
    with open(path) as f:
        lines = f.read().split('\n')
    with write_file_atomically(path) as f:
        for line in lines:
            if line:
                name, _, old = line.partition('=')
                cur = os.environ.get(name)
                if name == 'azul_python_image':
                    new = config.docker_images['python']['ref']
                    if old != cur:
                        log.warning('%r differs between boot (%r) and current (%r) environment. '
                                    'This suggests that the environment was not loaded correctly.',
                                    name, old, cur)
                    if old != new:
                        log.info('Updating %r from %r to %r. '
                                 'You need to run _refresh.',
                                 name, old, new)
                else:
                    assert cur is not None
                    new = cur
                    if old != new:
                        log.info('Updating %r from %r to %r', name, old, new)
                f.write(f'{name}={new}\n')


if __name__ == '__main__':
    configure_script_logging(log)
    main()
