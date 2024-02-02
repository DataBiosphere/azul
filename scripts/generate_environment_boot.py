import os

from azul import (
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
                new = os.environ[name]
                if old != new:
                    log.info('Updating %r from %r to %r', name, old, new)
                f.write(f'{name}={new}\n')


if __name__ == '__main__':
    configure_script_logging(log)
    main()
