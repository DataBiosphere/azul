import os

from azul import (
    config,
    logging,
)
from azul.docker import (
    resolve_docker_image_for_pull,
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
                    ref, gist = resolve_docker_image_for_pull('python')
                    # We remove the current registry from the ref, assuming that
                    # it is reintroduced when the variable is used, by
                    # prepending the value of `azul_docker_registry`. If
                    # `azul_docker_registry` is set to a mirror now, it needs to
                    # be set to the same or another mirror then, otherwise the
                    # digest encoded in the image reference may not be valid.
                    # Digests of single-platform images are the same upstream
                    # and in the mirrors. Digests of multi-platform images are
                    # only consistent accross the mirrors. Their digest in an
                    # upstream registry is very likely different. And because
                    # the digest is portable between mirrors, it does not matter
                    # which specific mirror is currently configured, only that
                    # it is a mirror.
                    ref = ref.port_from(config.docker_registry)
                    new = str(ref)
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
                    else:
                        log.info('No change for %r, still set to %r', name, old)
                f.write(f'{name}={new}\n')


if __name__ == '__main__':
    configure_script_logging(log)
    main()
