import logging
from pathlib import (
    Path,
)
import subprocess
from sys import (
    argv,
)
from typing import (
    Any,
    ContextManager,
    Iterable,
)

import docker
import posix_ipc

from azul import (
    config,
    require,
)
from azul.docker import (
    ImageRef,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)


def copy_image(src: str):
    src = ImageRef.parse(src)
    dst = ImageRef.create(name=config.docker_registry + src.name,
                          tag=src.tag)
    dst_parts = []
    client = docker.client.from_env()
    for platform in src.platforms:
        output = client.api.pull(src.name,
                                 platform=str(platform),
                                 tag=src.tag,
                                 stream=True)
        log_lines(src, 'pull', output)
        image = client.images.get(str(src))
        dst_part = ImageRef.create(name=config.docker_registry + src.name,
                                   tag=src.tag + '-' + str(platform).replace('/', '-'))
        image.tag(dst_part.name, tag=dst_part.tag)
        output = client.api.push(dst_part.name,
                                 tag=dst_part.tag,
                                 stream=True)
        log_lines(src, 'push', output)
        dst_parts.append(dst_part)

    subprocess.run([
        'docker', 'manifest', 'rm',
        str(dst)
    ], check=False)
    subprocess.run([
        'docker', 'manifest', 'create',
        str(dst),
        *list(map(str, dst_parts))
    ], check=True)
    subprocess.run([
        'docker', 'manifest', 'push',
        str(dst)
    ], check=True)


def log_lines(context: Any, command: str, output: Iterable[bytes]):
    for line in output:
        log.info('%s: docker %s %s', context, command, line.decode().strip())


class Semaphore(ContextManager):
    """
    A semaphore for synchronizing multiple Python programs, or multiple
    instances of a Python program. Not thread-safe. It's a wrapper around
    posix_ipc.Semaphore that handles creation on demand and unlinking more
    succinctly, and that adds logging of important events in the lifecycle the
    semaphore.
    """

    def __init__(self, name: str, initial_value: int) -> None:
        super().__init__()
        self.name = name
        self.initial_value = initial_value
        self.inner = None

    def __enter__(self):
        if self.inner is None:
            log.debug('Creating or opening semaphore %r', self.name)
            self.inner = posix_ipc.Semaphore(self.name,
                                             initial_value=self.initial_value,
                                             flags=posix_ipc.O_CREAT)
        log.info('Acquiring semaphore %r', self.name)
        self.inner.acquire()
        log.info('Acquired semaphore %r', self.name)
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool:
        self.inner.release()
        log.info('Released semaphore %r', self.name)
        return False

    def destroy(self):
        if self.inner is None:
            log.debug('Opening semaphore %r for deletion', self.name)
            semaphore = None
            try:
                semaphore = posix_ipc.Semaphore(self.name)
            except Exception:
                if log.isEnabledFor(logging.DEBUG):
                    log.debug('Failed to open semaphore %s', self.name, exc_info=True)
            finally:
                if semaphore is not None:
                    self._destroy(semaphore)
        else:
            semaphore, self.inner = self.inner, None
            self._destroy(semaphore)

    def _destroy(self, semaphore):
        log.info('Deleting semaphore %r', self.name)
        semaphore.unlink()


def main():
    # Terraform has a default concurrency of 10, which means that there could be
    # as many concurrent invocations of this script. This would overwhelm the
    # local Docker daemon and cause many of those invocations to raise socket
    # timeouts. There is currently no way to limit Terraform's concurrency per
    # resource or resource type, and we don't want to limit it on a global
    # basis, so we have to enforce a concurrency limit here using a semaphore.
    num_args = len(argv)
    require(num_args in (1, 2), 'Must pass zero or one argument', num_args)
    semaphore = Semaphore(name='/' + Path(__file__).stem + '.' + config.deployment_stage,
                          initial_value=1)
    if num_args == 2:
        with semaphore:
            copy_image(argv[1])
    else:
        semaphore.destroy()


if __name__ == '__main__':
    configure_script_logging(log)
    main()
