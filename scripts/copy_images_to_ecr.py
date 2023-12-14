import argparse
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
from more_itertools import (
    partition,
)
import posix_ipc

from azul import (
    config,
    reject,
)
from azul.args import (
    AzulArgumentHelpFormatter,
)
from azul.deployment import (
    aws,
)
from azul.docker import (
    ImageRef,
    Platform,
    images,
    platforms,
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
        log.info('Pulling image %r for platform %r …', src, platform)
        output = client.api.pull(src.name,
                                 platform=str(platform),
                                 tag=src.tag,
                                 stream=True)
        log_lines(src, 'pull', output)
        log.info('Pulled image %r for platform %r', src, platform)
        image = client.images.get(str(src))
        dst_part = ImageRef.create(name=config.docker_registry + src.name,
                                   tag=make_platform_tag(src.tag, platform))
        image.tag(dst_part.name, tag=dst_part.tag)
        log.info('Pushing image %r', dst_part)
        output = client.api.push(dst_part.name,
                                 tag=dst_part.tag,
                                 stream=True)
        log_lines(src, 'push', output)
        log.info('Pushed image %r', dst_part)
        dst_parts.append(dst_part)

    log.info('Creating manifest image %r', dst)
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
    log.info('Created manifest image %r', dst)


def make_platform_tag(tag, platform: Platform):
    reject(is_platform_tag(tag), 'Input already looks like a platform tag', tag)
    return tag + platform_tag_suffix(platform)


def is_platform_tag(tag):
    return any(tag.endswith(platform_tag_suffix(p)) for p in platforms)


def platform_tag_suffix(platform):
    return '-' + str(platform).replace('/', '-')


def delete_unused_images(repository):
    # The set of expected tags computed below is an upper bound. Not every
    # platform is available for every image in the source repository the image
    # was copied from. If a bug causes the ECR repository to end up with an
    # image tagged with a platform that didn't exist in the source repository,
    # this function will not delete that image. We're accepting that improbable
    # outcome in return for not having to list images in the source repository.
    expected_tags = set(
        image.tag if platform is None else make_platform_tag(image.tag, platform)
        for image in images
        for platform in [None, *platforms]  # None for the multi-platform index image
        if image.name == repository
    )
    log.info('Listing images in repository %r', repository)
    image_ids = aws.ecr.list_images(repositoryName=repository)['imageIds']
    unused_image_ids = [
        image_id
        for image_id in image_ids
        if image_id.get('imageTag') not in expected_tags
    ]
    if unused_image_ids:
        # ECR enforces referential integrity so we have to delete index
        # images before we delete the platform images they refer to.
        def is_platform_image(image_id):
            return is_platform_tag(image_id['imageTag'])

        batches = map(list, partition(is_platform_image, unused_image_ids))
        for batch in batches:
            if batch:
                if config.terraform_keep_unused:
                    log.info('Would delete images %r from repository %r but '
                             'deletion of unused resources is disabled',
                             batch, repository)
                else:
                    log.info('Deleting images %r from repository %r', batch, repository)
                    response = aws.ecr.batch_delete_image(repositoryName=repository,
                                                          imageIds=batch)
                    reject(bool(response['failures']),
                           'Failed to delete images', response['failures'])
    else:
        log.info('No stale images found, nothing to delete')


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
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=AzulArgumentHelpFormatter)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--copy', nargs='+', metavar='IMAGE',
                       help='Copy the given fully qualified image to ECR.')
    group.add_argument('--cleanup', action='store_true',
                       help='Clean up. Use this after all invocations with --copy have exited.')
    group.add_argument('--delete-unused', nargs='+', metavar='REPOSITORY',
                       help='Delete unused images and tags from the given ECR image repository.')
    options = parser.parse_args(argv[1:])

    if options.copy or options.cleanup:
        # Terraform has a default concurrency of 10, which means that there could be
        # as many concurrent invocations of this script. This would overwhelm the
        # local Docker daemon and cause many of those invocations to raise socket
        # timeouts. There is currently no way to limit Terraform's concurrency per
        # resource or resource type, and we don't want to limit it on a global
        # basis, so we have to enforce a concurrency limit here using a semaphore.
        semaphore = Semaphore(name='/' + Path(__file__).stem + '.' + config.deployment_stage,
                              initial_value=1)
        if options.copy:
            with semaphore:
                for image in options.copy:
                    copy_image(image)
        else:
            assert options.cleanup, options
            log.info('Deleting semaphore at %r', semaphore.name)
            semaphore.destroy()
    elif options.delete_unused:
        for repository in options.delete_unused:
            delete_unused_images(repository)
    else:
        assert False, options


if __name__ == '__main__':
    configure_script_logging(log)
    main()
