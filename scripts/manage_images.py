import argparse
import json
import logging
from operator import (
    itemgetter,
)
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
    cast,
)

from more_itertools import (
    chunked,
    one,
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
    DigestImageRef,
    ImageGist,
    ImageIndexManifest,
    ImageIndexPart,
    IndexImageGist,
    Platform,
    Repository,
    TagImageRef,
    get_docker_image_gist,
    get_docker_image_gists,
    platforms,
)
from azul.files import (
    write_file_atomically,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)


def copy_image(src: str):
    src = TagImageRef.parse(src)
    gist = get_docker_image_gist(src)
    try:
        parts = cast(IndexImageGist, gist)['parts']
    except KeyError:
        copy_single_platform_image(src, cast(ImageGist, gist), tag=src.tag)
    else:
        copy_multi_platform_image(src, parts)


def copy_single_platform_image(src: TagImageRef,
                               gist: ImageGist,
                               tag: str,
                               ) -> tuple[DigestImageRef, int]:
    platform = Platform.parse(gist['platform'])
    log.info('Copying image %r for platform %r', src, platform)
    digest = gist['digest']
    dst = TagImageRef.create(name=config.docker_registry + src.name, tag=tag)

    with Repository.temporary_auth_file(dst, src) as auth_file:
        command = [
            'skopeo', 'copy', '--authfile', auth_file, '--preserve-digests',
            'docker://' + str(src.with_digest(digest)),
            'docker://' + str(dst)
        ]
        subprocess.run(command, check=True)

    response = aws.ecr.batch_get_image(repositoryName=dst.relative_name,
                                       imageIds=[{'imageDigest': digest, 'imageTag': dst.tag}])
    dst_manifest_str = one(response['images'])['imageManifest']
    dst_manifest = json.loads(dst_manifest_str)
    image_id = dst_manifest['config']['digest']
    assert image_id == gist['id']
    size = len(dst_manifest_str)
    return dst.with_digest(digest), size


def copy_multi_platform_image(src: TagImageRef,
                              src_parts: dict[str, ImageGist]
                              ) -> None:
    log.info('Copying all parts of multi-platform image %r …', src)
    dst_parts: dict[Platform, ImageIndexPart] = {}
    for platform, src_part in src_parts.items():
        assert platform == src_part['platform'], (platform, src_part)
        platform = Platform.parse(platform)
        dst_tag = make_platform_tag(src.tag, platform)
        dst_part, size = copy_single_platform_image(src, src_part, tag=dst_tag)
        dst_parts[platform] = ImageIndexPart(digest=dst_part.digest, size=size)
    log.info('Copied all parts (%d in total) of multi-platform image %r',
             len(dst_parts), src)
    dst_manifest = ImageIndexManifest.create(dst_parts)
    dst = TagImageRef.create(name=config.docker_registry + src.name, tag=src.tag)
    try:
        aws.ecr.put_image(repositoryName=dst.relative_name,
                          imageManifest=dst_manifest.json,
                          # ECR computes the digest and compares it against
                          # what we specify here
                          imageDigest=dst_manifest.digest,
                          imageTag=dst.tag)
    except aws.ecr.exceptions.ImageAlreadyExistsException:
        # "The specified image has already been pushed, and there were no
        # changes to the manifest or image tag after the last push"
        #
        # https://docs.aws.amazon.com/AmazonECR/latest/APIReference/API_PutImage.html
        pass
    log.info('Copied multi-platform image with digest %s from %r to %r',
             dst_manifest.digest, src, dst)


def make_platform_tag(tag, platform: Platform):
    reject(is_platform_tag(tag), 'Input already looks like a platform tag', tag)
    return tag + platform_tag_suffix(platform)


def is_platform_tag(tag):
    return any(tag.endswith(platform_tag_suffix(p)) for p in platforms)


def platform_tag_suffix(platform):
    return '-' + str(platform).replace('/', '-')


def delete_unused_images(repository):
    expected_tags = set()
    for ref, gist in get_docker_image_gists().items():
        ref = TagImageRef.parse(ref)
        expected_tags.add(ref.tag)
        try:
            parts = cast(IndexImageGist, gist)['parts']
        except KeyError:
            pass
        else:
            for platform in parts.keys():
                platform = Platform.parse(platform)
                expected_tags.add(make_platform_tag(ref.tag, platform))

    log.info('Listing images in repository %r', repository)
    paginator = aws.ecr.get_paginator('describe_images')
    pages = paginator.paginate(repositoryName=repository)
    unused_images = [
        {
            'image_id': {
                # Typically we only want to remove individual tags and rely on
                # ECR to remove the image when we remove the last tag. If there
                # are any untagged images for whatever reason, we remove them by
                # specifying only their digest.
                **({} if imageTag is None else {'imageTag': imageTag}),
                'imageDigest': image['imageDigest'],
            },
            'is_index': 'artifactMediaType' not in image
        }
        for page in pages
        for image in page['imageDetails']
        for imageTag in image.get('imageTags', [None])
        if imageTag not in expected_tags
    ]
    if unused_images:
        # ECR enforces referential integrity so we have to delete index
        # images before we delete the platform images they refer to.
        groups = reversed(partition(itemgetter('is_index'), unused_images))
        for group in groups:
            batches = chunked(group, 100)  # we can delete at most 100 images at a time
            for batch in batches:
                if batch:
                    if config.terraform_keep_unused:
                        log.info('Would delete images %r from repository %r but '
                                 'deletion of unused resources is disabled',
                                 batch, repository)
                    else:
                        image_ids = list(map(itemgetter('image_id'), batch))
                        log.info('Deleting images %r from repository %r', image_ids, repository)
                        response = aws.ecr.batch_delete_image(repositoryName=repository,
                                                              imageIds=image_ids)
                        reject(bool(response['failures']),
                               'Failed to delete images', response['failures'])
    else:
        log.info('No stale images found, nothing to delete')


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


def update_manifests():
    gists = Repository.get_gists()
    with write_file_atomically(config.docker_image_gists_path) as f:
        json.dump(gists, f, indent=4)


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
    group.add_argument('--update-manifests', action='store_true',
                       help='Update the canned manifests for all images')

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
    elif options.update_manifests:
        update_manifests()
    else:
        assert False, options


if __name__ == '__main__':
    configure_script_logging(log)
    main()
