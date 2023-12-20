from base64 import (
    urlsafe_b64encode,
)
from collections import (
    defaultdict,
)
from hashlib import (
    sha1,
)
import logging
from typing import (
    Iterable,
    Optional,
)

import attr
from more_itertools import (
    one,
    padded,
)

from azul import (
    cached_property,
    config,
    require,
)

log = logging.getLogger(__name__)


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class ImageRef:
    """
    A fully qualified reference to a Docker image. Does not support any
    abbreviations such as omitting the registry (defaulting to ``docker.io``),
    username (defaulting to ``library``) or tag (defaulting to ``latest``).
    """
    #: The part before the first slash. This is usually the domain name of image
    #: registry e.g., ``"docker.io"``
    registry: str

    #: The part between the first and second slash. This is usually the name of
    #: the user or organisation owning the image. It can also be a generic term
    #: such as ``"library"``.
    username: str

    #: The part after the second slash, split on the remaining slashes. Will
    #: have at least one element.
    repository: list[str]

    #: The part after the colon. This is the name of a tag assigned to the
    #: image.
    tag: str

    @classmethod
    def parse(cls, image_ref: str) -> 'ImageRef':
        name, tag = image_ref.split(':')
        return cls.create(name, tag)

    @classmethod
    def create(cls, name: str, tag: str) -> 'ImageRef':
        registry, username, *repository = name.split('/')
        return cls(registry=registry,
                   username=username,
                   repository=repository,
                   tag=tag)

    def __str__(self) -> str:
        """
        The inverse of :py:meth:`parse`.
        """
        return self.name + ':' + self.tag

    @property
    def name(self):
        """
        The part before the first colon i.e., everything except the tag.
        """
        return '/'.join([self.registry, self.username, *self.repository])

    @property
    def tf_repository(self):
        """
        A string suitable for identifying (in Terraform config) the ECR
        repository resource holding this image.
        """
        hash = urlsafe_b64encode(sha1(self.name.encode()).digest()).decode()[:-1]
        return 'repository_' + hash

    @property
    def tf_image(self):
        """
        A string suitable for identifying (in Terraform config) any resource
        specific to this image.
        """
        hash = urlsafe_b64encode(sha1(str(self).encode()).digest()).decode()[:-1]
        return 'image_' + hash

    @cached_property
    def platforms(self) -> set['Platform']:
        """
        The set of relevant platforms this image is available for. A relevant
        platform is one that is listed in :py:attr:`config.docker_platforms`.
        This method uses the Docker client library to inspect the image in the
        registry that hosts it, via the Docker daemon the client is configured
        to use.
        """
        return _filter_platforms(self, platforms)


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class Platform:
    os: str
    arch: str
    variant: Optional[str]

    def normalize(self) -> 'Platform':
        os = _normalize_os(self.os)
        arch, variant = _normalize_arch(self.arch, self.variant)
        return Platform(os=os, arch=arch, variant=variant)

    @classmethod
    def parse(cls, platform: str) -> 'Platform':
        os, arch, variant = padded(platform.split('/'), None, 3)
        require(os, 'Invalid operating system in Docker platform', platform)
        require(arch, 'Invalid architecture in Docker platform', platform)
        require(variant or variant is None, 'Invalid variant in Docker platform', platform)
        return Platform(os=os, arch=arch, variant=variant)

    def __str__(self) -> str:
        result = [self.os, self.arch]
        if self.variant is not None:
            result.append(self.variant)
        return '/'.join(result)


images_by_alias = {
    alias: ImageRef.parse(name)
    for alias, name in config.docker_images.items()
}

images = images_by_alias.values()

platforms = list(map(Platform.parse, config.docker_platforms))

images_by_name: dict[str, list] = defaultdict(list)
for image in images:
    images_by_name[image.name].append(image)
del image

images_by_tf_repository: dict[tuple[str, str], list[ImageRef]] = (lambda: {
    (name, one(set(image.tf_repository for image in images))): images
    for name, images in images_by_name.items()
})()


def _filter_platforms(image: ImageRef, allowed_platforms: Iterable[Platform]) -> set[Platform]:
    import docker
    allowed_platforms = {p.normalize() for p in allowed_platforms}
    log.info('Distribution for image %r …', image)
    api = docker.client.from_env().api
    dist = api.inspect_distribution(str(image))
    actual_platforms = {
        Platform(os=p['os'], arch=p['architecture'], variant=p.get('variant')).normalize()
        for p in dist['Platforms']
    }
    matching_platforms = allowed_platforms & actual_platforms
    log.info('     … declares matching platforms %r', matching_platforms)
    return matching_platforms


# https://github.com/containerd/containerd/blob/1fbd70374134b891f97ce19c70b6e50c7b9f4e0d/platforms/database.go#L62

def _normalize_os(os: str) -> str:
    os = os and os.lower()
    if os == 'macos':
        os = 'darwin'
    return os


# https://github.com/containerd/containerd/blob/1fbd70374134b891f97ce19c70b6e50c7b9f4e0d/platforms/database.go#L76

def _normalize_arch(arch: str,
                    variant: Optional[str]
                    ) -> tuple[str, Optional[str]]:
    arch = arch.lower()
    variant = variant and variant.lower()
    if arch == 'i386':
        arch = '386'
        variant = None
    elif arch in ('x86_64', 'x86-64', 'amd64'):
        arch = 'amd64'
        if variant == 'v1':
            variant = None
    elif arch in ('aarch64', 'arm64'):
        arch = 'arm64'
        if variant in ('8', 'v8'):
            variant = None
    elif arch == 'armhf':
        arch = 'arm'
        variant = 'v7'
    elif arch == 'armel':
        arch = 'arm'
        variant = 'v6'
    elif arch == 'arm':
        if variant in (None, '7'):
            variant = 'v7'
        elif variant in ('5', '6', '8'):
            variant = 'v' + variant
    return arch, variant
