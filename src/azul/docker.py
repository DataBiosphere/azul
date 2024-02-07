from base64 import (
    urlsafe_b64encode,
)
from collections import (
    defaultdict,
)
from hashlib import (
    sha1,
)
import json
import logging
import os
import subprocess
from typing import (
    Iterable,
    Optional,
    Self,
    TypedDict,
)

import attr
import attrs
from dxf import (
    DXF,
    DXFBase,
)
from furl import (
    furl,
)
from more_itertools import (
    one,
    padded,
)
import requests

from azul import (
    cached_property,
    config,
    require,
)
from azul.types import (
    JSONs,
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
        The name of the image, starting with the registry, up to, but not
        including, the tag.
        """
        return '/'.join([self.registry, self.relative_name])

    @property
    def relative_name(self):
        """
        The name of the image relative to the registry.
        """
        return '/'.join([self.username, *self.repository])

    @property
    def registry_host(self):
        """
        Same as :py:attr:``registry`` with hacks for DockerHub.

        https://github.com/docker/cli/issues/3793#issuecomment-1269051403
        """
        registry = self.registry
        return 'registry-1.docker.io' if registry == 'docker.io' else registry

    @property
    def tf_repository(self):
        """
        A string suitable for identifying (in Terraform config) the ECR
        repository resource holding this image.
        """
        hash = urlsafe_b64encode(sha1(self.name.encode()).digest()).decode()[:-1]
        return 'repository_' + hash

    @property
    def tf_alnum_repository(self):
        """
        An alphanumeric string suitable for identifying (in Terraform config)
        the ECR repository resource holding this image. Unlike `tf_repository`,
        the string may only contain characters in [0-9a-zA-Z].
        """
        return 'repository' + sha1(self.name.encode()).hexdigest()

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

    def normalize(self) -> Self:
        os = _normalize_os(self.os)
        arch, variant = _normalize_arch(self.arch, self.variant)
        return Platform(os=os, arch=arch, variant=variant)

    @classmethod
    def parse(cls, platform: str) -> Self:
        os, arch, variant = padded(platform.split('/'), None, 3)
        require(os, 'Invalid operating system in Docker platform', platform)
        require(arch, 'Invalid architecture in Docker platform', platform)
        require(variant or variant is None, 'Invalid variant in Docker platform', platform)
        return Platform(os=os, arch=arch, variant=variant)

    @classmethod
    def from_json(cls, platform) -> Self:
        return cls(os=platform['os'],
                   arch=platform['architecture'],
                   variant=platform.get('variant'))

    def __str__(self) -> str:
        result = [self.os, self.arch]
        if self.variant is not None:
            result.append(self.variant)
        return '/'.join(result)


images_by_alias = {
    alias: ImageRef.parse(spec['ref'])
    for alias, spec in config.docker_images.items()
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
        Platform.from_json(p).normalize()
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


class Artifact(TypedDict):
    """
    An manifest or a blob, something that can be hashed.
    """

    #: A hash of the content, most likely starting in `sha256:`
    digest: str


class Manifest(Artifact):
    """
    A Docker image
    """
    #: Type of system to run the image on, as in `os/arch` or `os/arch/variant`
    platform: str

    #: The hash of the image config JSON, most likely starting in `sha256:`.
    #: This is consistent accross registries and includes the hashes of the
    #: uncompressed, binary content of the image, and is commonly referred to as
    #: the "image ID".
    id: str


class ManifestList(Artifact):
    """
    A multi-platform image, also known as an image index
    """
    #: The images in the list, by platform (`os/arch` or `os/arch/variant`)
    manifests: dict[str, Manifest]


@attrs.define(frozen=True, slots=False)
class Repository:
    host: str
    name: str

    @classmethod
    def get_manifests(cls):
        manifests: dict[str, Manifest | ManifestList] = {}
        for alias, ref in images_by_alias.items():
            log.info('Getting information for %r (%s)', alias, ref)
            repository = Repository(host=ref.registry_host,
                                    name=ref.relative_name)
            digest = repository.get_tag(ref.tag)
            manifests[str(ref)] = repository.get_manifest(digest)
        return manifests

    def get_tag(self, tag: str) -> str:
        """
        Return the manifest digest associated with the given tag.
        """
        log.info('Getting tag %r', tag)
        digest, _ = self._client.head_manifest_and_response(tag)
        return digest

    def get_manifest(self, digest: str) -> Manifest | ManifestList:
        """
        Return the manifest for the given digest.
        """
        log.info('Getting manifest %r', digest)
        manifest, _ = self._client.get_manifest_and_response(digest)
        manifest = json.loads(manifest)
        match manifest['mediaType']:
            case ('application/vnd.oci.image.index.v1+json'
                  | 'application/vnd.docker.distribution.manifest.list.v2+json'):
                return {
                    'digest': digest,
                    'manifests': self._get_manifests(manifest['manifests'])
                }
            case ('application/vnd.docker.distribution.manifest.v2+json'
                  | 'application/vnd.oci.image.manifest.v1+json'):
                config_digest = manifest['config']['digest']
                config = json.loads(self.get_blob(config_digest))
                return {
                    'digest': digest,
                    'id': config_digest,
                    'platform': str(Platform.from_json(config).normalize())
                }
            case media_type:
                raise NotImplementedError(media_type)

    def _get_manifests(self, manifest_list: JSONs):
        manifests: dict[str, Manifest] = {}
        for entry in manifest_list:
            platform = Platform.from_json(entry['platform']).normalize()
            if platform in platforms:
                platform = str(platform)
                digest = entry['digest']
                manifest: Manifest = self.get_manifest(digest)
                require(manifest['platform'] == platform,
                        'Inconsistent platform in config and manifest',
                        entry, manifest)
                manifests[platform] = manifest
        return manifests

    def get_blob(self, digest: str) -> bytes:
        """
        Return the content for the given digest.
        """
        log.info('Getting blob %r', digest)
        chunks = self._client.pull_blob(digest)
        return b''.join(chunks)

    @cached_property
    def _client(self):
        return DXF(host=self.host, repo=self.name, auth=self._auth)

    def _auth(self, dxf: DXFBase, response: requests.Response):
        host: str = furl(response.request.url).host
        if host == 'ghcr.io':
            dxf.authenticate(authorization=self._ghcr_io_auth,
                             response=response)
        elif host.endswith('.docker.io') or host == 'docker.io':
            username, password = self._docker_io_auth
            dxf.authenticate(username=username,
                             password=password,
                             response=response)
        else:
            raise NotImplementedError(host)

    @property
    def _ghcr_io_auth(self) -> str:
        return 'Authorization: ' + os.environ['GITHUB_TOKEN']

    @cached_property
    def _docker_io_auth(self) -> tuple[str, str]:
        with open(os.path.expanduser('~/.docker/config.json')) as f:
            config = json.load(f)
        command = 'docker-credential-' + config['credsStore']
        output = subprocess.check_output(args=[command, 'get'],
                                         input=b'https://index.docker.io/v1/')
        output = json.loads(output)
        return output['Username'], output['Secret']


def resolve_docker_image_for_launch(alias: str) -> str:
    """
    Return an image reference that can be used to launch a container from the
    image with the given alias.
    """
    return config.docker_registry + config.docker_images[alias]['ref']
