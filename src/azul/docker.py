from abc import (
    ABCMeta,
    abstractmethod,
)
from base64 import (
    b64decode,
    b64encode,
    urlsafe_b64encode,
)
from collections import (
    defaultdict,
)
from contextlib import (
    contextmanager,
)
from hashlib import (
    sha1,
    sha256,
)
import json
import logging
import os
import re
import subprocess
import tempfile
from typing import (
    Any,
    Iterable,
    Literal,
    Optional,
    Self,
    TypedDict,
    cast,
)

import attrs
import docker
from docker.models.images import (
    Image,
)
from dxf import (
    DXF,
    DXFBase,
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


@attrs.define(frozen=True)
class ImageRef(metaclass=ABCMeta):
    """
    A fully qualified reference to a Docker image in a registry.

    Does not support any abbreviations such as omitting the registry (defaulting
    to ``docker.io``), username (defaulting to ``library``) or tag (defaulting
    to ``latest``).
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
    repository: tuple[str]

    @classmethod
    def parse(cls, image_ref: str) -> Self:
        """
        >>> ImageRef.parse('2@1')
        DigestImageRef(registry='docker.io', username='library', repository=('2',), digest='1')
        >>> ImageRef.parse('3/2:1')
        TagImageRef(registry='docker.io', username='3', repository=('2',), tag='1')
        >>> ImageRef.parse('4/3/2:1')
        TagImageRef(registry='4', username='3', repository=('2',), tag='1')
        >>> ImageRef.parse('5/4/3/2:1')
        TagImageRef(registry='5', username='4', repository=('3', '2'), tag='1')
        >>> ImageRef.parse('localhost:5000/docker.io/ucscgi/azul-pycharm:2023.3.4-15')
        ... # doctest: +NORMALIZE_WHITESPACE
        TagImageRef(registry='localhost:5000',
                    username='docker.io',
                    repository=('ucscgi', 'azul-pycharm'),
                    tag='2023.3.4-15')
        """
        if '@' in image_ref:
            return DigestImageRef.parse(image_ref)
        else:
            return TagImageRef.parse(image_ref)

    @classmethod
    def _create(cls, name: str, **kwargs) -> Self:
        name = name.split('/')
        if len(name) == 1:
            registry, username, repository = 'docker.io', 'library', name
        elif len(name) == 2:
            registry, (username, *repository) = 'docker.io', name
        elif len(name) > 2:
            registry, username, *repository = name
        else:
            assert False
        # noinspection PyArgumentList
        return cls(registry=registry,
                   username=username,
                   repository=tuple(repository),
                   **kwargs)

    @property
    def name(self):
        """
        The name of the image, starting with the registry, up to, but not
        including, the tag.
        """
        return '/'.join((self.registry, self.relative_name))

    @property
    def relative_name(self):
        """
        The name of the image relative to the registry.
        """
        return '/'.join((self.username, *self.repository))

    @property
    def registry_host(self):
        """
        Same as :py:attr:``registry`` with hacks for DockerHub.

        https://github.com/docker/cli/issues/3793#issuecomment-1269051403
        """
        registry = self.registry
        return 'registry-1.docker.io' if registry == 'docker.io' else registry

    def with_digest(self, digest: str) -> 'DigestImageRef':
        return DigestImageRef.create(self.name, digest)

    def with_tag(self, tag: str) -> 'TagImageRef':
        return TagImageRef.create(self.name, tag)

    ecr_registry_host_re = re.compile(r'[\d]+\.dkr\.ecr\.[^.]+\.amazonaws\.com')

    @property
    def is_mirrored(self) -> bool:
        return self.ecr_registry_host_re.fullmatch(self.registry_host) is not None

    def port_to(self, registry: str) -> Self:
        """
        >>> ref = ImageRef.parse('a/b/c:d')
        >>> ref.port_to('e')
        TagImageRef(registry='e', username='a', repository=('b', 'c'), tag='d')
        >>> ref.port_to('')
        TagImageRef(registry='a', username='b', repository=('c',), tag='d')
        >>> ref.port_to('a')
        ... # doctest: +NORMALIZE_WHITESPACE
        Traceback (most recent call last):
        ...
        azul.RequirementError: ('Reference already ported to registry',
        TagImageRef(registry='a', username='b', repository=('c',), tag='d'),
        'a')
        """
        if registry:
            require(self.registry != registry,
                    'Reference already ported to registry',
                    self, registry)
            return type(self).parse(registry + '/' + str(self))
        else:
            return self

    def port_from(self, registry: str) -> Self:
        """
        >>> ref = ImageRef.parse('a/b/c:d')
        >>> ref.port_to('e').port_from('e')
        TagImageRef(registry='a', username='b', repository=('c',), tag='d')
        >>> ref.port_to('').port_from('')
        TagImageRef(registry='a', username='b', repository=('c',), tag='d')
        >>> ref.port_from('e')
        ... # doctest: +NORMALIZE_WHITESPACE
        Traceback (most recent call last):
        ...
        azul.RequirementError: ('Reference does not use the registry to port from',
        TagImageRef(registry='a', username='b', repository=('c',), tag='d'), 'e')
        """
        if registry:
            require(self.registry == registry,
                    'Reference does not use the registry to port from',
                    self, registry)
            return type(self).parse(str(self).removeprefix(registry + '/'))
        else:
            return self

    @property
    def auth_server_url(self) -> str:
        """
        The Docker client tracks credentials in ~/.docker/config.json using the
        URL or hostname of the server requesting authentication. Similarly, the
        credential helpers expect the same value on stdandard input. This method
        returns that value for this repository.
        """
        if self.registry == 'docker.io':
            return 'https://index.docker.io/v1/'
        else:
            return self.registry_host

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

    @property
    @abstractmethod
    def qualifier(self) -> str:
        raise NotImplementedError


@attrs.define(frozen=True)
class DigestImageRef(ImageRef):
    """
    A fully qualified and stable reference to a Docker image in a registry.
    """

    #: The part after the '@', a hash of the image manifest. While it uniquely
    #: identifies an image within a registry, it is not consistent accross
    #: registries. The same image can have different digests in different
    #: registries.
    digest: str

    @classmethod
    def parse(cls, image_ref: str) -> Self:
        name, digest = image_ref.split('@')
        return cls.create(name, digest)

    @classmethod
    def create(cls, name: str, digest: str) -> Self:
        return super()._create(name, digest=digest)

    def __str__(self) -> str:
        """
        The inverse of :py:meth:`parse`.
        """
        return self.name + '@' + self.digest

    @property
    def qualifier(self) -> str:
        return self.digest


@attrs.define(frozen=True)
class TagImageRef(ImageRef):
    """
    A fully qualified reference to a tagged Docker image in a registry.
    """

    #: The part after the colon in an image name. This is the name of a tag
    #: associated with the image. Tags refer to digests and are mutable. For a
    #: stable references to images in a registry use :py:class:`DigestImageRef`.
    tag: str

    @classmethod
    def parse(cls, image_ref: str) -> Self:
        # A colon in the first part of the name might separate host and port
        name, _, tag = image_ref.rpartition(':')
        return cls.create(name, tag)

    @classmethod
    def create(cls, name: str, tag: str) -> Self:
        return super()._create(name, tag=tag)

    def __str__(self) -> str:
        """
        The inverse of :py:meth:`parse`.
        """
        return self.name + ':' + self.tag

    @property
    def qualifier(self) -> str:
        return self.tag


@attrs.define(frozen=True)
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
    def from_json(cls, platform, config: bool = False) -> Self:
        def case(s):
            return s.capitalize() if config else s

        return cls(os=platform[case('os')],
                   arch=platform[case('architecture')],
                   variant=platform.get(case('variant')))

    def __str__(self) -> str:
        result = [self.os, self.arch]
        if self.variant is not None:
            result.append(self.variant)
        return '/'.join(result)


images_by_alias = {
    alias: TagImageRef.parse(spec['ref'])
    for alias, spec in config.docker_images.items()
}

images = images_by_alias.values()

platforms = list(map(Platform.parse, config.docker_platforms))

images_by_name: dict[str, list] = defaultdict(list)
for image in images:
    images_by_name[image.name].append(image)
del image

images_by_tf_repository: dict[tuple[str, str], list[TagImageRef]] = {
    (name, one(set(image.tf_repository for image in images))): images
    for name, images in images_by_name.items()
}


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


class Gist(TypedDict):
    """
    Represents an image manifest or a blob, or any Docker artifact with a digest
    """

    #: A hash of the content, typically starting in `sha256:`
    digest: str


class ImageGist(Gist):
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


class IndexImageGist(Gist):
    """
    A multi-platform image, also known as an image index
    """
    #: While the inherited ``digest`` property pertains to the original
    #: registry, this property contains the digest of the image in the mirror
    #: registry, i.e. ECR.  Even though the digests of the platform-specific
    #: parts of a multi-platform image are the same in both registries, the
    #: digest of the mirrored multi-platform image usually differs from the
    #: original because 1) the mirror only includes a subset of the original
    #: parts and 2) the digest algorithm is generally sensitive to insignificant
    #: JSON differences in whitespace or property order.
    mirror_digest: str

    #: The images in the list, by platform (`os/arch` or `os/arch/variant`)
    parts: dict[str, ImageGist]


@attrs.define(frozen=True, slots=False)
class Repository:
    image_ref: ImageRef

    @cached_property
    def host(self) -> str:
        return self.image_ref.registry_host

    @cached_property
    def name(self) -> str:
        return self.image_ref.relative_name

    @classmethod
    def get_gists(cls):
        gists: dict[str, ImageGist | IndexImageGist] = {}
        for alias, ref in images_by_alias.items():
            log.info('Getting information for %r (%s)', alias, ref)
            repository = cls(ref)
            digest = repository.get_tag(ref.tag)
            gists[str(ref)] = repository.get_gist(digest)
        return gists

    def get_tag(self, tag: str) -> str:
        """
        Return the manifest digest associated with the given tag.
        """
        log.info('Getting tag %r', tag)
        digest, _ = self._client.head_manifest_and_response(tag)
        return digest

    def get_gist(self, digest: str) -> ImageGist | IndexImageGist:
        """
        Return the manifest for the given digest.
        """
        log.info('Getting manifest %r', digest)
        manifest, _ = self._client.get_manifest_and_response(digest)
        manifest = json.loads(manifest)
        match manifest['mediaType']:
            case ('application/vnd.oci.image.index.v1+json'
                  | 'application/vnd.docker.distribution.manifest.list.v2+json'):
                parts = self._get_mirrored_parts(manifest['manifests'])
                mirror_manifest = ImageIndexManifest.create({
                    platform: ImageIndexPart(digest=part['digest'], size=size)
                    for platform, (part, size) in parts.items()
                })
                return {
                    'digest': digest,
                    'mirror_digest': mirror_manifest.digest,
                    'parts': {str(platform): part for platform, (part, size) in parts.items()}
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

    def _get_mirrored_parts(self,
                            manifests: JSONs
                            ) -> dict[Platform, tuple[ImageGist, int]]:
        gists = {}
        for manifest in manifests:
            platform = Platform.from_json(manifest['platform']).normalize()
            if platform in platforms:
                digest, size = manifest['digest'], manifest['size']
                gist: ImageGist = self.get_gist(digest)
                require(gist['platform'] == str(platform),
                        'Inconsistent platform between manifest and manifest list',
                        manifest, gist)
                gists[platform] = gist, size
        return gists

    def get_blob(self, digest: str) -> bytes:
        """
        Return the content for the given digest.
        """
        log.info('Getting blob %r', digest)
        chunks = self._client.pull_blob(digest)
        return b''.join(chunks)

    @cached_property
    def _client(self):
        return DXF(host=self.host,
                   repo=self.name,
                   auth=self._dxf_auth,
                   insecure=self.host.startswith('localhost:') or self.host == 'localhost')

    def _dxf_auth(self, dxf: DXFBase, response: requests.Response):
        username, password = self._auth
        dxf.authenticate(username=username,
                         password=password,
                         response=response)

    @cached_property
    def _auth(self) -> tuple[str, str]:
        auth_server_url = self.image_ref.auth_server_url
        with open(os.path.expanduser('~/.docker/config.json')) as f:
            config = json.load(f)
        try:
            creds_store = config['credsStore']
        except KeyError:
            return self._decode_auth(config['auths'][auth_server_url]['auth'])
        else:
            command = 'docker-credential-' + creds_store
            output = subprocess.check_output(args=[command, 'get'],
                                             input=auth_server_url.encode('ascii'))
            output = json.loads(output)
            return output['Username'], output['Secret']

    @property
    def encoded_auth(self) -> str:
        return self._encode_auth(*self._auth)

    def _decode_auth(self, auth: str) -> tuple[str, str]:
        auth = b64decode(auth.encode('ascii')).decode()
        username, _, secret = auth.partition(':')
        return username, secret

    def _encode_auth(self, username: str, secret: str) -> str:
        auth = username + ':' + secret
        return b64encode(auth.encode()).decode('ascii')

    @classmethod
    @contextmanager
    def temporary_auth_file(cls, *refs: ImageRef):
        """
        While some utilities in the Docker/OCI ecosystem are able to read
        plain-text credentials from the Docker client's configuration file
        (~/.docker/config.json), they often lack support for the credential
        helpers that can be configured there. Removing the credStore entry from
        that configiguration file would disable these helpers, but a prominent
        Docker client distribution (Docker Desktop for macOS and Windows)
        reinserts the entry every time it starts up.

        This context manager provides a temporary containers-auth.json [1] with
        plain-text credentials for the repositories hosting the given images.
        The credentials are obtained by extracting plain-text credentials from
        ~/.docker/config.json or by invoking the credStore helper configured
        there. The path to the temporary file is passed to the context on entry
        and the file is deleted when the context is exited.

        [1] https://github.com/containers/image/blob/main/docs/containers-auth.json.5.md
        """
        with tempfile.NamedTemporaryFile() as auth_file:
            auths = {
                'auths': {
                    ref.auth_server_url: {
                        'auth': cls(ref).encoded_auth
                    }
                    for ref in refs
                },
            }
            auth_file.write(json.dumps(auths).encode())
            auth_file.flush()
            yield auth_file.name


@attrs.frozen(kw_only=True)
class ImageIndexPart:
    digest: str
    size: int


@attrs.frozen(kw_only=True)
class ImageIndexManifest:
    json: str
    digest: str

    @classmethod
    def create(cls, parts: dict[Platform, ImageIndexPart]) -> Self:
        manifest = {
            'schemaVersion': 2,
            'mediaType': 'application/vnd.docker.distribution.manifest.list.v2+json',
            'manifests': [
                {
                    'mediaType': 'application/vnd.docker.distribution.manifest.v2+json',
                    'size': part.size,
                    'digest': part.digest,
                    'platform': {
                        'architecture': platform.arch,
                        'os': platform.os
                    }
                }
                for platform, part in parts.items()
            ]
        }
        manifest = json.dumps(manifest, indent=4)
        digest = 'sha256:' + sha256(manifest.encode()).hexdigest()
        return cls(json=manifest, digest=digest)


def pull_docker_image(ref: ImageRef) -> Image:
    return _push_or_pull(ref, 'pull')


def push_docker_image(ref: ImageRef) -> Image:
    return _push_or_pull(ref, 'push')


def _push_or_pull(ref: ImageRef,
                  direction: Literal['push'] | Literal['pull']
                  ) -> Image:
    log.info('%sing image %r …', direction.capitalize(), ref)
    client = docker.client.from_env()
    # Despite its name, the `tag` keyword argument can be a digest, too
    method = getattr(client.api, direction)
    output = method(ref.name, tag=ref.qualifier, stream=True)
    log_lines(ref, direction, output)
    log.info('%sed image %r', direction.capitalize(), ref)
    return client.images.get(str(ref))


def log_lines(context: Any, command: str, output: Iterable[bytes]):
    for line in output:
        log.debug('%s: docker %s %s', context, command, line.decode().strip())


def get_docker_image_gist(ref: TagImageRef) -> ImageGist | IndexImageGist:
    return get_docker_image_gists()[str(ref)]


def get_docker_image_gists() -> dict[str, ImageGist | IndexImageGist]:
    with open(config.docker_image_gists_path) as f:
        return json.load(f)


def resolve_docker_image_for_launch(alias: str) -> str:
    """
    Return an image reference that can be used to launch a container from the
    image with the given alias. The alias is the top level key in the JSON
    object contained in the environment variable `azul_docker_images`.
    """
    ref_to_pull, gist = resolve_docker_image_for_pull(alias)
    image = pull_docker_image(ref_to_pull)
    # In either case, the verification below ensures that the image we pulled
    # has the expected ID.
    try:
        parts = cast(IndexImageGist, gist)['parts']
    except KeyError:
        # For single-platform images, this is straight forward.
        assert image.id == cast(ImageGist, gist)['id']
    else:
        # To determine the expected ID for images that are part of a multi-
        # platform image aka "manifest list" aka "image index", we need to know
        # what specific platform was pulled since we left it to Docker to
        # determine the best match.
        platform = Platform.from_json(image.attrs, config=True).normalize()
        assert image.id == parts[str(platform)]['id']
    # Returning the image ID means that the container will be launched using
    # exactly the image we just pulled and verified.
    return image.id


def resolve_docker_image_for_pull(alias: str
                                  ) -> tuple[DigestImageRef, ImageGist | IndexImageGist]:
    """
    Return a reference to, and the gist of, the image with the given alias, for
    the purpose of pulling said image.
    """
    ref = TagImageRef.parse(config.docker_images[alias]['ref'])
    log.info('Resolving %r image %r …', alias, ref)
    gist = get_docker_image_gist(ref)
    ref = ref.port_to(config.docker_registry)
    # For multi-arch images, we need to use the digest of the mirrored image, if
    # we're pulling from a mirror.  For single-arch images, the digest is the
    # same between the upstream and mirror registries.
    digest = gist[
        'mirror_digest'
        if 'parts' in gist and ref.is_mirrored else
        'digest'
    ]
    ref = ref.with_digest(digest)
    log.info('Resolved %r image to %r', alias, ref)
    return ref, gist
