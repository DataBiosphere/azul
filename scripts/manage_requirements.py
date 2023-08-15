import argparse
from collections.abc import (
    Iterable,
)
from dataclasses import (
    dataclass,
)
import logging
from pathlib import (
    Path,
)
from typing import (
    Any,
    IO,
    NamedTuple,
    Optional,
    Union,
)

import docker
from more_itertools import (
    one,
)
import requirements
from requirements.requirement import (
    Requirement,
)

from azul import (
    RequirementError,
    cached_property,
    config,
    reject,
    require,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)

Version = str


class Versions(frozenset[Version]):

    def __new__(cls, *versions: Version) -> Any:
        return super().__new__(cls, versions)

    def __str__(self) -> str:
        return ','.join('==' + v for v in sorted(self))

    def __or__(self, other: 'Versions') -> 'Versions':
        # We need to hand-implement this because the overridden base class
        # method returns a base class instance, unfortunately.
        return type(self)(*self, *other)

    def __lt__(self, other):
        # Currently we only support sorting singletons
        return one(self) < one(other)


@dataclass(frozen=True)
class PinnedRequirement:
    name: str
    versions: Versions = Versions()

    @classmethod
    def create(cls, req: Requirement) -> Optional['PinnedRequirement']:
        if req.specifier:
            op, version = one(req.specs)
            assert op == '=='
            return cls(name=req.name.lower(), versions=Versions(version))
        elif req.vcs:
            reject(req.revision is None, 'VCS requirements must carry a specific revision', req)
            return cls(name=req.name.lower())
        elif req.recursive:
            return None
        else:
            raise RequirementError('Unable to handle requirement', req)

    def __or__(self, other: Optional['PinnedRequirement']) -> 'PinnedRequirement':
        assert self.name == other.name
        if self.versions == other.versions:
            return self
        else:
            return PinnedRequirement(name=other.name,
                                     versions=self.versions | other.versions)

    def __lt__(self, other: 'PinnedRequirement'):
        if self.name < other.name:
            return True
        elif self.name == other.name:
            return self.versions < other.versions
        else:
            return False

    def __str__(self) -> str:
        assert self.versions
        return self.name + str(self.versions)


class PinnedRequirements:

    def __init__(self, reqs: Iterable[Optional[PinnedRequirement]]) -> None:
        reqs = list(filter(None, reqs))
        self._reqs = {req.name: req for req in reqs}
        assert len(reqs) == len(self._reqs)

    def __and__(self, other: 'PinnedRequirements') -> 'PinnedRequirements':
        def lookup(req: PinnedRequirement) -> Optional[PinnedRequirement]:
            try:
                other_req = other[req]
            except KeyError:
                return None
            else:
                return req | other_req

        return PinnedRequirements(lookup(req) for req in self)

    def __sub__(self, other: 'PinnedRequirements') -> 'PinnedRequirements':
        return PinnedRequirements(req for req in self if req not in other)

    def __le__(self, other: 'PinnedRequirements') -> bool:
        return self._reqs.keys() <= other._reqs.keys()

    def __iter__(self):
        return iter(self._reqs.values())

    def __contains__(self, item: Union[str, PinnedRequirement]):
        return self._name(item) in self._reqs

    def _name(self, item: Union[str, PinnedRequirement]) -> str:
        return item.name if isinstance(item, PinnedRequirement) else item

    def __getitem__(self, item: Union[str, PinnedRequirement]) -> PinnedRequirement:
        return self._reqs[self._name(item)]

    def __setitem__(self, item: Union[str, PinnedRequirement], req: PinnedRequirement) -> None:
        self._reqs[self._name(item)] = req

    def __repr__(self) -> str:
        return f'Requirements({repr(list(self._reqs.values()))})'

    def __str__(self) -> str:
        return '\n'.join(map(str, self._reqs.values()))

    def __bool__(self):
        return bool(self._reqs)


class Qualifier(NamedTuple):
    name: str
    extension: str
    image: Optional[str]


@dataclass
class Main:
    build_image: str
    runtime_image: str

    @cached_property
    def pip(self):
        return Qualifier(name='pip',
                         extension='.pip',
                         image=None)

    @cached_property
    def runtime(self):
        return Qualifier(name='runtime',
                         extension='',
                         image=self.runtime_image)

    @cached_property
    def build(self):
        return Qualifier(name='build',
                         extension='.dev',
                         image=self.build_image)

    @cached_property
    def project_root(self):
        return Path(config.project_root)

    @cached_property
    def docker(self):
        return docker.from_env()

    def run(self):
        pip_reqs = self.get_direct_reqs(self.pip)
        direct_runtime_reqs = self.get_direct_reqs(self.runtime)
        direct_build_reqs = self.get_direct_reqs(self.build)
        dupes = direct_build_reqs & direct_runtime_reqs
        require(not dupes, 'Some requirements are declared as both run and build time', dupes)

        all_reqs = self.get_reqs(self.build)
        build_reqs = all_reqs - pip_reqs
        all_runtime_reqs = self.get_reqs(self.runtime)
        runtime_reqs = all_runtime_reqs - pip_reqs
        require(runtime_reqs <= build_reqs,
                'Runtime requirements are not a subset of build requirements',
                runtime_reqs - build_reqs)
        overlap = build_reqs & runtime_reqs
        ambiguities = PinnedRequirements(req for req in overlap if len(req.versions) > 1)
        for req in ambiguities:
            build_req = build_reqs[req]
            # We can't resolve these ambiguities automatically because different
            # versions of a package may have different dependencies in and of
            # themselves, so pinning just the dependency in question might omit
            # some of its dependencies. By pinning it explicitly the normal
            # dependency resolution kicks in, including all transitive
            # dependencies of the pinned version.
            log.error('Ambiguous version of transitive runtime requirement %s. '
                      'Consider pinning it to the version used at build time (%s).',
                      req, build_req.versions)
        require(not ambiguities,
                'Ambiguous transitive runtime requirement versions',
                ambiguities)

        build_only_reqs = build_reqs - runtime_reqs
        transitive_build_reqs = build_only_reqs - direct_build_reqs
        transitive_runtime_reqs = runtime_reqs - direct_runtime_reqs
        assert not transitive_build_reqs & transitive_runtime_reqs
        # Assert that all_reqs really includes everything
        for i, reqs in enumerate([
            pip_reqs,
            direct_runtime_reqs,
            transitive_runtime_reqs,
            direct_build_reqs,
            transitive_build_reqs,
            all_runtime_reqs
        ]):
            assert reqs <= all_reqs, (i, reqs - all_reqs)
        self.write_transitive_reqs(transitive_build_reqs, self.build)
        self.write_transitive_reqs(transitive_runtime_reqs, self.runtime)
        self.write_all_reqs(all_reqs)

    def parse_reqs(self, file_or_str: Union[IO, str]) -> PinnedRequirements:
        parsed_reqs = requirements.parse(file_or_str, recurse=False)
        parsed_reqs = set(map(PinnedRequirement.create, parsed_reqs))
        return PinnedRequirements(parsed_reqs - {None})

    def get_reqs(self, qualifier: Qualifier) -> PinnedRequirements:
        # Some major version of pip between 19 and 22 changed the format of the
        # output of `pip freeze` for VCS dependencies. We'll likely have to
        # upgrade the dependency parser to absorb that. For now we'll just use
        # `pip list --format=freeze` which resolves VCS dependencies to the
        # name==version format.
        #
        command = '.venv/bin/pip list --format=freeze'
        docker_command = f'docker run {qualifier.image} {command}'
        log.info('Getting direct and transitive %s requirements using %r',
                 qualifier.name, docker_command)
        stdout = self.docker.containers.run(image=qualifier.image,
                                            command=command,
                                            detach=False,
                                            stdout=True,
                                            auto_remove=True)
        return self.parse_reqs(stdout.decode())

    def get_direct_reqs(self, qualifier: Qualifier) -> PinnedRequirements:
        file_name = f'requirements{qualifier.extension}.txt'
        path = self.project_root / file_name
        log.info('Reading direct %s requirements from %s', qualifier.name, path)
        with open(path) as f:
            return self.parse_reqs(f)

    def write_transitive_reqs(self, reqs: PinnedRequirements, qualifier: Qualifier) -> None:
        self.write_reqs(reqs,
                        file_name=f'requirements{qualifier.extension}.trans.txt',
                        type='transitive')

    def write_all_reqs(self, reqs: PinnedRequirements) -> None:
        self.write_reqs(reqs,
                        file_name='requirements.all.txt',
                        type='all')

    def write_reqs(self, reqs, *, file_name, type):
        path = self.project_root / file_name
        log.info('Writing %s requirements to %s', type, path)
        with open(path, 'w') as f:
            f.writelines(f'{req}\n' for req in sorted(reqs))


if __name__ == '__main__':
    configure_script_logging(log)
    parser = argparse.ArgumentParser()
    parser.add_argument('--image', required=True)
    parser.add_argument('--build-image', required=True)
    options = parser.parse_args()
    main = Main(build_image=options.build_image,
                runtime_image=options.image)
    main.run()
