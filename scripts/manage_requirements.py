import argparse
from dataclasses import (
    dataclass,
)
import logging
from pathlib import (
    Path,
)
from typing import (
    Any,
    FrozenSet,
    IO,
    Iterable,
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


class Versions(FrozenSet[Version]):

    def __new__(cls, *versions: Version) -> Any:
        return super().__new__(cls, versions)

    def __str__(self) -> str:
        return ','.join('==' + v for v in self)

    def __or__(self, other: 'Versions') -> 'Versions':
        # We need to hand-implement this because the overridden base class
        # method returns a base class instance, unfortunately.
        return type(self)(*self, *other)


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
    extension: str
    image: Optional[str]


@dataclass
class Main:
    build_image: str
    runtime_image: str

    @cached_property
    def pip(self): return Qualifier('.pip', None)

    @cached_property
    def runtime(self): return Qualifier('', self.runtime_image)

    @cached_property
    def build(self): return Qualifier('.dev', self.build_image)

    @cached_property
    def project_root(self):
        return Path(config.project_root)

    @cached_property
    def docker(self):
        return docker.from_env()

    def run(self):
        pip_deps = self.get_direct_reqs(self.pip)
        direct_runtime_reqs = self.get_direct_reqs(self.runtime)
        direct_build_reqs = self.get_direct_reqs(self.build)
        dupes = direct_build_reqs & direct_runtime_reqs
        require(not dupes, 'Some requirements are declared as both run and build time', dupes)

        build_reqs = self.get_reqs(self.build) - pip_deps
        runtime_reqs = self.get_reqs(self.runtime) - pip_deps
        assert runtime_reqs <= build_reqs
        overlap = build_reqs & runtime_reqs
        ambiguities = PinnedRequirements(req for req in overlap if len(req.versions) > 1)
        for req in ambiguities:
            build_req = build_reqs[req]
            log.warning('Pinning transitive runtime requirement %s to %s, the '
                        'version resolved at build time.', req, build_req.versions)
            runtime_reqs[req] = build_req

        build_only_reqs = build_reqs - runtime_reqs
        transitive_build_reqs = build_only_reqs - direct_build_reqs
        transitive_runtime_reqs = runtime_reqs - direct_runtime_reqs
        assert not transitive_build_reqs & transitive_runtime_reqs
        self.write_transitive_reqs(transitive_build_reqs, self.build)
        self.write_transitive_reqs(transitive_runtime_reqs, self.runtime)

    def parse_reqs(self, file_or_str: Union[IO, str]) -> PinnedRequirements:
        parsed_reqs = requirements.parse(file_or_str, recurse=False)
        parsed_reqs = set(map(PinnedRequirement.create, parsed_reqs))
        return PinnedRequirements(parsed_reqs - {None})

    def get_reqs(self, qualfier: Qualifier) -> PinnedRequirements:
        command = '.venv/bin/pip freeze --all'
        log.info('Getting direct and transitive requirements by running %s on image %s',
                 command, qualfier.image)
        stdout = self.docker.containers.run(image=qualfier.image,
                                            command=command,
                                            detach=False,
                                            stdout=True,
                                            auto_remove=True)
        return self.parse_reqs(stdout.decode())

    def get_direct_reqs(self, qualifier: Qualifier) -> PinnedRequirements:
        file_name = f'requirements{qualifier.extension}.txt'
        path = self.project_root / file_name
        log.info('Reading direct requirements from %s', path)
        with open(path) as f:
            return self.parse_reqs(f)

    def write_transitive_reqs(self, reqs: PinnedRequirements, qualifier: Qualifier) -> None:
        file_name = f'requirements{qualifier.extension}.trans.txt'
        path = self.project_root / file_name
        log.info('Writing transitive requirements to %s', path)
        with open(path, 'w') as f:
            f.writelines(sorted(f'{req}\n' for req in reqs))


if __name__ == '__main__':
    configure_script_logging(log)
    parser = argparse.ArgumentParser()
    parser.add_argument('--image', required=True)
    parser.add_argument('--build-image', required=True)
    options = parser.parse_args()
    main = Main(build_image=options.build_image,
                runtime_image=options.image)
    main.run()
