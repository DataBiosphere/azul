from collections import (
    defaultdict,
)
import hashlib
import logging
from pathlib import (
    Path,
)
import re
import shutil
import subprocess
from typing import (
    Iterable,
)
from zipfile import (
    ZipFile,
    ZipInfo,
)

import attr

from azul import (
    cached_property,
    config,
    require,
)
from azul.deployment import (
    aws,
)
from azul.files import (
    file_sha1,
)

log = logging.getLogger(__name__)


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class Requirement:
    name: str
    version: str
    git: bool

    wheel_re = re.compile(r'(?P<distribution>[^-]+)-'
                          r'(?P<version>[^-]+)-'
                          r'(?:(?P<build_tag>\d[^-]*)-)?'
                          r'(?P<python_tag>[^-]+)-'
                          r'(?P<abi_tag>[^-]+)-'
                          r'(?P<platform_tag>[^-]+).whl')

    @classmethod
    def from_pin(cls, line: str):
        if line.startswith('git+'):
            line = line[len('git+'):]
            version, name = line.split('#egg=')
            return cls(name=name, version=version, git=True)
        else:
            name, version = line.split('==')
            return cls(name=name, version=version, git=False)

    @classmethod
    def from_wheel(cls, wheel: str):
        wheel = cls.wheel_re.fullmatch(wheel)
        return cls(name=wheel['distribution'], version=wheel['version'], git=False)

    def to_pin(self) -> str:
        if self.git:
            return f'git+{self.version}#egg={self.name}'
        else:
            return f'{self.name}=={self.version}'


class DependenciesLayer:
    root = Path(config.project_root)
    lambda_dir = root / 'lambdas'
    reqs_file = root / 'requirements.txt'
    reqs_trans_file = root / 'requirements.trans.txt'
    reqs_pip_file = root / 'requirements.pip.txt'
    reqs_dev_file = root / 'requirements.dev.txt'
    wheel_dir = lambda_dir / '.wheels'
    layer_dir = lambda_dir / 'layer'
    out_dir = layer_dir / '.chalice' / 'terraform'

    @property
    def s3(self):
        return aws.client('s3')

    def _update_required(self) -> bool:
        log.info('Checking for dependencies layer package at s3://%s/%s.',
                 config.lambda_layer_bucket, self.object_key)
        try:
            # Since the object is content-addressed, just checking for the
            # object's presence is sufficient
            self.s3.head_object(Bucket=config.lambda_layer_bucket, Key=self.object_key)
        except self.s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return True
            else:
                raise
        else:
            return False

    def update_layer(self):
        if self._update_required():
            log.info('Generating new layer package ...')
            input_zip = self.out_dir / 'deployment.zip'
            layer_zip = self.out_dir / 'layer.zip'
            self._generate_requirements()
            self._build_package(input_zip, layer_zip)
            self._validate_layer(layer_zip)
            log.info('Uploading layer package to S3 ...')
            self.s3.upload_file(str(layer_zip), config.lambda_layer_bucket, self.object_key)
            log.info('Successfully staged updated layer package.')
        else:
            log.info('Layer package already up-to-date.')

    def _generate_requirements(self):
        log.debug('Generating requirements file for layer ...')
        reqs = self._all_reqs()
        vendored_reqs = [Requirement.from_wheel(w.name)
                         for w in self.wheel_dir.iterdir()]
        log.debug('Filtering out vendored requirements %r', vendored_reqs)
        require(set(vendored_reqs).issubset(reqs), vendored_reqs, reqs)
        # Keep reqs a list to preserve ordering
        non_vendored_reqs = [r for r in reqs if r not in vendored_reqs]
        reqs_by_name = defaultdict(list)
        for req in non_vendored_reqs:
            reqs_by_name[req.name].append(req)
        duplicates = {k: v for k, v in reqs_by_name.items() if len(v) > 1}
        assert not duplicates, duplicates
        with open(self.layer_dir / 'requirements.txt', 'w') as f:
            for req in non_vendored_reqs:
                f.write(req.to_pin() + '\n')

    def _build_package(self, input_zip: Path, output_zip: Path):
        # Delete Chalice's build cache because our layer cache eviction rules
        # are stricter and we want a full rebuild.
        try:
            deployment_dir = self.layer_dir / '.chalice' / 'deployments'
            log.debug("Removing Chalice's deployment cache at %r", str(deployment_dir))
            shutil.rmtree(deployment_dir)
        except FileNotFoundError:
            pass
        command = ['chalice', 'package', self.out_dir]
        log.info('Running %r', command)
        subprocess.run(command, cwd=self.layer_dir).check_returncode()
        log.info('Packaging %s', output_zip)
        with ZipFile(input_zip, 'r') as deployment_zip:
            with ZipFile(output_zip, 'w') as layer_zip:
                for src_zip_info in deployment_zip.infolist():
                    if src_zip_info.filename != 'app.py':
                        # ZipFile doesn't copy permissions. Setting permissions
                        # manually also requires setting other fields.
                        dst_zip_info = ZipInfo(filename=str(Path('python') / src_zip_info.filename))
                        dst_zip_info.external_attr = src_zip_info.external_attr
                        dst_zip_info.date_time = src_zip_info.date_time
                        dst_zip_info.compress_type = src_zip_info.compress_type
                        with deployment_zip.open(src_zip_info, 'r') as rf:
                            with layer_zip.open(dst_zip_info, 'w') as wf:
                                shutil.copyfileobj(rf, wf, length=1024 * 1024)

    def _validate_layer(self, layer_zip: Path):
        with ZipFile(layer_zip, 'r') as z:
            infos = z.infolist()
        files = defaultdict(list)
        for info in infos:
            files[info.filename].append(info)
        duplicates = {k: v for k, v in files.items() if len(v) > 1}
        assert not duplicates, duplicates

    def _all_reqs(self) -> Iterable[Requirement]:
        reqs = []
        for file in (self.reqs_file, self.reqs_trans_file):
            reqs += self._parse_reqs(file)
        return reqs

    def _parse_reqs(self, reqs: Path) -> Iterable[Requirement]:
        with reqs.open('r') as f:
            for line in f:
                if line.startswith('#') or line.startswith('-r'):
                    continue
                else:
                    if ' #' in line:
                        line, *_ = line.split(' #')
                    line = line.strip()
                    yield Requirement.from_pin(line)

    @cached_property
    def object_key(self):
        # We include requirements.txt, requirements.trans.txt, and vendored
        # wheels in the hash to keep the layer content-addressable. The other
        # files don't reference content in the layer, but we include them
        # regardless since they may affect how the layer is generated. For
        # example, Chalice is a dev-requirement, but updating may affect how
        # dependencies are packaged.
        relevant_files = (
            self.reqs_file,
            self.reqs_trans_file,
            *self.wheel_dir.iterdir(),
            self.reqs_pip_file,
            self.reqs_dev_file,
            __file__,
            self.root / 'scripts' / 'stage_layer.py'
        )
        sha1 = hashlib.sha1()
        for file in relevant_files:
            sha1.update(file_sha1(file).encode())
        return f'azul/{config.deployment_stage}/{config.lambda_layer_key}/{sha1.hexdigest()}.zip'
