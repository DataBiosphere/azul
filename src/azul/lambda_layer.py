from collections import (
    defaultdict,
)
import hashlib
import logging
from pathlib import (
    Path,
)
import shutil
import subprocess
from zipfile import (
    ZipFile,
    ZipInfo,
)

from azul import (
    cached_property,
    config,
)
from azul.deployment import (
    aws,
)
from azul.files import (
    file_sha1,
)

log = logging.getLogger(__name__)


class DependenciesLayer:

    @property
    def s3(self):
        return aws.s3

    def _update_required(self) -> bool:
        log.info('Checking for dependencies layer package at s3://%s/%s.',
                 aws.shared_bucket, self.object_key)
        try:
            # Since the object is content-addressed, just checking for the
            # object's presence is sufficient
            self.s3.head_object(Bucket=aws.shared_bucket, Key=self.object_key)
        except self.s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return True
            else:
                raise
        else:
            return False

    layer_dir = Path(config.project_root) / 'lambdas' / 'layer'

    def update_layer(self):
        if self._update_required():
            log.info('Generating new layer package ...')
            out_dir = self.layer_dir / '.chalice' / 'terraform'
            self._build_package(out_dir)
            input_zip = out_dir / 'deployment.zip'
            output_zip = out_dir / 'layer.zip'
            self._filter_package(input_zip, output_zip)
            self._validate_layer(output_zip)
            log.info('Uploading layer package to S3 ...')
            self.s3.upload_file(str(output_zip), aws.shared_bucket, self.object_key)
            log.info('Successfully staged updated layer package.')
        else:
            log.info('Layer package already up-to-date.')

    def _build_package(self, out_dir):
        # Delete Chalice's build cache because our layer cache eviction rules
        # are stricter and we want a full rebuild.
        try:
            cache_dir = self.layer_dir / '.chalice' / 'deployments'
            log.info('Removing deployment cache at %r', str(cache_dir))
            shutil.rmtree(cache_dir)
        except FileNotFoundError:
            pass
        command = ['chalice', 'package', out_dir]
        log.info('Running %r', command)
        subprocess.run(command, cwd=self.layer_dir).check_returncode()

    def _filter_package(self, input_zip_path: Path, output_zip_path: Path):
        """
        Filter a ZIP file, removing `app.py` and prefixingother archive member
        paths with `python/`.
        """
        log.info('Filtering %r to %r', str(input_zip_path), str(output_zip_path))
        with ZipFile(input_zip_path, 'r') as input_zip:
            with ZipFile(output_zip_path, 'w') as output_zip:
                for input in input_zip.infolist():
                    if input.filename != 'app.py':
                        # ZipFile doesn't copy permissions. Setting permissions
                        # manually also requires setting other fields.
                        output = ZipInfo(filename='python/' + input.filename)
                        output.external_attr = input.external_attr
                        output.date_time = input.date_time
                        output.compress_type = input.compress_type
                        with input_zip.open(input, 'r') as rf:
                            with output_zip.open(output, 'w') as wf:
                                shutil.copyfileobj(rf, wf, length=1024 * 1024)

    def _validate_layer(self, layer_zip: Path):
        with ZipFile(layer_zip, 'r') as z:
            infos = z.infolist()
        files = defaultdict(list)
        for info in infos:
            files[info.filename].append(info)
        duplicates = {k: v for k, v in files.items() if len(v) > 1}
        assert not duplicates, duplicates

    @cached_property
    def object_key(self):
        sha1 = hashlib.sha1()
        for path in Path(config.chalice_bin).iterdir():
            sha1.update(file_sha1(path).encode())
        return f'azul/{config.deployment_stage}/{config.lambda_layer_key}/{sha1.hexdigest()}.zip'
