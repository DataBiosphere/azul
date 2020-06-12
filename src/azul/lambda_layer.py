from functools import cached_property
import logging
from pathlib import Path
import shutil
import subprocess
from zipfile import (
    ZipFile,
    ZipInfo,
)

import boto3

from azul import (
    config,
)
from azul.files import file_sha1

log = logging.getLogger(__name__)


class DependenciesLayer:
    layer_dir = Path(config.project_root) / 'lambdas' / 'layer'
    out_dir = layer_dir / '.chalice' / 'terraform'

    @cached_property
    def s3(self):
        return boto3.client('s3')

    def _update_required(self) -> bool:
        log.info('Checking if layer package needs updating ...')
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

    def update_layer(self, force: bool = False):
        log.info('Using dependencies layer package at s3://%s/%s.', config.lambda_layer_bucket, self.object_key)
        if force or self._update_required():
            log.info('Staging layer package ...')
            input_zip = self.out_dir / 'deployment.zip'
            layer_zip = self.out_dir / 'layer.zip'
            if force:
                log.info('Tainting current lambda layer resource to force update')
                command = ['make', 'taint_dependencies_layer']
                subprocess.run(command, cwd=Path(config.project_root) / 'terraform').check_returncode()
            self._build_package(input_zip, layer_zip)
            log.info('Uploading layer package to S3 ...')
            self.s3.upload_file(str(layer_zip), config.lambda_layer_bucket, self.object_key)
            log.info('Successfully staged updated layer package.')
        else:
            log.info('Layer package already up-to-date.')

    def _build_package(self, input_zip, output_zip):
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

    @cached_property
    def object_key(self):
        path = Path(config.project_root) / 'requirements.txt'
        sha1 = file_sha1(path)
        return f'{config.lambda_layer_key}/{sha1}.zip'
