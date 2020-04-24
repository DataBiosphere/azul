import logging
from pathlib import Path
import shutil
import subprocess
from zipfile import (
    ZipFile,
    ZipInfo,
)

from boltons.cacheutils import cachedproperty
import boto3

from azul import (
    config,
)
from azul.files import file_sha1

log = logging.getLogger(__name__)


class LayerBuilder:
    layer_dir = Path(config.project_root) / 'lambdas' / 'layer'
    out_dir = layer_dir / '.chalice' / 'terraform'

    def __init__(self):
        self.s3 = boto3.client('s3')

    def update_layer_if_necessary(self):
        log.info('Updating layer in bucket: %s with key %s', config.layer_bucket, self.object_key)
        if self.update_required():
            log.info('Updating layer.zip')
            self.update_layer()
        else:
            log.info('No update necessary')

    def update_required(self) -> bool:
        log.info('Checking if layer.zip needs to be updated')
        try:
            # Since the object is content-addressed, just checking for the
            # object's presence is sufficient
            self.s3.head_object(Bucket=config.layer_bucket, Key=self.object_key)
        except self.s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return True
            else:
                raise
        else:
            return False

    def update_layer(self, force: bool = False):
        input_zip = self.out_dir / 'deployment.zip'
        layer_zip = self.out_dir / 'layer.zip'
        if force:
            log.info('Tainting current lambda layer resource to force update')
            command = ['terraform', 'taint', 'aws_lambda_layer_version.dependencies']
            subprocess.run(command, cwd=Path(config.project_root) / 'terraform')
        self.build_layer_zip(layer_zip, input_zip)
        log.info('Uploading layer ZIP to S3')
        self.s3.upload_file(str(layer_zip), config.layer_bucket, self.object_key)

    def build_layer_zip(self, destination, input_zip):
        command = ['chalice', 'package', self.out_dir]
        log.info('Running: %s', command)
        subprocess.run(command, cwd=self.layer_dir).check_returncode()
        log.info('Packaging %s', destination)
        with ZipFile(input_zip, 'r') as deployment_zip:
            with ZipFile(destination, 'w') as layer_zip:
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

    @cachedproperty
    def object_key(self):
        return config.layer_object_key(file_sha1(Path(config.project_root) / 'requirements.txt'))
