from pathlib import Path

from azul import config
from azul.deployment import emit_tf
from azul.files import file_sha1

emit_tf({
    "resource": [
        {
            "aws_lambda_layer_version": {
                "dependencies": {
                    "layer_name": config.qualified_resource_name("dependencies"),
                    "s3_bucket": config.layer_bucket,
                    "s3_key": config.layer_object_key(file_sha1(Path(config.project_root) / 'requirements.txt'))
                }
            }
        }
    ],
})
