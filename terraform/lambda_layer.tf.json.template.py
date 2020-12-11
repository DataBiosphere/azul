from azul import (
    config,
)
from azul.lambda_layer import (
    DependenciesLayer,
)
from azul.terraform import (
    emit_tf,
)

layer = DependenciesLayer()

emit_tf({
    "resource": [
        {
            "aws_lambda_layer_version": {
                "dependencies": {
                    "layer_name": config.qualified_resource_name("dependencies"),
                    "s3_bucket": config.lambda_layer_bucket,
                    "s3_key": layer.object_key
                }
            }
        }
    ],
})
