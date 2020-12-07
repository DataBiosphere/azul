from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.terraform import (
    emit_tf,
)

emit_tf({
    "module": {
        # Not using config.project_root because, "A local path must begin with
        # either ./ or ../"
        # https://www.terraform.io/docs/modules/sources.html#local-paths
        f"chalice_{lambda_name}": {
            "source": f"./{lambda_name}",
            "role_arn": "${aws_iam_role." + lambda_name + ".arn}",
            "layer_arn": "${aws_lambda_layer_version.dependencies.arn}",
            "es_endpoint":
                aws.es_endpoint
                if config.share_es_domain else
                ("${aws_elasticsearch_domain.index.endpoint}", 443),
            "es_instance_count":
                aws.es_instance_count
                if config.share_es_domain else
                "${aws_elasticsearch_domain.index.cluster_config[0].instance_count}",
        } for lambda_name in config.lambda_names()
    }
})
