from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.terraform import (
    emit_tf,
    vpc,
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
            "cloudwatch_log_group_provisioner": f"{lambda_name}_log_group_provisioner",
            config.var_vpc_security_group_id: f"${{aws_security_group.{lambda_name}.id}}",
            config.var_vpc_subnet_ids: ([
                f"${{data.aws_subnet.gitlab_{vpc.subnet_name(public=False)}_{zone}.id}}"
                for zone in range(vpc.num_zones)
            ]),
            **((
                {
                    config.var_vpc_endpoint_id: f"${{aws_vpc_endpoint.{lambda_name}.id}}"
                } if config.private_api else {
                }
            ))
        } for lambda_name in config.lambda_names()
    }
})
