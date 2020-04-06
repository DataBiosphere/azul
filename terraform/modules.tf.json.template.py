from azul.deployment import emit_tf

emit_tf({
    "module": {
        # Not using config.project_root because, "A local path must begin with
        # either ./ or ../"
        # https://www.terraform.io/docs/modules/sources.html#local-paths
        "chalice_indexer": {
            "source": "./indexer",
            "role_arn": "${aws_iam_role.indexer.arn}"
        },
        "chalice_service": {
            "source": "./service",
            "role_arn": "${aws_iam_role.service.arn}"
        }
    }
})
