import os

from azul import (
    config,
    require,
)
from azul.deployment import emit_tf

expected_component_path = os.path.join(os.path.abspath(config.project_root), 'terraform', config.terraform_component)
actual_component_path = os.path.dirname(os.path.abspath(__file__))
require(os.path.samefile(expected_component_path, actual_component_path),
        f"The current Terraform component is set to '{config.terraform_component}'. "
        f"You should therefore be in '{expected_component_path}'")

emit_tf({
    "data": [
        {
            "aws_caller_identity": {
                "current": {}
            }
        },
        {
            "aws_region": {
                "current": {}
            }
        },
        *([{
            "google_client_config": {
                "current": {}
            }
        }] if config.enable_gcp() else [])
    ],
    "locals": {
        "account_id": "${data.aws_caller_identity.current.account_id}",
        "region": "${data.aws_region.current.name}",
        "google_project": "${data.google_client_config.current.project}" if config.enable_gcp() else None
    },
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
