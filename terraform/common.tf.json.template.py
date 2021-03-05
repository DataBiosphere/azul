from pathlib import (
    Path,
)

from azul import (
    config,
    require,
)
from azul.terraform import (
    emit_tf,
)

expected_component_path = (Path(config.project_root) / 'terraform'
                           / config.terraform_component)
actual_component_path = Path(__file__).absolute().parent
require(expected_component_path.samefile(actual_component_path),
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
        "google_project": ("${data.google_client_config.current.project}"
                           if config.enable_gcp() else None)
    },
})
