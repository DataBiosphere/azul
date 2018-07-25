from azul import config
from azul.template import emit

emit({
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
    }
})
