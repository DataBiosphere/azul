from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.terraform import (
    emit_tf,
)

emit_tf(tag_resources=False, config={
    "terraform": {
        "backend": {
            "s3": {
                "bucket": aws.shared_bucket,
                "key": f"azul-{config.terraform_component}-{config.deployment_stage}.tfstate",
                "region": aws.region_name,
            }
        }
    }
})
