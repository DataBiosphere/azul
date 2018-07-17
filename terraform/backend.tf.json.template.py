from azul import config
from azul.deployment import aws
from azul.template import emit

emit(
    {
        "terraform": {
            "backend": {
                "s3": {
                    "bucket": config.terraform_backend_bucket_template.format(account_id=aws.account),
                    # If we break the TF config up into components, the component name goes in between the two dashes.
                    "key": "azul--" + config.deployment_stage + ".tfstate",
                    "region": aws.region_name,
                    **(
                        {
                            "profile": aws.profile['source_profile'],
                            "role_arn": aws.profile['role_arn']
                        } if 'role_arn' in aws.profile else {
                        }
                    )
                }
            }
        }
    }
)
