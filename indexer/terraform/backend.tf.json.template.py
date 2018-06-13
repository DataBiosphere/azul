from utils.deployment import aws
from utils.template import emit, env

emit(
    {
        "terraform": {
            "backend": {
                "s3": {
                    "bucket": env.AZUL_TERRAFORM_BACKEND_BUCKET_TEMPLATE.format(account_id=aws.account),
                    # If we break the TF config up into components, the component name goes in between the two dashes.
                    "key": "azul--" + env.AZUL_DEPLOYMENT_STAGE + ".tfstate",
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
