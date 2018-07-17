from azul.deployment import aws
from azul.template import emit

emit(
    {
        "provider": {
            "aws": {
                "version": "~> 1.22",
                "profile": aws.profile['source_profile'],
                "assume_role": {
                    "role_arn": aws.profile['role_arn']
                }
            } if 'role_arn' in aws.profile else {
            }
        }
    }
)
