from azul.deployment import aws
from azul.template import emit

emit(
    {
        "provider": [
            {
                "aws": {
                    "version": "~> 1.22",
                    "profile": aws.profile['source_profile'],
                    "assume_role": {
                        "role_arn": aws.profile['role_arn']
                    }
                } if 'role_arn' in aws.profile else {
                }
            },
            {
                "aws": {
                    # Pin the region for the certificates of the API Gateway custom domain names. Certificates of
                    # edge-optimized custom domain names have to reside in us-east-1.
                    "alias": "us-east-1",
                    "region": "us-east-1"
                }
            }
        ]
    }
)
