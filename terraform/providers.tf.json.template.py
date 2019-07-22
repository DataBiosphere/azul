from azul.deployment import aws
from azul.template import emit

emit({
    "provider": [
        {
            "null": {
                'version': "~> 2.1"
            }
        },
        {
            "google": {
                'version': "~> 1.18"
            }
        },
        *({
            "aws": {
                'version': "~> 1.52",
                **(
                    {
                        'region': region,
                        'alias': region
                    } if region else {
                    }
                ),
                **(
                    {
                        'profile': aws.profile['source_profile'],
                        'assume_role': {
                            'role_arn': aws.profile['role_arn']
                        }
                    } if 'role_arn' in aws.profile else {
                    }
                )
            }
        } for region in (None, 'us-east-1'))
        # Generate a default `aws` provider and one that pins the region for the certificates of the API Gateway
        # custom domain names. Certificates of edge-optimized custom domain names have to reside in us-east-1.
    ]
})
