from azul.deployment import (
    aws,
)
from azul.terraform import (
    emit_tf,
)

emit_tf({
    "provider": [
        {
            "template": {
                'version': '2.2.0'
            }
        },
        {
            "null": {
                'version': '2.1.2'
            }
        },
        {
            "google": {
                'version': '2.20.3'
            }
        },
        *({
            "aws": {
                'version': '2.70.0',
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
        # Generate a default `aws` provider and one that pins the region for the
        # certificates of the API Gateway custom domain names. Certificates of
        # edge-optimized custom domain names have to reside in us-east-1.
    ]
})
