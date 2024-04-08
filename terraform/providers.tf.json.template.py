from azul import (
    config,
)
from azul.terraform import (
    emit_tf,
)

emit_tf(tag_resources=False, config={
    'terraform': {
        'required_version': config.terraform_version,
        'required_providers': {
            'external': {
                'source': 'hashicorp/external',
                'version': '2.2.0'
            },
            'null': {
                'source': 'hashicorp/null',
                'version': '3.2.0'
            },
            'google': {
                'source': 'hashicorp/google',
                'version': '4.58.0'
            },
            'aws': {
                'source': 'hashicorp/aws',
                'version': '5.11.0'
            },
            'opensearch': {
                'source': 'opensearch-project/opensearch',
                'version': '2.2.1'
            }
        },
    },
    'provider': [
        *(
            # Generate a default `aws` provider and one that pins the region for the certificates of the API Gateway
            # custom domain names. Certificates of edge-optimized custom domain names have to reside in us-east-1.
            {
                'aws': {
                    'region': region,
                    'alias': region
                } if region else {
                }
            }
            for region in (None, 'us-east-1', 'us-west-2')
        ),
        {
            'google': {
                'billing_project': config.google_project(),
                'user_project_override': True,
            }
        }
    ]
})
