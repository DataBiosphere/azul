import json

from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.terraform import (
    emit_tf,
    provider_fragment,
)

emit_tf({
    'resource': {
        'aws_s3_bucket': {
            'cloudtrail_shared': {
                **provider_fragment(config.cloudtrail_s3_bucket_region),
                'bucket': f'edu-ucsc-gi-{aws.account_name}-cloudtrail'
            }
        },
        'aws_s3_bucket_policy': {
            'cloudtrail_shared': {
                'bucket': '${aws_s3_bucket.cloudtrail_shared.id}',
                'policy': json.dumps({
                    'Version': '2012-10-17',
                    'Statement': [
                        {
                            'Effect': 'Allow',
                            'Principal': {
                                'Service': 'cloudtrail.amazonaws.com'
                            },
                            'Action': 's3:GetBucketAcl',
                            'Resource': '${aws_s3_bucket.cloudtrail_shared.arn}'
                        },
                        {
                            'Effect': 'Allow',
                            'Principal': {
                                'Service': 'cloudtrail.amazonaws.com'
                            },
                            'Action': 's3:PutObject',
                            'Resource': '${aws_s3_bucket.cloudtrail_shared.arn}/AWSLogs/'
                                        f'{config.aws_account_id}/*',
                            'Condition': {
                                'StringEquals': {
                                    's3:x-amz-acl': 'bucket-owner-full-control'
                                }
                            }
                        }
                    ]
                })
            }
        },
        'aws_cloudtrail': {
            'shared': {
                **provider_fragment(config.cloudtrail_trail_region),
                'name': 'azul-shared',
                's3_bucket_name': '${aws_s3_bucket.cloudtrail_shared.id}',
                'enable_log_file_validation': True,
                'is_multi_region_trail': True
            }
        }
    }
})
