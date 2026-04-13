from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.infra.terraform import (
    chalice,
)
from azul.lib.collections import (
    alist,
)

direct_access_role = config.dss_direct_access_role('indexer')

policy = {
    'Version': '2012-10-17',
    'Statement': [
        {
            'Effect': 'Allow',
            'Action': [
                'logs:CreateLogGroup',
                'logs:CreateLogStream',
                'logs:PutLogEvents'
            ],
            'Resource': 'arn:aws:logs:*:*:*'
        },
        {
            'Effect': 'Allow',
            'Action': [
                'es:ESHttpDelete',
                'es:ESHttpGet',
                'es:ESHttpHead',
                'es:ESHttpPut',
                'es:ESHttpPost',
                'es:ESHttpDelete'
            ],
            'Resource': f'arn:aws:es:{aws.region_name}:{aws.account}:domain/{config.opensearch_domain}/*'
        },
        {
            'Effect': 'Allow',
            'Action': [
                'es:DescribeElasticsearchDomain'
            ],
            'Resource': f'arn:aws:es:{aws.region_name}:{aws.account}:domain/{config.opensearch_domain}'
        },
        {
            'Effect': 'Allow',
            'Action': [
                'sqs:ChangeMessageVisibility*',
                'sqs:DeleteMessage*',
                'sqs:ReceiveMessage',
                'sqs:SendMessage'
            ],
            'Resource': [
                f'arn:aws:sqs:{aws.region_name}:{aws.account}:{name}'
                for name in config.work_queue_names
            ]
        },
        {
            'Effect': 'Allow',
            'Action': [
                'sqs:GetQueueAttributes',
                'sqs:GetQueueUrl',
            ],
            'Resource': [
                f'arn:aws:sqs:{aws.region_name}:{aws.account}:{name}'
                for name in config.all_queue_names
            ]
        },
        {
            'Effect': 'Allow',
            'Action': [
                'sqs:ListQueues'
            ],
            'Resource': [
                f'arn:aws:sqs:{aws.region_name}:{aws.account}:*'
            ]
        },
        {
            'Effect': 'Allow',
            'Action': [
                'secretsmanager:GetSecretValue'
            ],
            'Resource': [
                f'arn:aws:secretsmanager:{aws.region_name}:{aws.account}:secret:*'
            ]
        },
        *(
            [
                {
                    'Effect': 'Allow',
                    'Action': [
                        's3:GetObject',
                    ],
                    'Resource': [
                        f'arn:aws:s3:::{aws.dss_main_bucket(config.dss_endpoint)}/*',
                    ]
                },
            ] if config.dss_endpoint else []
        ),
        *(
            [
                {
                    'Effect': 'Allow',
                    'Action': [
                        's3:GetObject',
                    ],
                    'Resource': [
                        f'arn:aws:s3:::{aws.logs_bucket}/{prefix}'
                        for prefix in (
                            config.alb_access_log_path_prefix('*', deployment=None),
                            config.s3_access_log_path_prefix('*', deployment=None),
                        )
                    ]
                },
            ] if config.enable_log_forwarding else []
        ),
        *(
            [
                {
                    'Effect': 'Allow',
                    'Action': [
                        's3:ListBucket',
                        's3:GetObject',
                        's3:PutObject',
                    ],
                    'Resource': [
                        f'arn:aws:s3:::{resource}'
                        for bucket in alist(
                            aws.mirror_bucket,
                            config.mirror_bucket,
                            aws.ma_mirror_bucket,
                        )
                        for resource in [bucket, f'{bucket}/*']
                    ]
                },
                {
                    'Effect': 'Allow',
                    'Action': [
                        'dynamodb:GetItem',
                        'dynamodb:PutItem',
                    ],
                    'Resource': [
                        f'arn:aws:dynamodb:{aws.region_name}:{aws.account}:table/'
                        f'{config.dynamo_sources_cache_table_name}'
                    ]
                }
            ] if config.enable_mirroring else []
        ),
        {
            'Effect': 'Allow',
            'Action': [
                's3:GetObject',
                's3:PutObject'
            ],
            'Resource': [
                '${aws_s3_bucket.%s.arn}/health/*' % config.storage_term,
            ]
        },
        {
            'Effect': 'Allow',
            'Action': [
                'ssm:GetParameter'
            ],
            'Resource': [
                f'arn:aws:ssm:{aws.region_name}:{aws.account}:parameter/dcp/*'
            ]
        },
        *(
            [
                {
                    'Effect': 'Allow',
                    'Action': 'sts:AssumeRole',
                    'Resource': direct_access_role
                }
            ] if direct_access_role is not None else [
            ]
        ),
        *chalice.vpc_lambda_iam_policy()
    ]
}
