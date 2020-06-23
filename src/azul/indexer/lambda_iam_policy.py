from azul import config
from azul.deployment import aws

direct_access_role = config.dss_direct_access_role('indexer')

policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:*:*:*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "es:ESHttpDelete",
                "es:ESHttpGet",
                "es:ESHttpHead",
                "es:ESHttpPut",
                "es:ESHttpPost",
                "es:ESHttpDelete"
            ],
            "Resource": f"arn:aws:es:{aws.region_name}:{aws.account}:domain/{config.es_domain}/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "es:DescribeElasticsearchDomain"
            ],
            "Resource": f"arn:aws:es:{aws.region_name}:{aws.account}:domain/{config.es_domain}"
        },
        {
            "Effect": "Allow",
            "Action": [
                "sqs:ChangeMessageVisibility*",
                "sqs:DeleteMessage*",
                "sqs:ReceiveMessage",
                "sqs:SendMessage"
            ],
            "Resource": [
                f"arn:aws:sqs:{aws.region_name}:{aws.account}:{name}"
                for name in config.work_queue_names
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "sqs:GetQueueAttributes",
                "sqs:GetQueueUrl",
            ],
            "Resource": [
                f"arn:aws:sqs:{aws.region_name}:{aws.account}:{name}"
                for name in config.all_queue_names
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "sqs:ListQueues"
            ],
            "Resource": [
                f"arn:aws:sqs:{aws.region_name}:{aws.account}:*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "secretsmanager:GetSecretValue"
            ],
            "Resource": [
                f"arn:aws:secretsmanager:{aws.region_name}:{aws.account}:secret:*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
            ],
            "Resource": [
                f"arn:aws:s3:::{aws.dss_main_bucket(config.dss_endpoint)}/*",
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject"
            ],
            "Resource": [
                f"arn:aws:s3:::{config.s3_bucket}/health/*",
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "sqs:ReceiveMessage",
                "sqs:ChangeMessageVisibility",
            ],
            "Resource": [
                f"arn:aws:sqs:{aws.region_name}:{aws.account}:{name}"
                for name in config.fail_queue_names
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:BatchWriteItem",
                "dynamodb:DescribeTable"
            ],
            "Resource": [
                f"arn:aws:dynamodb:{aws.region_name}:{aws.account}:table/{config.dynamo_failures_table_name}",
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "ssm:GetParameter"
            ],
            "Resource": [
                f"arn:aws:ssm:{aws.region_name}:{aws.account}:parameter/dcp/*"
            ]
        },
        *(
            [
                {
                    "Effect": "Allow",
                    "Action": "sts:AssumeRole",
                    "Resource": direct_access_role
                }
            ] if direct_access_role is not None else [
            ]
        )
    ]
}
