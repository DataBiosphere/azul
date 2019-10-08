from azul import config
from azul.deployment import aws
from azul.template import emit

emit({
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
                for name in (config.notify_queue_name,
                             config.token_queue_name,
                             config.document_queue_name)
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
                f"arn:aws:s3:::{config.dss_main_bucket()}/*",
            ]
        },
    ]
})
