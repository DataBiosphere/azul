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
                "sqs:GetQueueAttributes",
                "sqs:GetQueueUrl",
                "sqs:ReceiveMessage",
                "sqs:SendMessage"
            ],
            "Resource": [
                f"arn:aws:sqs:{aws.region_name}:{aws.account}:azul-notify-{config.deployment_stage}"
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
        }
    ]
})
