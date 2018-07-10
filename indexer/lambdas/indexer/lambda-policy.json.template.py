from utils.deployment import aws
from utils.template import emit, env

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
            "Resource": f"arn:aws:es:{aws.region_name}:{aws.account}:domain/{env.AZUL_ES_DOMAIN}/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "es:DescribeElasticsearchDomain"
            ],
            "Resource": f"arn:aws:es:{aws.region_name}:{aws.account}:domain/{env.AZUL_ES_DOMAIN}"
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
                f"arn:aws:sqs:{aws.region_name}:{aws.account}:azul-notify-{env.AZUL_DEPLOYMENT_STAGE}"
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
