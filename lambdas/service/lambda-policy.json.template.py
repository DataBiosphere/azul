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
                "s3:PutObject",
                "s3:GetObject",
                "s3:PutObjectAcl"
            ],
            "Resource": [
                f"arn:aws:s3:::{config.s3_bucket}/*",
                f"arn:aws:s3:::{config.url_redirect_full_domain_name}/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": f"arn:aws:s3:::{config.url_redirect_full_domain_name}"
        },
        {
            "Effect": "Allow",
            "Action": [
                "states:StartExecution"
            ],
            "Resource": [
                f"arn:aws:states:{aws.region_name}:{aws.account}:stateMachine:{config.manifest_state_machine_name}"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "states:DescribeExecution"
            ],
            "Resource": [
                f"arn:aws:states:{aws.region_name}:{aws.account}:execution:{config.manifest_state_machine_name}:*"
            ]
        }
    ]
})
