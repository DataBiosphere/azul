from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.modules import (
    load_app_module,
)
from azul.terraform import (
    chalice,
)

direct_access_role = config.dss_direct_access_role('service')
service = load_app_module('service')

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
                "secretsmanager:GetSecretValue"
            ],
            "Resource": [
                f"arn:aws:secretsmanager:{aws.region_name}:{aws.account}:secret:*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:PutObjectAcl",
                "s3:PutObjectTagging",
                "s3:GetObjectTagging"
            ],
            "Resource": [
                "${aws_s3_bucket.%s.arn}/*" % config.storage_term,
                f"arn:aws:s3:::{aws.shared_bucket}/*"
            ]
        },
        # Needed for GetObject to work in versioned bucket
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObjectVersion"
            ],
            "Resource": [
                f"arn:aws:s3:::{aws.shared_bucket}/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"  # Without this, GetObject and HeadObject yield 403 for missing keys, not 404
            ],
            "Resource": [
                "${aws_s3_bucket.%s.arn}" % config.storage_term,
                f"arn:aws:s3:::{aws.shared_bucket}"
            ]
        },
        *(
            [
                # Remove once https://github.com/HumanCellAtlas/data-store/issues/1837 is resolved
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{aws.dss_checkout_bucket(config.dss_endpoint)}/*",
                    ]
                },
                # Remove once https://github.com/HumanCellAtlas/data-store/issues/1837 is resolved
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:ListBucket"
                        # Without this, GetObject and HeadObject yield 403 for missing keys, not 404
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{aws.dss_checkout_bucket(config.dss_endpoint)}"
                    ]
                }
            ] if config.dss_endpoint else []
        ),
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:Query",
                "dynamodb:GetItem",
                "dynamodb:PutItem",
                "dynamodb:UpdateItem",
                "dynamodb:DeleteItem",
                "dynamodb:BatchWriteItem",
                "dynamodb:DescribeTable"
            ],
            "Resource": [
                f"arn:aws:dynamodb:{aws.region_name}:{aws.account}:table/{table_name}"
                for table_name in (
                    config.dynamo_object_version_table_name,
                    config.dynamo_sources_cache_table_name
                )
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "states:StartExecution"
            ],
            "Resource": [
                f"arn:aws:states:{aws.region_name}:{aws.account}:stateMachine:"
                f"{config.qualified_resource_name(config.manifest_sfn)}"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "states:DescribeExecution"
            ],
            "Resource": [
                f"arn:aws:states:{aws.region_name}:{aws.account}:execution:"
                f"{config.qualified_resource_name(config.manifest_sfn)}*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "kms:GenerateMac",
                "kms:VerifyMac"
            ],
            "Resource": [
                "${aws_kms_key.%s.arn}" % config.manifest_kms_key_tf_name
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
        ),
        *chalice.vpc_lambda_iam_policy()
    ]
}
