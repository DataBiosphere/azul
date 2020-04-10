from azul import config
from azul.deployment import aws

direct_access_role = config.dss_direct_access_role('service')

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
                "s3:PutObject",
                "s3:GetObject",
                "s3:HeadObject",
                "s3:PutObjectAcl",
                "s3:PutObjectTagging",
                "s3:GetObjectTagging"
            ],
            "Resource": [
                f"arn:aws:s3:::{config.s3_bucket}/*",
                f"arn:aws:s3:::{config.url_redirect_full_domain_name}/*",
                f"arn:aws:s3:::{config.terraform_backend_bucket}/*"
            ]
        },
        # Needed for GetObject to work in versioned bucket
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObjectVersion"
            ],
            "Resource": [
                f"arn:aws:s3:::{config.terraform_backend_bucket}/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"  # Without this, GetObject and HeadObject yield 403 for missing keys, not 404
            ],
            "Resource": [
                f"arn:aws:s3:::{config.s3_bucket}",
                f"arn:aws:s3:::{config.url_redirect_full_domain_name}",
                f"arn:aws:s3:::{config.terraform_backend_bucket}"
            ]
        },
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
                "s3:ListBucket"  # Without this, GetObject and HeadObject yield 403 for missing keys, not 404
            ],
            "Resource": [
                f"arn:aws:s3:::{aws.dss_checkout_bucket(config.dss_endpoint)}"
            ]
        },
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
                f"arn:aws:dynamodb:{aws.region_name}:{aws.account}:table/{config.dynamo_cart_table_name}",
                f"arn:aws:dynamodb:{aws.region_name}:{aws.account}:table/{config.dynamo_cart_item_table_name}",
                f"arn:aws:dynamodb:{aws.region_name}:{aws.account}:table/{config.dynamo_user_table_name}",
                f"arn:aws:dynamodb:{aws.region_name}:{aws.account}:table/{config.dynamo_object_version_table_name}",
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:Query"
            ],
            "Resource": [
                f"arn:aws:dynamodb:{aws.region_name}:{aws.account}:table/{config.dynamo_cart_table_name}/index/*",
                f"arn:aws:dynamodb:{aws.region_name}:{aws.account}:table/{config.dynamo_cart_item_table_name}/index/*",
                f"arn:aws:dynamodb:{aws.region_name}:{aws.account}:table/{config.dynamo_user_table_name}/index/*",
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "states:StartExecution"
            ],
            "Resource": [
                f"arn:aws:states:{aws.region_name}:{aws.account}:stateMachine:{config.manifest_state_machine_name}",
                f"arn:aws:states:{aws.region_name}:{aws.account}:stateMachine:{config.cart_item_state_machine_name}",
                f"arn:aws:states:{aws.region_name}:{aws.account}:stateMachine:{config.cart_export_state_machine_name}"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "states:DescribeExecution"
            ],
            "Resource": [
                f"arn:aws:states:{aws.region_name}:{aws.account}:execution:{config.manifest_state_machine_name}:*",
                f"arn:aws:states:{aws.region_name}:{aws.account}:execution:{config.cart_item_state_machine_name}:*",
                f"arn:aws:states:{aws.region_name}:{aws.account}:execution:{config.cart_export_state_machine_name}"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "apigateway:GET"
            ],
            "Resource": [
                f"arn:aws:apigateway:{aws.region_name}::"
                "/restapis/${module.chalice_service.rest_api_id}/stages/%s/exports/oas30" % config.deployment_stage
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
