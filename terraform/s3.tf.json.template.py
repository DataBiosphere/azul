import boto3
import json
from azul import config
from azul.deployment import aws
from azul.template import emit

emit({
    "resource": [
        {
            "aws_s3_bucket": {
                "bucket": {
                    "bucket": config.s3_bucket,
                    "acl": "private",
                    "lifecycle_rule": {
                        "id": "manifests",
                        "enabled": True,
                        "prefix": "manifests/",
                        "expiration": {
                            "days": 1
                        }
                    }
                }
            }
        }
    ]
})
