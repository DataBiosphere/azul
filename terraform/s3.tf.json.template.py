from azul import config
from azul.template import emit

emit({
    "resource": [
        {
            "aws_s3_bucket": {
                "private_bucket": {
                    "bucket": config.s3_private_bucket,
                    "acl": "private",
                    "lifecycle_rule": {
                        "id": "manifests",
                        "enabled": True,
                        "prefix": "manifests/",
                        "expiration": {
                            "days": 1
                        }
                    }
                },
                "public_bucket": {
                    "bucket": config.s3_public_bucket,
                    "acl": "public-read",
                    "website": {
                        "index_document": "error.html"
                    }
                }
            }
        }
    ]
})
