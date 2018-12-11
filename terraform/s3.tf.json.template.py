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
        },
        {
            "aws_route53_record": {
                "public_bucket_alias": {
                    "zone_id": "${data.aws_route53_zone.azul.zone_id}",
                    "name": config.s3_public_bucket_domain,
                    "type": "A",
                    "alias": {
                        "name": "${aws_s3_bucket.public_bucket.website_endpoint}",
                        "zone_id": "${aws_s3_bucket.public_bucket.hosted_zone_id}",
                        "evaluate_target_health": True
                    }
                }
            }
        }
    ]
})
