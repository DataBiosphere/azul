from azul import config
from azul.template import emit

emit({
    "resource": [
        {
            "aws_s3_bucket": {
                "bucket": {
                    "bucket": config.s3_bucket,
                    "acl": "private",
                    "force_destroy": True,
                    "lifecycle_rule": {
                        "id": "manifests",
                        "enabled": True,
                        "prefix": "manifests/",
                        "expiration": {
                            "days": config.manifest_expiration
                        }
                    }
                },
                "url_bucket": {
                    "bucket": config.url_redirect_full_domain_name,
                    "force_destroy": not config.is_main_deployment,
                    "acl": "public-read",
                    "website": {
                        # index_document is required; pointing to a non-existent file to return a 404
                        "index_document": "404.html"
                    }
                }
            }
        },
        *([{
            "aws_route53_record": {
                "url_redirect_record": {
                    "zone_id": "${data.aws_route53_zone.azul_url.zone_id}",
                    "name": config.url_redirect_full_domain_name,
                    "type": "CNAME",
                    "ttl": "300",
                    "records": ["${aws_s3_bucket.url_bucket.website_endpoint}"]
                }
            }
        }] if config.url_redirect_base_domain_name else [])
    ],
    **({"data": [
        {
            "aws_route53_zone": {
                "azul_url": {
                    "name": config.url_redirect_base_domain_name + ".",
                    "private_zone": False
                }
            }
        }
    ]} if config.url_redirect_base_domain_name else {})
})
