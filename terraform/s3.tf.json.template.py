from azul import (
    config,
)
from azul.terraform import (
    emit_tf,
)

emit_tf({
    "resource": [
        {
            "aws_s3_bucket": {
                "storage": {
                    "bucket": config.s3_bucket,
                    "force_destroy": True
                },
                "urls": {
                    "bucket": config.url_redirect_full_domain_name,
                    "force_destroy": not config.is_main_deployment(),
                }
            },
            "aws_s3_bucket_lifecycle_configuration": {
                "storage": {
                    "bucket": "${aws_s3_bucket.storage.id}",
                    "rule": {
                        "id": "manifests",
                        "status": "Enabled",
                        "filter": {
                            "prefix": "manifests/"
                        },
                        "expiration": {
                            "days": config.manifest_expiration
                        },
                        "abort_incomplete_multipart_upload": {
                            "days_after_initiation": 1
                        }
                    }
                }
            },
            "aws_s3_bucket_acl": {
                "storage": {
                    "bucket": "${aws_s3_bucket.storage.id}",
                    "acl": "private",
                },
                "urls": {
                    "bucket": "${aws_s3_bucket.urls.id}",
                    "acl": "public-read",
                }
            },
            "aws_s3_bucket_website_configuration": {
                "urls": {
                    "bucket": "${aws_s3_bucket.urls.id}",
                    "index_document": {
                        "suffix": "404.html"
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
                    "records": ["${aws_s3_bucket_website_configuration.urls.website_endpoint}"]
                }
            }
        }] if config.url_redirect_base_domain_name else [])
    ],
    **({
           "data": [
               {
                   "aws_route53_zone": {
                       "azul_url": {
                           "name": config.url_redirect_base_domain_name + ".",
                           "private_zone": False
                       }
                   }
               }
           ]
       } if config.url_redirect_base_domain_name else {})
})
