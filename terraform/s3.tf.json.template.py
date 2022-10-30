from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.terraform import (
    emit_tf,
)

emit_tf({
    'data': {
        'aws_route53_zone': {
            **(
                {
                    'azul_url': {
                        'name': config.url_redirect_base_domain_name + '.',
                        'private_zone': False
                    }
                }
                if config.url_redirect_base_domain_name else
                {}
            )
        },
        'aws_s3_bucket': {
            'logs': {
                'bucket': aws.qualified_bucket_name(config.logs_term),
            }
        },
    },
    'resource': {
        'aws_s3_bucket': {
            'storage': {
                'bucket': config.s3_bucket,
                'force_destroy': True
            },
            'urls': {
                'bucket': config.url_redirect_full_domain_name,
                'force_destroy': config.is_sandbox_or_personal_deployment,
            }
        },
        'aws_s3_bucket_lifecycle_configuration': {
            'storage': {
                'bucket': '${aws_s3_bucket.storage.id}',
                'rule': {
                    'id': 'manifests',
                    'status': 'Enabled',
                    'filter': {
                        'prefix': 'manifests/'
                    },
                    'expiration': {
                        'days': config.manifest_expiration
                    },
                    'abort_incomplete_multipart_upload': {
                        'days_after_initiation': 1
                    }
                }
            }
        },
        'aws_s3_bucket_logging': {
            'storage': {
                'bucket': '${aws_s3_bucket.storage.id}',
                'target_bucket': '${data.aws_s3_bucket.logs.id}',
                # Other S3 log delivieries, like ELB, implicitly put a slash
                # after the prefix. S3 doesn't, so we add one explicitly.
                'target_prefix': config.s3_access_log_path_prefix('storage') + '/'
            }
        },
        'aws_s3_bucket_acl': {
            'storage': {
                'bucket': '${aws_s3_bucket.storage.id}',
                'acl': 'private',
            },
            'urls': {
                'bucket': '${aws_s3_bucket.urls.id}',
                'acl': 'public-read',
            }
        },
        'aws_s3_bucket_website_configuration': {
            'urls': {
                'bucket': '${aws_s3_bucket.urls.id}',
                'index_document': {
                    'suffix': '404.html'
                }
            }
        },
        'aws_route53_record': {
            **(
                {
                    'url_redirect_record': {
                        'zone_id': '${data.aws_route53_zone.azul_url.zone_id}',
                        'name': config.url_redirect_full_domain_name,
                        'type': 'CNAME',
                        'ttl': '300',
                        'records': ['${aws_s3_bucket_website_configuration.urls.website_endpoint}']
                    }
                }
                if config.url_redirect_base_domain_name else
                {}
            )
        }
    },
})
