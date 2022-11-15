from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.terraform import (
    emit_tf,
)

tf_config = {
    'data': {
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
            }
        }
    }
}

tf_config['resource']['aws_s3_bucket_public_access_block'] = {
    bucket: {
        **(
            {'provider': srs['provider']}
            if 'provider' in srs else {}
        ),
        'bucket': '${aws_s3_bucket.%s.id}' % bucket,
        'block_public_acls': True,
        'block_public_policy': True,
        'ignore_public_acls': True,
        'restrict_public_buckets': True
    } for bucket, srs in tf_config['resource']['aws_s3_bucket'].items()
}

emit_tf(tf_config)
