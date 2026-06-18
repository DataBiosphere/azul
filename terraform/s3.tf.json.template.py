from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.infra.terraform import (
    block_public_s3_bucket_access,
    emit_tf,
    enable_s3_bucket_inventory,
    set_empty_s3_bucket_lifecycle_config,
)
from azul.lib.functions import (
    iif,
)

tf_config = {
    'data': {
        'aws_s3_bucket': {
            config.logs_term: {
                'bucket': aws.logs_bucket,
            }
        },
    },
    'resource': {
        'aws_s3_bucket': {
            config.storage_term: {
                'bucket': aws.storage_bucket,
                'force_destroy': True
            },
            **iif(config.enable_mirroring, {
                config.mirror_term: {
                    'bucket': aws.mirror_bucket,
                },
                config.ma_mirror_term: {
                    'bucket': aws.ma_mirror_bucket,
                }
            })
        },
        'aws_s3_bucket_lifecycle_configuration': {
            config.storage_term: {
                'bucket': '${aws_s3_bucket.%s.id}' % config.storage_term,
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
            },
            **iif(config.enable_mirroring, {
                term: {
                    'bucket': '${aws_s3_bucket.%s.id}' % term,
                    'rule': {
                        'id': 'mirror_cleanup',
                        'status': 'Enabled',
                        'abort_incomplete_multipart_upload': {
                            'days_after_initiation': 7
                        }
                    }
                } for term in [config.mirror_term, config.ma_mirror_term]
            })
        },
        'aws_s3_bucket_logging': {
            bucket: {
                'bucket': '${aws_s3_bucket.%s.id}' % bucket,
                'target_bucket': '${data.aws_s3_bucket.%s.id}' % config.logs_term,
                # Other S3 log deliveries, like ELB, implicitly put a slash
                # after the prefix. S3 doesn't, so we add one explicitly.
                'target_prefix': config.s3_access_log_path_prefix(bucket) + '/'
            }
            for bucket in (
                config.storage_term,
                *iif(config.enable_mirroring, [config.mirror_term, config.ma_mirror_term])
            )
        }
    }
}
tf_config = enable_s3_bucket_inventory(tf_config)
tf_config = block_public_s3_bucket_access(tf_config)
tf_config = set_empty_s3_bucket_lifecycle_config(tf_config)
emit_tf(tf_config)
