from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.infra.terraform import (
    emit_tf,
)


def _key_id(key: config.KMSKey) -> str | None:
    try:
        response = aws.kms.describe_key(KeyId=key.alias)
    except aws.kms.exceptions.NotFoundException:
        return None
    else:
        return response['KeyMetadata']['KeyId']


emit_tf({
    'resource': [
        {
            'aws_kms_key': {
                key.name: {
                    'key_usage': key.usage,
                    'customer_master_key_spec': key.spec,
                }
                for key in config.kms_keys
            },
            'aws_kms_alias': {
                key.name: {
                    'name': key.alias,
                    'target_key_id': '${aws_kms_key.%s.key_id}' % key.name
                }
                for key in config.kms_keys
            }
        },
    ],
    'import': [
        entry
        for key in config.kms_keys
        for key_id in [_key_id(key)]
        if key_id is not None
        for entry in [
            {
                'to': f'aws_kms_key.{key.name}',
                'id': key_id
            },
            {
                'to': f'aws_kms_alias.{key.name}',
                'id': key.alias
            }
        ]
    ]
})
