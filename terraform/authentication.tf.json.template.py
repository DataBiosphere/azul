from azul import (
    config,
)
from azul.infra.terraform import (
    emit_tf,
)

# In blue-green mode, the alias has none of these resources
#
emit_tf(None if config.deployment.is_alias else {
    'data': {
        'aws_kms_key': {
            key.name: {
                'key_id': key.alias
            }
            for key in config.kms_keys
        }
    }
})
