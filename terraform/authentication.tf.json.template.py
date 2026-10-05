from azul import (
    config,
)
from azul.infra.terraform import (
    emit_tf,
)

emit_tf({
    'data': {
        'aws_kms_key': {
            key.name: {
                'key_id': key.alias
            }
            for key in config.kms_keys
        }
    }
})
