from azul import (
    config,
)
from azul.infra.terraform import (
    emit_tf,
)

emit_tf({
    'resource': {
        'aws_ecr_repository': {
            f'{app}_lambda': {
                'name': config.qualified_resource_name(app),
                'force_delete': True
            }
            for app in ('indexer', 'service')
        }
    }
})
