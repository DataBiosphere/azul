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
    },
    # FIXME: Remove after all deployments are upgraded
    #        https://github.com/DataBiosphere/azul/issues/8215
    'removed': [
        *[
            {
                'from': f'{resource_type}.{key.name}',
                'lifecycle': {
                    'destroy': False
                }
            }
            for key in config.kms_keys
            for resource_type in ('aws_kms_key', 'aws_kms_alias')
        ],
        *[
            {
                'from': f'google_service_account.azul{service_account.value}',
                'lifecycle': {
                    'destroy': False
                }
            }
            for service_account in config.ServiceAccount
        ],
        *(
            [
                {
                    'from': 'google_project_iam_member.azul',
                    'lifecycle': {
                        'destroy': False
                    }
                },
                {
                    'from': 'google_project_iam_custom_role.azul',
                    'lifecycle': {
                        'destroy': False
                    }
                },
            ]
            if config.is_tdr_enabled() else
            []
        ),
    ]
})
