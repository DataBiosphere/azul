from azul import (
    config,
)
from azul.infra.terraform import (
    emit_tf,
    vpc,
)

# In blue-green mode, the alias has none of these resources
#
emit_tf(None if config.deployment.is_alias else {
    'data': [
        {
            'aws_nat_gateway': {
                **{
                    f'gitlab_{zone}': {
                        'filter': {
                            'name': 'tag:Name',
                            'values': [f'azul-gitlab_{zone}']
                        },
                    }
                    for zone in range(vpc.num_zones)
                }
            }
        }
    ]
})
