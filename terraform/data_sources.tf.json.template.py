from azul.terraform import (
    emit_tf,
    vpc,
)

emit_tf({
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
