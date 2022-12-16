# FIXME: Remove after all deployments have been upgraded to 1.3.x
#        https://github.com/DataBiosphere/azul/issues/4744

import json
import logging

import jq
from more_itertools import (
    one,
)

from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)


def main():
    coordinates = dict(Bucket=config.versioned_bucket,
                       Key=f'azul-{config.terraform_component}-{config.deployment_stage}.tfstate')
    log.info('Migrating Terraform state at %r', coordinates)
    response = aws.s3.get_object(**coordinates)
    state = json.load(response['Body'])
    tf_version = state['terraform_version']
    if tf_version == '1.3.4':
        log.info('Terraform state appears to have been migrated already.')
    elif tf_version == '0.12.31':
        program = jq.compile(' | '.join([
            '.terraform_version = "1.3.4"',
            '.serial |= . + 1',
            '.check_results = null',
            '.resources[].provider |= sub(' + ';'.join([
                r'"\\.(?<p>[^.]+)(?<r>\\.[^.]+)?$"'
                r';"[\"registry.terraform.io/hashicorp/\(.p)\"]\(.r//"")"'
            ]) + ')',
            '.resources[].instances[].sensitive_attributes = []'
        ]))
        state = one(program.input(state).all())
        state = json.dumps(state, indent=4)
        aws.s3.put_object(**coordinates, Body=state)
        log.info('Successfully migrated Terraform state at %r', coordinates)
    else:
        assert False, ('Unexpected Terraform version in state file', tf_version)


if __name__ == '__main__':
    configure_script_logging(log)
    main()
