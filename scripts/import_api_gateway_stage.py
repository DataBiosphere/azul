import boto3
from more_itertools import (
    one,
)

from azul import (
    config,
    logging,
)
from azul.logging import (
    configure_script_logging,
)
from azul.terraform import (
    terraform,
)

log = logging.getLogger(__name__)


def main():
    def resource(name):
        return 'aws_api_gateway_stage.' + name

    resources = terraform.run('state', 'list').splitlines()
    names = {
        name
        for name in ('service', 'indexer')
        if resource(name) not in resources
    }
    if names:
        api_gateway = boto3.client('apigateway')
        apis = api_gateway.get_rest_apis()
        for name in names:
            qname = config.qualified_resource_name(name)
            matching_apis = (
                api['id']
                for api in apis['items']
                if api['name'] == qname
            )
            api_id = one(matching_apis, too_short=None)
            if api_id is None:
                log.warning('Cannot find a REST API under the name %r. '
                            'Skipping import of stage.', qname)
            else:
                address = resource(name)
                id = f'{api_id}/{config.deployment_stage}'
                terraform.run('import', address, id)
    else:
        log.info('Stage appears to have been imported already.')


if __name__ == '__main__':
    configure_script_logging()
    main()
