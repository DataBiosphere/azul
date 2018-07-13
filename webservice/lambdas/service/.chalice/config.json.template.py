import os

from utils import config
from utils.deployment import aws
from utils.template import emit

suffix = '-' + config.deployment_stage
assert config.service_name.endswith(suffix)

host, port = config.es_endpoint

emit({
    'version': '2.0',
    'app_name': config.service_name[:-len(suffix)],  # Chalice appends stage name implicitly
    'api_gateway_stage': 'api',
    'manage_iam_role': False,
    'iam_role_arn': f'arn:aws:iam::{aws.account}:role/{config.service_name}',
    'environment_variables': {
        **{k: v for k, v in os.environ.items() if k.startswith('AZUL_') and k != 'AZUL_HOME'},
        # Hard-wire the ES endpoint, so we don't need to look it up at run-time, for every request/invocation
        'AZUL_ES_ENDPOINT': f'{host}:{port}',
        'HOME': '/tmp'
    },
    'lambda_timeout': 31,
    'lambda_memory_size': 1024
})
