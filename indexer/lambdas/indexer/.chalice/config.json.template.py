import os

from utils.deployment import aws
from utils.template import emit, env

suffix = '-' + env.AZUL_DEPLOYMENT_STAGE
assert env.AZUL_INDEXER_NAME.endswith(suffix)

host, port = aws.es_endpoint(env.AZUL_ES_DOMAIN)

emit({
    "version": "2.0",
    "app_name": env.AZUL_INDEXER_NAME[:-len(suffix)],  # Chalice appends stage name implicitly
    "api_gateway_stage": env.AZUL_DEPLOYMENT_STAGE,
    "manage_iam_role": False,
    "iam_role_arn": f"arn:aws:iam::{aws.account}:role/{env.AZUL_INDEXER_NAME}",
    "environment_variables": {
        "AZUL_ES_ENDPOINT": f"{host}:{port}",
        **{k: v for k, v in os.environ.items() if k.startswith('AZUL_') and k != 'AZUL_HOME'},
        "HOME": "/tmp"
    },
    "lambda_timeout": 300,
    "lambda_memory_size": 512
})
