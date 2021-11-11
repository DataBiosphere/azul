import json
from typing import (
    Mapping,
    Optional,
)


def env() -> Mapping[str, Optional[str]]:
    """
    Returns a dictionary that maps environment variable names to values. The 
    values are either None or strings. String values can contain references to 
    other environment variables in the form `{FOO}` where FOO is the name of an 
    environment variable. See 

    https://docs.python.org/3.8/library/string.html#format-string-syntax

    for the concrete syntax. These references will be resolved *after* the
    overall environment has been compiled by merging all relevant
    `environment.py` and `environment.local.py` files.

    Entries with a `None` value will be excluded from the environment. They
    can be used to document a variable without a default value in which case
    other, more specific `environment.py` or `environment.local.py` files must
    provide the value.
    """
    return {
        # Set variables for the `prod2` deployment here.
        #
        # Only modify this file if you intend to commit those changes. To change the
        # environment with a setting that's specific to you AND the deployment, create
        # a environment.local.py right next to this file and make your changes there.
        # Settings applicable to all environments but specific to you go into
        # environment.local.py at the project root.

        'AZUL_VERSIONED_BUCKET': 'edu-ucsc-gi-azul-dcp2-prod-config.{AWS_DEFAULT_REGION}',
        'AZUL_DOMAIN_NAME': 'azul2.data.humancellatlas.org',

        'AZUL_DEPLOYMENT_STAGE': 'prod2',

        'AZUL_S3_BUCKET': 'edu-ucsc-gi-azul-dcp2-prod-storage-{AZUL_DEPLOYMENT_STAGE}',

        'AZUL_CATALOGS': json.dumps({
            f'{name}': dict(atlas=atlas,
                            internal=bool(i),
                            plugins=dict(metadata=dict(name='hca'),
                                         repository=dict(name='tdr')),
                            sources=sources)
            for atlas, names, sources in [
                (
                    'hca',
                    ['dcp10', 'it10'],
                    [
                        'tdr:datarepo-486c6d02:snapshot/hca_prod_6072616c87944b208f52fb15992ea5a4__20211004_20211004:',
                        'tdr:datarepo-49f1b676:snapshot/hca_prod_df88f39f01a84b5b92f43177d6c0f242__20211004_20211004:'
                    ]
                )
            ] for i, name in enumerate(names)
        }),

        'AZUL_PARTITION_PREFIX_LENGTH': '2',

        'AZUL_TDR_SOURCE_LOCATION': 'US',
        'AZUL_TDR_SERVICE_URL': 'https://data.terra.bio',
        'AZUL_SAM_SERVICE_URL': 'https://sam.dsde-prod.broadinstitute.org',

        'AZUL_URL_REDIRECT_BASE_DOMAIN_NAME': 'azul2.data.humancellatlas.org',
        'AZUL_URL_REDIRECT_FULL_DOMAIN_NAME': 'url.{AZUL_URL_REDIRECT_BASE_DOMAIN_NAME}',

        # This deployment shares an ES domain with `prod`
        'AZUL_SHARE_ES_DOMAIN': '1',
        'AZUL_ES_DOMAIN': 'azul-index-prod',

        'AZUL_DEBUG': '1',

        'AZUL_OWNER': 'hannes@ucsc.edu',

        'AZUL_AWS_ACCOUNT_ID': '542754589326',
        'AWS_DEFAULT_REGION': 'us-east-1',

        'GOOGLE_PROJECT': 'platform-hca-prod',
    }
