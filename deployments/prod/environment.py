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
        # Set variables for the `prod` (short for production) deployment here.
        #
        # Only modify this file if you intend to commit those changes. To change the
        # environment with a setting that's specific to you AND the deployment, create
        # a environment.local.py right next to this file and make your changes there.
        # Settings applicable to all environments but specific to you go into
        # environment.local.py at the project root.

        'AZUL_VERSIONED_BUCKET': 'edu-ucsc-gi-azul-dcp2-prod-config.{AWS_DEFAULT_REGION}',
        'AZUL_DOMAIN_NAME': 'azul.data.humancellatlas.org',

        'AZUL_DEPLOYMENT_STAGE': 'prod',

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
                        'tdr:tdr-fp-43194825:snapshot/hca_prod_20201120_dcp2___20211004_dcp10:'
                    ]
                ),
                (
                    'hca',
                    ['dcp1', 'it1'],
                    [
                        'tdr:broad-datarepo-terra-prod-hca2:snapshot/hca_prod_20201118_dcp1___20201209:',
                    ]
                ),
                (
                    'lungmap',
                    ['lungmap', 'it0lungmap'],
                    [
                        'tdr:tdr-fp-a02eee6b:snapshot/hca_prod_1bdcecde16be420888f478cd2133d11d__20211013_20211013_lungmap:',
                        'tdr:tdr-fp-41535c81:snapshot/hca_prod_00f056f273ff43ac97ff69ca10e38c89__20211004_lungmap_20211015:',
                        'tdr:tdr-fp-ad968fd8:snapshot/hca_prod_2620497955a349b28d2b53e0bdfcb176__20211012_lungmap_20211013_lungmap:',
                    ]
                )
            ] for i, name in enumerate(names)
        }),

        'AZUL_PARTITION_PREFIX_LENGTH': '2',

        'AZUL_TDR_SOURCE_LOCATION': 'US',
        'AZUL_TDR_SERVICE_URL': 'https://jade-terra.datarepo-prod.broadinstitute.org',
        'AZUL_SAM_SERVICE_URL': 'https://sam.dsde-prod.broadinstitute.org',

        'AZUL_URL_REDIRECT_BASE_DOMAIN_NAME': 'azul.data.humancellatlas.org',
        'AZUL_URL_REDIRECT_FULL_DOMAIN_NAME': 'url.{AZUL_URL_REDIRECT_BASE_DOMAIN_NAME}',

        # $0.372/h × 4 × 24h/d × 30d/mo = $1071.36/mo
        'AZUL_ES_INSTANCE_TYPE': 'r5.xlarge.elasticsearch',
        'AZUL_ES_INSTANCE_COUNT': '4',

        'AZUL_ES_VOLUME_SIZE': '128',

        'AZUL_DEBUG': '1',

        'AZUL_OWNER': 'hannes@ucsc.edu',

        'AZUL_AWS_ACCOUNT_ID': '542754589326',
        'AWS_DEFAULT_REGION': 'us-east-1',

        'GOOGLE_PROJECT': 'platform-hca-prod',

        'AZUL_CONTRIBUTION_CONCURRENCY': '300/64',
    }
