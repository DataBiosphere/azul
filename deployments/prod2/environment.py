import json
from typing import (
    Mapping,
    Optional,
)


def partition_prefix_length(n: int) -> int:
    """
    For a given number of subgraphs, return a partition prefix length that
    yields at most 512 subgraphs per partition.

    >>> [partition_prefix_length(n) for n in (0, 1, 512, 513, 16 * 512, 16 * 513 )]
    [0, 0, 0, 1, 1, 2]
    """
    return 1 + partition_prefix_length(n // 16) if n > 512 else 0


def mksrc(project, snapshot, subgraphs, ma: int = 0):
    """
    :param ma: 1 for managed access
    """
    return f'tdr:{project}:snapshot/{snapshot}:/{partition_prefix_length(subgraphs)}'


dcp1_sources = [
    mksrc('datarepo-673cd580', 'hca_prod_005d611a14d54fbf846e571a1f874f70__20211129_dcp1_20211129_dcp1', 13),
    mksrc('datarepo-daebba9b', 'hca_prod_027c51c60719469fa7f5640fe57cbece__20211129_dcp1_20211129_dcp1', 7),
    mksrc('datarepo-41546537', 'hca_prod_091cf39b01bc42e59437f419a66c8a45__20211129_dcp1_20211129_dcp1', 19),
    mksrc('datarepo-23f37ee4', 'hca_prod_116965f3f09447699d28ae675c1b569c__20211129_dcp1_20211129_dcp1', 7),
    mksrc('datarepo-dde43a62', 'hca_prod_1defdadaa36544ad9b29443b06bd11d6__20211129_dcp1_20211129_dcp1', 2560),
    mksrc('datarepo-3fb60b9c', 'hca_prod_4a95101c9ffc4f30a809f04518a23803__20211129_dcp1_20211129_dcp1', 33),
    mksrc('datarepo-70f89602', 'hca_prod_4d6f6c962a8343d88fe10f53bffd4674__20211129_dcp1_20211129_dcp1', 11),
    mksrc('datarepo-db7a8a68', 'hca_prod_4e6f083b5b9a439398902a83da8188f1__20211129_dcp1_20211129_dcp1', 29),
    mksrc('datarepo-b0a40e19', 'hca_prod_577c946d6de54b55a854cd3fde40bff2__20211129_dcp1_20211129_dcp1', 6),
    mksrc('datarepo-5df70008', 'hca_prod_74b6d5693b1142efb6b1a0454522b4a0__20211129_dcp1_20211129_dcp1', 5459),
    mksrc('datarepo-8d469885', 'hca_prod_8185730f411340d39cc3929271784c2b__20211213_dcp1_20220104_dcp1', 11),
    mksrc('datarepo-ba05c608', 'hca_prod_90bd693340c048d48d76778c103bf545__20211220_dcp1_20220104_dcp1', 2244),
    mksrc('datarepo-c3a18afe', 'hca_prod_9c20a245f2c043ae82c92232ec6b594f__20211221_dcp1_20220104_dcp1', 11),
    mksrc('datarepo-8bdfb63a', 'hca_prod_a29952d9925e40f48a1c274f118f1f51__20211213_dcp1_20220104_dcp1', 25),
    mksrc('datarepo-2fa410c8', 'hca_prod_a9c022b4c7714468b769cabcf9738de3__20211213_dcp1_20220104_dcp1', 22),
    mksrc('datarepo-f646ffa6', 'hca_prod_abe1a013af7a45ed8c26f3793c24a1f4__20211213_dcp1_20220104_dcp1', 45),
    mksrc('datarepo-0e566a81', 'hca_prod_ae71be1dddd84feb9bed24c3ddb6e1ad__20211213_dcp1_20220104_dcp1', 3514),
    mksrc('datarepo-798bacbd', 'hca_prod_c4077b3c5c984d26a614246d12c2e5d7__20211220_dcp1_20220106_dcp1', 215),
    mksrc('datarepo-6d2bb0f1', 'hca_prod_cc95ff892e684a08a234480eca21ce79__20211220_dcp1_20220106_dcp1', 255),
    mksrc('datarepo-dc119c81', 'hca_prod_f81efc039f564354aabb6ce819c3d414__20211220_dcp1_20220106_dcp1', 3),
    mksrc('datarepo-62ca7774', 'hca_prod_f86f1ab41fbb4510ae353ffd752d4dfc__20211220_dcp1_20220106_dcp1', 19),
    mksrc('datarepo-057faf2b', 'hca_prod_f8aa201c4ff145a4890e840d63459ca2__20211220_dcp1_20220106_dcp1', 382),
]

dcp10_sources = [
    mksrc('datarepo-486c6d02', 'hca_prod_6072616c87944b208f52fb15992ea5a4__20211004_20211004', 603),
    mksrc('datarepo-49f1b676', 'hca_prod_df88f39f01a84b5b92f43177d6c0f242__20211004_20211004', 1),
]


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
            f'{catalog}{suffix}': dict(atlas=atlas,
                                       internal=internal,
                                       plugins=dict(metadata=dict(name='hca'),
                                                    repository=dict(name='tdr')),
                                       sources=sources)
            for atlas, catalog, sources in [
                (
                    'hca',
                    'dcp1',
                    dcp1_sources
                ),
                (
                    'hca',
                    'dcp10',
                    dcp10_sources
                )
            ] for suffix, internal in [
                ('', False),
                ('-it', True)
            ]
        }),

        'AZUL_PARTITION_PREFIX_LENGTH': '2',

        'AZUL_TDR_SOURCE_LOCATION': 'US',
        'AZUL_TDR_SERVICE_URL': 'https://data.terra.bio',
        'AZUL_SAM_SERVICE_URL': 'https://sam.dsde-prod.broadinstitute.org',

        'AZUL_URL_REDIRECT_BASE_DOMAIN_NAME': 'azul2.data.humancellatlas.org',
        'AZUL_URL_REDIRECT_FULL_DOMAIN_NAME': 'url.{AZUL_URL_REDIRECT_BASE_DOMAIN_NAME}',

        # $0.382/h × 4 × 24h/d × 30d/mo = $1100.16/mo
        'AZUL_ES_INSTANCE_TYPE': 'r6gd.xlarge.elasticsearch',
        'AZUL_ES_INSTANCE_COUNT': '4',

        'AZUL_DEBUG': '1',

        'AZUL_OWNER': 'hannes@ucsc.edu',

        'AZUL_AWS_ACCOUNT_ID': '542754589326',
        'AWS_DEFAULT_REGION': 'us-east-1',

        'GOOGLE_PROJECT': 'platform-hca-prod',
    }
