from collections.abc import (
    Mapping,
)
import json
from typing import (
    Optional,
)


def partition_prefix_length(n: int) -> int:
    """
    For a given number of subgraphs, return a partition prefix length that is
    expected to rarely exceed 512 subgraphs per partition.

    >>> [partition_prefix_length(n) for n in (0, 1, 512, 513, 16 * 512, 16 * 513 )]
    [0, 0, 0, 1, 1, 2]
    """
    return 1 + partition_prefix_length(n // 16) if n > 512 else 0


ma = 1  # managed access
pop = 2  # remove snapshot


def mksrc(google_project, snapshot, subgraphs, flags: int = 0):
    assert flags <= ma | pop
    source = None if flags & pop else ':'.join([
        'tdr',
        google_project,
        'snapshot/' + snapshot,
        '/' + str(partition_prefix_length(subgraphs))
    ])
    key = '_'.join(snapshot.split('_')[1:-1])
    return key, source


def mkdict(items):
    result = dict(items)
    assert len(items) == len(result), 'collisions detected'
    assert list(result.keys()) == sorted(result.keys()), 'input not sorted'
    return result


anvil_sources = mkdict([
    mksrc("datarepo-6e7e19c0", "ANVIL_CMG_UWash_GRU_20221020_ANV4_202211041542", 0),
    mksrc("datarepo-06b1a988", "ANVIL_CMG_UWASH_DS_BAV_IRB_PUB_RD_20221020_ANV4_202211041546", 0),
    mksrc("datarepo-31aa1bb0", "ANVIL_CMG_UWash_DS_EP_20221020_ANV4_202211041549", 0),
    mksrc("datarepo-1ca77ea2", "ANVIL_CMG_UWASH_HMB_IRB_20221020_ANV4_202211041552", 0),
    mksrc("datarepo-63552b69", "ANVIL_CMG_UWASH_HMB_20221020_ANV4_202211041555", 0),
    mksrc("datarepo-d422a012", "ANVIL_CMG_UWASH_DS_HFA_20221020_ANV4_202211041559", 0),
    mksrc("datarepo-47d6f90b", "ANVIL_CMG_UWash_GRU_IRB_20221020_ANV4_202211041602", 0),
    mksrc("datarepo-3b12f2ea", "ANVIL_CMG_UWASH_DS_NBIA_20221020_ANV4_202211041605", 0),
    mksrc("datarepo-1c1969e5", "ANVIL_CMG_UWASH_DS_BDIS_20221020_ANV4_202211041609", 0),
    mksrc("datarepo-392cd2da", "ANVIL_GTEx_V8_hg38_20221013_ANV4_202211071512", 0),
])


def env() -> Mapping[str, Optional[str]]:
    """
    Returns a dictionary that maps environment variable names to values. The
    values are either None or strings. String values can contain references to
    other environment variables in the form `{FOO}` where FOO is the name of an
    environment variable. See

    https://docs.python.org/3.9/library/string.html#format-string-syntax

    for the concrete syntax. These references will be resolved *after* the
    overall environment has been compiled by merging all relevant
    `environment.py` and `environment.local.py` files.

    Entries with a `None` value will be excluded from the environment. They
    can be used to document a variable without a default value in which case
    other, more specific `environment.py` or `environment.local.py` files must
    provide the value.
    """
    return {
        # Set variables for the `anvildev` (short for AnVIL development)
        # deployment here.
        #
        # Only modify this file if you intend to commit those changes. To change the
        # environment with a setting that's specific to you AND the deployment, create
        # a environment.local.py right next to this file and make your changes there.
        # Settings applicable to all environments but specific to you go into
        # environment.local.py at the project root.

        'AZUL_DEPLOYMENT_STAGE': 'anvildev',

        'AZUL_DOMAIN_NAME': 'anvil.gi.ucsc.edu',
        'AZUL_PRIVATE_API': '1',

        'AZUL_VERSIONED_BUCKET': 'edu-ucsc-gi-platform-anvil-dev.{AWS_DEFAULT_REGION}',
        'AZUL_S3_BUCKET': 'edu-ucsc-gi-platform-anvil-dev-{AZUL_DEPLOYMENT_STAGE}',

        'AZUL_CATALOGS': json.dumps({
            f'{catalog}{suffix}': dict(atlas=atlas,
                                       internal=internal,
                                       plugins=dict(metadata=dict(name='anvil'),
                                                    repository=dict(name='tdr_anvil')),
                                       sources=list(filter(None, sources.values())))
            for atlas, catalog, sources in [
                ('anvil', 'anvil', anvil_sources),
            ]
            for suffix, internal in [
                ('', False),
                ('-it', True)
            ]
        }),

        'AZUL_TDR_SOURCE_LOCATION': 'us-central1',
        'AZUL_TDR_SERVICE_URL': 'https://data.terra.bio',
        'AZUL_SAM_SERVICE_URL': 'https://sam.dsde-prod.broadinstitute.org',

        'AZUL_ENABLE_MONITORING': '1',

        # $0.382/h × 3 × 24h/d × 30d/mo = $825.12/mo
        'AZUL_ES_INSTANCE_TYPE': 'r6gd.xlarge.elasticsearch',
        'AZUL_ES_INSTANCE_COUNT': '3',

        'AZUL_DEBUG': '1',

        'AZUL_BILLING': 'anvil',

        'AZUL_OWNER': 'hannes@ucsc.edu',

        'AZUL_MONITORING_EMAIL': 'azul-group@ucsc.edu',

        'AZUL_AWS_ACCOUNT_ID': '289950828509',
        'AWS_DEFAULT_REGION': 'us-east-1',

        'GOOGLE_PROJECT': 'platform-anvil-dev',

        'AZUL_DEPLOYMENT_INCARNATION': '2',

        'AZUL_GOOGLE_OAUTH2_CLIENT_ID': '561542988117-3cv4g8ii9enl2000ra6m02r3ne7bgnth.apps.googleusercontent.com',
    }
