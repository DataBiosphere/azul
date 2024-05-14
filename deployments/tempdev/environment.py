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


def mksrc(google_project, snapshot, subgraphs, flags: int = 0) -> tuple[str, str]:
    project = '_'.join(snapshot.split('_')[1:-3])
    assert flags <= ma | pop
    source = None if flags & pop else ':'.join([
        'tdr',
        google_project,
        'snapshot/' + snapshot,
        '/' + str(partition_prefix_length(subgraphs))
    ])
    return project, source


def mkdelta(items: list[tuple[str, str]]) -> dict[str, str]:
    result = dict(items)
    assert len(items) == len(result), 'collisions detected'
    assert list(result.keys()) == sorted(result.keys()), 'input not sorted'
    return result


def mklist(catalog: dict[str, str]) -> list[str]:
    return list(filter(None, catalog.values()))


def mkdict(previous_catalog: dict[str, str],
           num_expected: int,
           delta: dict[str, str]
           ) -> dict[str, str]:
    catalog = previous_catalog | delta
    num_actual = len(mklist(catalog))
    assert num_expected == num_actual, (num_expected, num_actual)
    return catalog


anvil_sources = mkdict({}, 3, mkdelta([
    mksrc('datarepo-dev-e53e74aa', 'ANVIL_1000G_2019_Dev_20230609_ANV5_202306121732', 6804),
    mksrc('datarepo-dev-42c70e6a', 'ANVIL_CCDG_Sample_1_20230228_ANV5_202302281520', 28),
    mksrc('datarepo-dev-97ad270b', 'ANVIL_CMG_Sample_1_20230225_ANV5_202302281509', 25)
]))


def env() -> Mapping[str, Optional[str]]:
    """
    Returns a dictionary that maps environment variable names to values. The
    values are either None or strings. String values can contain references to
    other environment variables in the form `{FOO}` where FOO is the name of an
    environment variable. See

    https://docs.python.org/3.11/library/string.html#format-string-syntax

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

        'AZUL_DEPLOYMENT_STAGE': 'tempdev',

        'AZUL_DOMAIN_NAME': 'temp.gi.ucsc.edu',
        'AZUL_PRIVATE_API': '0',

        'AZUL_S3_BUCKET': 'edu-ucsc-gi-platform-temp-dev-storage-{AZUL_DEPLOYMENT_STAGE}.{AWS_DEFAULT_REGION}',

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
        'AZUL_TDR_SERVICE_URL': 'https://jade.datarepo-dev.broadinstitute.org',
        'AZUL_SAM_SERVICE_URL': 'https://sam.dsde-dev.broadinstitute.org',
        'AZUL_DUOS_SERVICE_URL': 'https://consent.dsde-dev.broadinstitute.org',
        'AZUL_TERRA_SERVICE_URL': 'https://firecloud-orchestration.dsde-dev.broadinstitute.org',

        'AZUL_ENABLE_MONITORING': '1',

        # $0.191/h × 2 × 24h/d × 30d/mo = $275.08/mo
        'AZUL_ES_INSTANCE_TYPE': 'r6gd.large.elasticsearch',
        'AZUL_ES_INSTANCE_COUNT': '2',

        'AZUL_DEBUG': '1',

        'AZUL_BILLING': 'anvil',

        'AZUL_OWNER': 'hannes@ucsc.edu',

        'AZUL_MONITORING_EMAIL': 'azul-group@ucsc.edu',

        'AZUL_AWS_ACCOUNT_ID': '654654270592',
        'AWS_DEFAULT_REGION': 'us-east-1',

        'GOOGLE_PROJECT': 'platform-temp-dev',

        'AZUL_DEPLOYMENT_INCARNATION': '0',

        'AZUL_GOOGLE_OAUTH2_CLIENT_ID': '807674395527-erth0gf1m7qme5pe6bu384vpdfjh06dg.apps.googleusercontent.com',
    }
