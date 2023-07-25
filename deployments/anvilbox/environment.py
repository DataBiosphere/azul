from collections.abc import (
    Mapping,
)
import json
from typing import (
    Optional,
)

is_sandbox = True


def common_prefix(n: int) -> str:
    """
    For a given number of subgraphs, return a common prefix that yields around
    16 subgraphs.

    >>> [common_prefix(n) for n in (0, 1, 31, 32, 33, 512+15, 512+16, 512+17)]
    ['', '', '', '', '1', 'f', '01', '11']
    """
    hex_digits = '0123456789abcdef'
    m = len(hex_digits)
    # Double threshold to lower probability that no subgraphs match the prefix
    return hex_digits[n % m] + common_prefix(n // m) if n > 2 * m else ''


ma = 1  # managed access
pop = 2  # remove snapshot


def mksrc(google_project,
          snapshot,
          subgraphs,
          flags: int = 0,
          /,
          prefix: Optional[str] = None
          ) -> tuple[str, str]:
    project = '_'.join(snapshot.split('_')[1:-3])
    assert flags <= ma | pop
    if prefix is None:
        prefix = common_prefix(subgraphs)
    source = None if flags & pop else ':'.join([
        'tdr',
        google_project,
        'snapshot/' + snapshot,
        prefix + '/0'
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
        # Set variables for the `anvilbox` deployment here. The anvilbox is used
        # to run integration tests against PRs and to perform CI/CD experiments.
        #
        # You can use this file as a template for a personal deployment. Look
        # for conditionals using the `is_sandbox` variable and adjust the `else`
        # branch accordingly.
        #
        # Only modify this file if you intend to commit those changes. To apply
        # a setting that's specific to you AND the deployment, create an
        # `environment.local.py` file right next to this one and apply that
        # setting there. Settings that are applicable to all environments but
        # specific to you go into `environment.local.py` at the project root.

        # When using this file as a template for a personal deployment, replace
        # `None` with a short string that is specific to YOU.
        #
        'AZUL_DEPLOYMENT_STAGE': 'anvilbox' if is_sandbox else None,

        'AZUL_IS_SANDBOX': str(int(is_sandbox)),

        # This deployment uses a subdomain of the `anvildev` deployment's
        # domain.
        #
        'AZUL_DOMAIN_NAME': 'anvil.gi.ucsc.edu',
        'AZUL_SUBDOMAIN_TEMPLATE': '*.{AZUL_DEPLOYMENT_STAGE}',
        'AZUL_PRIVATE_API': '0',

        'AZUL_S3_BUCKET': 'edu-ucsc-gi-platform-anvil-dev-storage-{AZUL_DEPLOYMENT_STAGE}.{AWS_DEFAULT_REGION}',

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

        **(
            {
                # $0.382/h × 2 × 24h/d × 30d/mo = $550.08/mo
                'AZUL_ES_INSTANCE_TYPE': 'r6gd.xlarge.elasticsearch',
                'AZUL_ES_INSTANCE_COUNT': '2',
            } if is_sandbox else {
                # Personal deployments share an ES domain with `anvilbox`
                'AZUL_SHARE_ES_DOMAIN': '1',
                'AZUL_ES_DOMAIN': 'azul-index-anvilbox',
                # Personal deployments use fewer Lambda invocations in parallel.
                'AZUL_CONTRIBUTION_CONCURRENCY': '8',
                'AZUL_AGGREGATION_CONCURRENCY': '8',
            }
        ),

        'AZUL_DEBUG': '1',

        'AZUL_BILLING': 'anvil',

        # When using this file as a template for a personal deployment, change
        # `None` to a string containing YOUR email address.
        #
        'AZUL_OWNER': 'hannes@ucsc.edu' if is_sandbox else None,

        'AZUL_MONITORING_EMAIL': '{AZUL_OWNER}',

        'AZUL_AWS_ACCOUNT_ID': '289950828509',
        'AWS_DEFAULT_REGION': 'us-east-1',

        'GOOGLE_PROJECT': 'platform-anvil-dev',

        'AZUL_DEPLOYMENT_INCARNATION': '2',

        'AZUL_GOOGLE_OAUTH2_CLIENT_ID': '561542988117-cpo2avhomdh6t7fetp91js78cdhm9p47.apps.googleusercontent.com',
    }
