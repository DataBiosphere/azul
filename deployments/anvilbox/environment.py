from collections.abc import (
    Mapping,
)
import json
from typing import (
    Literal,
)

is_sandbox = True

pop = 1  # remove snapshot
no_mirror = 2  # do not mirror files from snapshot (redundant for managed access snapshots)

type DatasetName = str
type SourceSpec = str
type SourceConfig = dict[str, str | int | float | bool | None]
type SourceItem = tuple[SourceSpec, SourceConfig]


def source(google_project: str,
           snapshot: str,
           flags: int = 0,
           ) -> tuple[DatasetName, SourceItem | None]:
    assert len(google_project) == 8, google_project
    google_project = 'datarepo-dev-' + google_project
    assert not snapshot.startswith('ANVIL_'), snapshot
    snapshot = 'ANVIL_' + snapshot
    return _source('bigquery', google_project, snapshot, flags)


def _source(source_type: Literal['bigquery', 'parquet'],
            google_project,
            snapshot,
            flags: int = 0,
            ) -> tuple[DatasetName, SourceItem | None]:
    dataset = '_'.join(snapshot.split('_')[1:-3])
    assert flags <= pop | no_mirror
    source = None if flags & pop else (
        ':'.join([
            'tdr',
            source_type,
            'gcp',
            google_project,
            snapshot,
        ]),
        {
            'mirror': not (flags & no_mirror),
        }
    )
    return dataset, source


def delta(items: list[tuple[DatasetName, SourceItem | None]]
          ) -> dict[DatasetName, SourceItem | None]:
    result = dict(items)
    assert len(items) == len(result), 'collisions detected'
    assert list(result.keys()) == sorted(result.keys()), 'input not sorted'
    return result


def condense(catalog: dict[DatasetName, SourceItem | None]
             ) -> dict[SourceSpec, SourceConfig]:
    return dict(filter(None, catalog.values()))


def union(previous_catalog: dict[DatasetName, SourceItem | None],
          num_expected: int,
          delta: dict[DatasetName, SourceItem | None],
          ) -> dict[DatasetName, SourceItem | None]:
    catalog = previous_catalog | delta
    num_actual = len(condense(catalog))
    assert num_expected == num_actual, (num_expected, num_actual)
    return catalog


anvil_sources = union({}, 3, delta([
    # FIXME: Files from 1000G snapshot in anvildev can't be mirrored
    #        https://github.com/DataBiosphere/azul/issues/7634
    source('e53e74aa', '1000G_2019_Dev_20230609_ANV5_202306121732', no_mirror),
    source('42c70e6a', 'CCDG_Sample_1_20230228_ANV5_202302281520'),
    source('dd576076', 'CMG_Sample_1_20230225_ANV5_202512031111')
]))


def env() -> Mapping[str, str | None]:
    """
    Returns a dictionary that maps environment variable names to values. The
    values are either None or strings. String values can contain references to
    other environment variables in the form `{FOO}` where FOO is the name of an
    environment variable. See

    https://docs.python.org/3.12/library/string.html#format-string-syntax

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

        # This deployment uses a subdomain of the `anvildev` deployment's
        # domain.
        #
        'AZUL_DOMAIN_NAME': 'anvil.gi.ucsc.edu',
        'AZUL_SUBDOMAIN_TEMPLATE': '*.{AZUL_DEPLOYMENT_STAGE}',

        'AZUL_CATALOGS': json.dumps({
            f'{catalog}{suffix}': dict(atlas=atlas,
                                       internal=is_it,
                                       mirror_limit=it_mirror_limit if is_it else mirror_limit,
                                       plugins=dict(metadata=dict(name='anvil'),
                                                    repository=dict(name='tdr_anvil')),
                                       sources=condense(sources))
            for atlas, catalog, sources, mirror_limit, it_mirror_limit, in [
                ('anvil', 'anvil', anvil_sources, int(1.5 * 1024 ** 3), int(1.5 * 1024 ** 3)),
            ]
            for suffix, is_it in [
                ('', False),
                ('-it', True)
            ]
        }),

        'AZUL_TDR_SOURCE_LOCATION': 'us-central1',
        'AZUL_TDR_SERVICE_URL': 'https://jade.datarepo-dev.broadinstitute.org',
        'AZUL_SAM_SERVICE_URL': 'https://sam.dsde-dev.broadinstitute.org',
        'AZUL_DUOS_SERVICE_URL': 'https://consent.dsde-dev.broadinstitute.org',
        'AZUL_TERRA_SERVICE_URL': 'https://firecloud-orchestration.dsde-dev.broadinstitute.org',
        'azul_ecm_service_url': 'https://externalcreds.dsde-dev.broadinstitute.org',

        **(
            {
                'AZUL_OPENSEARCH_INSTANCE_TYPE': 'r6gd.large.search',
                'AZUL_OPENSEARCH_INSTANCE_COUNT': '2',
            } if is_sandbox else {
                # Personal deployments share an ES domain with `anvilbox`
                'AZUL_SHARE_OPENSEARCH_DOMAIN': '1',
                'AZUL_OPENSEARCH_DOMAIN': 'azul-index-anvilbox',
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

        'AZUL_ENABLE_MIRRORING': '1',

        # FIXME: Revert, once the underlying issue with requester-pays is fixed
        #        https://github.com/DataBiosphere/azul/issues/7955
        #
        'azul_it_flags': 'no_mirror',
    }
