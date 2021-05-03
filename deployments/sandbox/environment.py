from typing import (
    Mapping,
    Optional,
)

is_sandbox = '/sandbox/' in __file__


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
        # Set variables for the `sandbox` deployment here. The sandbox is used
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
        # `None` with a short string that is specific to to YOU.
        #
        'AZUL_DEPLOYMENT_STAGE': 'sandbox' if is_sandbox else None,

        'AZUL_CATALOGS': ','.join([
            'hca:dcp2:repository/tdr:metadata/hca',
            'hca:dcp2ebi:repository/tdr:metadata/hca',
            'lungmap:lungmap:repository/tdr:metadata/hca',
            'hca:it2:repository/tdr:metadata/hca',
            'hca:it2ebi:repository/tdr:metadata/hca',
            'lungmap:it3lungmap:repository/tdr:metadata/hca'
        ]),

        # FIXME: Add tooling to aid in prefix choice
        #        https://github.com/DataBiosphere/azul/issues/3027
        'AZUL_TDR_SOURCES': ','.join([
            'tdr:broad-jade-dev-data:snapshot/hca_dev_20201203___20210426:42',
        ]),
        **{
            f'AZUL_TDR_{catalog.upper()}_SOURCES': ','.join([
                f'tdr:broad-jade-dev-data:snapshot/hca_dev_20201023_ebiv4___20210302:4'
            ])
            for catalog in ('dcp2ebi', 'it2ebi')
        },
        **{
            f'AZUL_TDR_{catalog.upper()}_SOURCES': ','.join([
                'tdr:broad-jade-dev-data:snapshot/lungmap_dev_20210412__20210414:',
            ])
            for catalog in ('lungmap', 'it3lungmap')
        },
        'AZUL_TDR_SERVICE_URL': 'https://jade.datarepo-dev.broadinstitute.org',
        'AZUL_SAM_SERVICE_URL': 'https://sam.dsde-dev.broadinstitute.org',

        # This deployment uses a subdomain of the `dev` deployment's domain.
        #
        'AZUL_DOMAIN_NAME': 'dev.singlecell.gi.ucsc.edu',
        'AZUL_SUBDOMAIN_TEMPLATE': '*.{AZUL_DEPLOYMENT_STAGE}',

        'AZUL_DRS_DOMAIN_NAME': 'drs.{AZUL_DEPLOYMENT_STAGE}.dev.singlecell.gi.ucsc.edu',

        'AZUL_URL_REDIRECT_BASE_DOMAIN_NAME': 'dev.url.singlecell.gi.ucsc.edu',
        'AZUL_URL_REDIRECT_FULL_DOMAIN_NAME': '{AZUL_DEPLOYMENT_STAGE}.{AZUL_URL_REDIRECT_BASE_DOMAIN_NAME}',

        **(
            {
                # $0.186/h × 2 × 24h/d × 30d/mo = $267.84/mo
                'AZUL_ES_INSTANCE_TYPE': 'r5.large.elasticsearch',
                'AZUL_ES_INSTANCE_COUNT': '2',
            } if is_sandbox else {
                # Personal deployments share an ES domain with `sandbox`
                'AZUL_SHARE_ES_DOMAIN': '1',
                'AZUL_ES_DOMAIN': 'azul-index-sandbox',
                # Personal deployments use fewer Lambda invocations in parallel.
                'AZUL_INDEXER_CONCURRENCY': '8',
            }
        ),

        'AZUL_DSS_QUERY_PREFIX': '42',

        'AZUL_DEBUG': '1',

        # When using this file as a template for a personal deployment, change
        # `None` to a string contaiing YOUR email address.
        #
        'AZUL_OWNER': 'hannes@ucsc.edu' if is_sandbox else None,

        'AZUL_AWS_ACCOUNT_ID': '122796619775',
        'AWS_DEFAULT_REGION': 'us-east-1',

        # Set `GOOGLE_APPLICATION_CREDENTIALS` in `environment.local.py`
        #
        'GOOGLE_PROJECT': 'platform-hca-dev',
    }
