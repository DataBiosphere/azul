import json
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

        'AZUL_CATALOGS': json.dumps({
            name: dict(atlas=atlas,
                       internal=internal,
                       plugins=dict(metadata=dict(name='hca'),
                                    repository=dict(name='tdr')))
            for name, atlas, internal in [
                ('dcp2', 'hca', False),
                ('lungmap', 'lungmap', False),
                ('it2', 'hca', True),
                ('it3lungmap', 'lungmap', True)
            ]
        }),

        'AZUL_PARTITION_PREFIX_LENGTH': '1',

        'AZUL_TDR_SOURCES': ','.join([
            'tdr:datarepo-dev-a9252919:snapshot/hca_dev_005d611a14d54fbf846e571a1f874f70__20210827_20210903:4',
            'tdr:datarepo-dev-78bae095:snapshot/hca_dev_0fd8f91862d64b8bac354c53dd601f71__20210830_20210903:4',
            'tdr:datarepo-dev-1c2c69d9:snapshot/hca_dev_24c654a5caa5440a8f02582921f2db4a__20210830_20210903:4',
            'tdr:datarepo-dev-bdc9f342:snapshot/hca_dev_3e329187a9c448ec90e3cc45f7c2311c__20210901_20210903:4',
            'tdr:datarepo-dev-71de019e:snapshot/hca_dev_520afa10f9d24e93ab7a26c4c863ce18__20210827_20210928:4',
            'tdr:datarepo-dev-12b7a9e1:snapshot/hca_dev_7880637a35a14047b422b5eac2a2a358__20210901_20210903:4',
            'tdr:datarepo-dev-a198b032:snapshot/hca_dev_90bd693340c048d48d76778c103bf545__20210827_20210903:4',
            # Managed access:
            'tdr:datarepo-dev-02c59b72:snapshot/hca_dev_99101928d9b14aafb759e97958ac7403__20210830_20210903:4',
            # Managed access:
            'tdr:datarepo-dev-d4b988d6:snapshot/hca_dev_a004b1501c364af69bbd070c06dbc17d__20210830_20210903:',
            'tdr:datarepo-dev-7b7daff7:snapshot/hca_dev_a96b71c078a742d188ce83c78925cfeb__20210827_20210902:4',
            'tdr:datarepo-dev-71926fdc:snapshot/hca_dev_c893cb575c9f4f26931221b85be84313__20210901_20210903:4',
            'tdr:datarepo-dev-dbc582d9:snapshot/hca_dev_dbcd4b1d31bd4eb594e150e8706fa192__20210827_20210902:4',
            'tdr:datarepo-dev-10f0610a:snapshot/hca_dev_f81efc039f564354aabb6ce819c3d414__20210827_20210903:4'
        ]),
        'AZUL_TDR_SOURCE_LOCATION': 'us-central1',
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
                # $0.372/h × 2 × 24h/d × 30d/mo = $535.68/mo
                'AZUL_ES_INSTANCE_TYPE': 'r5.xlarge.elasticsearch',
                'AZUL_ES_INSTANCE_COUNT': '2',
            } if is_sandbox else {
                # Personal deployments share an ES domain with `sandbox`
                'AZUL_SHARE_ES_DOMAIN': '1',
                'AZUL_ES_DOMAIN': 'azul-index-sandbox',
                # Personal deployments use fewer Lambda invocations in parallel.
                'AZUL_CONTRIBUTION_CONCURRENCY': '8',
                'AZUL_AGGREGATION_CONCURRENCY': '8',
            }
        ),

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

        'AZUL_GOOGLE_OAUTH2_CLIENT_ID': '713613812354-3bj4m7vnsbco82bke96idvg8cpdv6r9r.apps.googleusercontent.com',
    }
