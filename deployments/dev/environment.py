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

    for the concrete syntax. The references will be resolved after the 
    environment has been compiled by merging all environment.py files.

    Entries with a None value will be excluded from the environment. They should 
    be used to document variables without providing a default value. Other,
    usually more specific environment.py files should provide the value.
    """
    return {
        # Set variables for the `dev` (short for development) deployment here.
        #
        # Only modify this file if you intend to commit those changes. To change the
        # environment with a setting that's specific to you AND the deployment, create
        # a environment.local.py right next to this file and make your changes there.
        # Settings applicable to all environments but specific to you go into
        # environment.local.py at the project root.

        'AZUL_DEPLOYMENT_STAGE': 'dev',

        'AZUL_CATALOGS': 'dcp2:repository/tdr:metadata/hca,'
                         'dcp2ebi:repository/tdr:metadata/hca,'
                         'it2:repository/tdr:metadata/hca,'
                         'it2ebi:repository/tdr:metadata/hca',

        'AZUL_TDR_SOURCE': 'tdr:broad-jade-dev-data:snapshot/hca_dev_20201217_test4___20201218',
        'AZUL_TDR_DCP2EBI_SOURCE': 'tdr:broad-jade-dev-data:snapshot/hca_dev_20201023_ebiv4___20201121',
        'AZUL_TDR_IT2EBI_SOURCE': 'tdr:broad-jade-dev-data:snapshot/hca_dev_20201023_ebiv4___20201121',
        'AZUL_TDR_SERVICE_URL': 'https://jade.datarepo-dev.broadinstitute.org',
        'AZUL_SAM_SERVICE_URL': 'https://sam.dsde-dev.broadinstitute.org',

        'AZUL_DRS_DOMAIN_NAME': 'drs.dev.singlecell.gi.ucsc.edu',

        'AZUL_URL_REDIRECT_BASE_DOMAIN_NAME': 'dev.url.singlecell.gi.ucsc.edu',
        'AZUL_URL_REDIRECT_FULL_DOMAIN_NAME': '{AZUL_DEPLOYMENT_STAGE}.{AZUL_URL_REDIRECT_BASE_DOMAIN_NAME}',

        # $0.372/h × 4 × 24h/d × 30d/mo = $1071.36/mo
        'AZUL_ES_INSTANCE_TYPE': 'r5.xlarge.elasticsearch',
        'AZUL_ES_INSTANCE_COUNT': '4',

        'AZUL_DEBUG': '1',

        'AZUL_OWNER': 'hannes@ucsc.edu',

        'AZUL_AWS_ACCOUNT_ID': '122796619775',
        'AWS_DEFAULT_REGION': 'us-east-1',

        'GOOGLE_PROJECT': 'platform-hca-dev',
    }
