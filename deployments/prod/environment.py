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

        'AZUL_CATALOGS': ','.join([
            'dcp2:repository/tdr:metadata/hca',
            'it2:repository/tdr:metadata/hca',
            'dcp1:repository/tdr:metadata/hca',
            'it1:repository/tdr:metadata/hca',
        ]),

        'AZUL_DSS_ENDPOINT': 'https://dss.data.humancellatlas.org/v1',
        'AZUL_DSS_DIRECT_ACCESS': '1',
        'AZUL_DSS_DIRECT_ACCESS_ROLE': 'arn:aws:iam::109067257620:role/azul-sc',
        'AZUL_SUBSCRIBE_TO_DSS': '0',

        'AZUL_TDR_SOURCE': 'tdr:broad-datarepo-terra-prod-hca2:snapshot/hca_prod_20201120_dcp2___20201124',
        'AZUL_TDR_DCP1_SOURCE': 'tdr:broad-datarepo-terra-prod-hca2:snapshot/hca_prod_20201118_dcp1___20201202',
        'AZUL_TDR_IT1_SOURCE': 'tdr:broad-datarepo-terra-prod-hca2:snapshot/hca_prod_20201118_dcp1___20201202',
        'AZUL_TDR_SERVICE_URL': 'https://jade-terra.datarepo-prod.broadinstitute.org',
        'AZUL_SAM_SERVICE_URL': 'https://sam.dsde-prod.broadinstitute.org',

        'AZUL_URL_REDIRECT_BASE_DOMAIN_NAME': 'azul.data.humancellatlas.org',
        'AZUL_URL_REDIRECT_FULL_DOMAIN_NAME': 'url.{AZUL_URL_REDIRECT_BASE_DOMAIN_NAME}',

        # $0.372/h × 4 × 24h/d × 30d/mo = $1071.36/mo
        'AZUL_ES_INSTANCE_TYPE': 'r5.xlarge.elasticsearch',
        'AZUL_ES_INSTANCE_COUNT': '4',

        'AZUL_DEBUG': '1',

        'AZUL_OWNER': 'hannes@ucsc.edu',

        'AZUL_AWS_ACCOUNT_ID': '542754589326',
        'AWS_DEFAULT_REGION': 'us-east-1',

        'GOOGLE_PROJECT': 'platform-hca-prod',
    }
