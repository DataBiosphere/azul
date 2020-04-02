from typing import Optional, Mapping


def env() -> Mapping[str, Optional[str]]:
    """
    Returns a dictionary that maps environment variable names to values. The 
    values are either None or strings. String values can contain references to 
    other environment variables in the form `{FOO}` where FOO is the name of an 
    environment variable. See 

    https://docs.python.org/3.6/library/string.html#format-string-syntax

    for the concrete syntax. The references will be resolved after the 
    environment has been compiled by merging all environment.py files.

    Entries with a None value will be excluded from the environment. They should 
    be used to document variables without providing a default value. Other,
    usually more specific environment.py files should provide the value.
    """ 
    return {
        # Set variables for the `dev` (short for development) deployment here.
        #
        # To define an AZUL_… variable use `_set AZUL_FOO bar`. For all other
        # variables use `export BAR=baz`.
        #
        # Only modify this file if you intend to commit those changes. To change the
        # environment with a setting that's specific to you AND the deployment, create
        # a environment.local right next to this file and make your changes there. In
        # that file the same rules apply with regard to `_set` vs `export`. Settings
        # applicable to all environments but specific to you go into environment.local
        # at the project root.
        
        'AZUL_DEPLOYMENT_STAGE': 'dev',
        
        'AZUL_DSS_ENDPOINT': 'https://dss.staging.data.humancellatlas.org/v1',
        'AZUL_DSS_DIRECT_ACCESS': '1',
        'AZUL_DSS_DIRECT_ACCESS_ROLE': 'arn:aws:iam::861229788715:role/azul-{{lambda_name}}-staging',
        'AZUL_SUBSCRIBE_TO_DSS': '1',
        
        'AZUL_DRS_DOMAIN_NAME': 'drs.dev.singlecell.gi.ucsc.edu',
        
        'AZUL_URL_REDIRECT_BASE_DOMAIN_NAME': 'dev.url.singlecell.gi.ucsc.edu',
        'AZUL_URL_REDIRECT_FULL_DOMAIN_NAME': '{AZUL_DEPLOYMENT_STAGE}.{AZUL_URL_REDIRECT_BASE_DOMAIN_NAME}',
        
        # $0.392/h × 2 × 24h/d × 30d/mo = $564.48/mo
        'AZUL_ES_INSTANCE_TYPE': 'r4.xlarge.elasticsearch',
        'AZUL_ES_INSTANCE_COUNT': '2',
        
        'AZUL_DEBUG': '1',
        
        'AZUL_OWNER': 'hannes@ucsc.edu',
        
        'AWS_DEFAULT_REGION': 'us-east-1',
        
        'GOOGLE_PROJECT': 'human-cell-atlas-travis-test',
        
        'AZUL_EXTERNAL_LAMBDA_ROLE_ASSUMPTORS': '122796619775,administrator,developer,azul-gitlab,azul-service-*,azul-indexer-*',
    }
