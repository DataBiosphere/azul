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
        # Set variables for the personal `yourname` deployment here.
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
        
        'AZUL_DEPLOYMENT_STAGE': '<yourname>',
        
        'AZUL_DSS_ENDPOINT': 'https://dss.staging.data.humancellatlas.org/v1',
        'AZUL_DSS_DIRECT_ACCESS': '1',
        'AZUL_DSS_DIRECT_ACCESS_ROLE': 'arn:aws:iam::861229788715:role/azul-{{lambda_name}}-staging',
        
        # A personal deployment uses a subdomain of the `dev` deployment's domain.
        #
        'AZUL_DOMAIN_NAME': 'dev.singlecell.gi.ucsc.edu',
        'AZUL_SUBDOMAIN_TEMPLATE': '{{lambda_name}}.{AZUL_DEPLOYMENT_STAGE}',
        
        'AZUL_URL_REDIRECT_BASE_DOMAIN_NAME': 'dev.url.singlecell.gi.ucsc.edu',
        'AZUL_URL_REDIRECT_FULL_DOMAIN_NAME': '{AZUL_DEPLOYMENT_STAGE}.{AZUL_URL_REDIRECT_BASE_DOMAIN_NAME}',
        
        # A personal deployment shares an ES domain with `dev`
        #
        'AZUL_SHARE_ES_DOMAIN': '1',
        'AZUL_ES_DOMAIN': 'azul-index-dev',
        
        'azul_dss_query_prefix': '42',
        
        'AZUL_DEBUG': '1',
        
        'AZUL_OWNER': '<your_email>',
        
        'AWS_DEFAULT_REGION': 'us-east-1',
        
        # Indexer Lambdas in personal deployments do not require default reserved concurrency
        'AZUL_INDEXER_CONCURRENCY': '8',
        
        # See also GOOGLE_APPLICATION_CREDENTIALS in .example.local/environment.local
        #
        'GOOGLE_PROJECT': 'human-cell-atlas-travis-test',
    }
