from typing import Optional, Mapping


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
        'AWS_PROFILE': 'yourprofile',
        
        # If you have the credentials of a service account, uncomment this line, and
        # change it point to a file with the credentials to that service account.
        # The account must have owner permissions to the project referenced by
        # the GOOGLE_PROJECT environment variable. The project (not the account) also
        # needs to be whitelisted in the DSS instance you are using for subscriptions to
        # work.
        #
        # Once those prerequisites are satisfied, set AZUL_SUBSCRIBE_TO_DSS to 1.
        #
        # The account whose credentials you specify here is used to create yet another
        # service account, one that represents the Azul indexer in your deployment. The
        # indexer's service account credentials are stored in AWS secrets manager.
        #
        # 'GOOGLE_APPLICATION_CREDENTIALS': '/path/to/your/gcp-credentials.json'
    }
