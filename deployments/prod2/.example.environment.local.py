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

    for the concrete syntax. These references will be resolved *after* the
    overall environment has been compiled by merging all relevant
    `environment.py` and `environment.local.py` files.

    Entries with a `None` value will be excluded from the environment. They
    can be used to document a variable without a default value in which case
    other, more specific `environment.py` or `environment.local.py` files must
    provide the value.
    """
    return {

        # In the AWS IAM console, create an access key and list it in a
        # dedicated configuration profile section of `~/.aws/config` and/or
        # `~/.aws/credentials`. Specify the name of the profile here.
        #
        'AWS_PROFILE': 'yourprofile',

        # Create a personal Google service account and obtain a private key for
        # it (as described in the README). Then modify this variable such that
        # it points to the file containing that private key.
        #
        'GOOGLE_APPLICATION_CREDENTIALS': '/path/to/your/private-key.json'
    }
