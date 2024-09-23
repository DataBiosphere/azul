from collections.abc import (
    Mapping,
)
import json
from typing import (
    Optional,
)


def env() -> Mapping[str, Optional[str]]:
    """
    Returns a dictionary that maps environment variable names to values. The
    values are either None or strings. String values can contain references to
    other environment variables in the form `{FOO}` where FOO is the name of an
    environment variable. See

    https://docs.python.org/3.11/library/string.html#format-string-syntax

    for the concrete syntax. These references will be resolved *after* the
    overall environment has been compiled by merging all relevant
    `environment.py` and `environment.local.py` files.

    Entries with a `None` value will be excluded from the environment. They
    can be used to document a variable without a default value in which case
    other, more specific `environment.py` or `environment.local.py` files must
    provide the value.
    """
    return {
        'azul_terraform_component': 'browser',
        'azul_browser_sites': json.dumps({
            'browser': {
                'zone': 'explore.data.humancellatlas.org',
                'domain': 'explore.data.humancellatlas.org',
                'project': 'ucsc/data-browser',
                'branch': 'ucsc/hca/prod',
                'tarball_name': 'hca',
                'tarball_path': 'out',
                'real_path': ''
            },
            'lungmap': {
                'zone': 'data-browser.lungmap.net',
                'domain': 'data-browser.lungmap.net',
                'project': 'ucsc/data-browser',
                'branch': 'ucsc/lungmap/prod',
                'tarball_name': 'lungmap',
                'tarball_path': 'out',
                'real_path': ''
            }
        })
    }