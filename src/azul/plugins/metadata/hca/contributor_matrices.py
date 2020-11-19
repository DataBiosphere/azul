from collections import (
    defaultdict,
)
from itertools import (
    chain,
    product,
)
from typing import (
    Mapping,
    Sequence,
)

from azul.collections import (
    NestedDict,
)
from azul.types import (
    JSON,
)


def make_contributor_matrices_tree(files: Sequence[Mapping[str, str]],
                                   ) -> JSON:
    """
    >>> from azul.doctests import assert_json
    >>> def f(files):
    ...     return assert_json(make_contributor_matrices_tree(files))

    >>> f([{'uuid': 'u', 'version': 'v', 'name': 'n', 'strata': 'a=1;b=2'}])
    {
        "a": {
            "1": {
                "b": {
                    "2": {
                        "files": [
                            {
                                "uuid": "u",
                                "version": "v",
                                "name": "n"
                            }
                        ]
                    }
                }
            }
        }
    }

    >>> f([{'uuid': 'u1', 'version': 'v1', 'name': 'n1', 'strata': 'a=1;b=2'},
    ...    {'uuid': 'u2', 'version': 'v2', 'name': 'n2', 'strata': 'a=1;b=2'}])
    {
        "a": {
            "1": {
                "b": {
                    "2": {
                        "files": [
                            {
                                "uuid": "u1",
                                "version": "v1",
                                "name": "n1"
                            },
                            {
                                "uuid": "u2",
                                "version": "v2",
                                "name": "n2"
                            }
                        ]
                    }
                }
            }
        }
    }

    >>> f([{'uuid': 'u1', 'version': 'v1', 'name': 'n1', 'strata': 'a=1;b=2\\na=3;b=4'},
    ...    {'uuid': 'u2', 'version': 'v2', 'name': 'n2', 'strata': 'a=1,5;b=7'}])
    {
        "a": {
            "1": {
                "b": {
                    "2": {
                        "files": [
                            {
                                "uuid": "u1",
                                "version": "v1",
                                "name": "n1"
                            }
                        ]
                    },
                    "7": {
                        "files": [
                            {
                                "uuid": "u2",
                                "version": "v2",
                                "name": "n2"
                            }
                        ]
                    }
                }
            },
            "3": {
                "b": {
                    "4": {
                        "files": [
                            {
                                "uuid": "u1",
                                "version": "v1",
                                "name": "n1"
                            }
                        ]
                    }
                }
            },
            "5": {
                "b": {
                    "7": {
                        "files": [
                            {
                                "uuid": "u2",
                                "version": "v2",
                                "name": "n2"
                            }
                        ]
                    }
                }
            }
        }
    }

    >>> f([{'uuid': 'u', 'version': 'v', 'name': 'n', 'strata': 'a=1;b=2\\na=1'}])
    Traceback (most recent call last):
    ...
    AssertionError: ['a', 'b']
    """
    for key in 'uuid', 'name':
        assert len(set(file[key] for file in files)) == len(files), files

    distinct_values = defaultdict(set)
    for file in files:
        # Each line in the stratification string represents a stratum,
        # each stratum is a list of points, each point has a dimension
        # and a list of values. Transform that string into a list of
        # dictionaries. Each entry in those dictionaries maps the
        # dimension to a value in that dimension. If dimension in a
        # stratum has multiple values, the stratum is expanded into
        # multiple strata, one per value. The strata are identical
        # except in the dimension that had the multiple values.
        file['strata'] = list(chain.from_iterable(
            map(dict, product(*(
                [(dimension, value) for value in values.split(',')]
                for dimension, values in (point.split('=') for point in stratum.split(';'))
            )))
            for stratum in file['strata'].split('\n')
        ))
        for stratum in file['strata']:
            for dimension, value in stratum.items():
                distinct_values[dimension].add(value)

    # To produce a tree with the most shared base branches possible we sort
    # the dimensions by number of distinct values on that dimension.
    sorted_dimensions = sorted(distinct_values,
                               key=lambda k: len(distinct_values[k]))

    # Verify that every stratum uses the same dimensions
    # FIXME: Allow CGM stratification tree with varying dimensions
    # https://github.com/DataBiosphere/azul/issues/2443
    for file in files:
        for stratum in file['strata']:
            assert set(sorted_dimensions) == stratum.keys(), sorted_dimensions

    # Build the tree, as a nested dictionary. The keys in the dictionary
    # alternate between dimensions and values. The leaves of the tree are
    # lists of matrix files. If a matrix covers multiple strata, its
    # URL will occur multiple times in the tree.
    tree = NestedDict(2 * len(sorted_dimensions), list)
    for file in files:
        for stratum in file['strata']:
            node = tree
            for dimension in sorted_dimensions:
                value = stratum.get(dimension)
                if value is not None:
                    node = node[dimension][value]
            node['files'].append(
                {
                    key: file[key]
                    for key in ('uuid', 'version', 'name')
                }
            )

    return tree
