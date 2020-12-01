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
    Tuple,
)

from azul.collections import (
    NestedDict,
)
from azul.types import (
    JSON,
)

default_order_of_matrix_dimensions = [
    'genusSpecies',
    'developmentStage',
    'organ',
    'libraryConstructionApproach',
]


def make_stratification_tree(files: Sequence[Mapping[str, str]]) -> JSON:
    """
    >>> from azul.doctests import assert_json
    >>> def f(files):
    ...     return assert_json(make_stratification_tree(files))

    >>> f(
    ...     [
    ...         {
    ...             'uuid': 'u',
    ...             'version': 'v',
    ...             'name': 'n',
    ...             'strata': 'developmentStage=a;genusSpecies=b;organ=c'
    ...         }
    ...     ]
    ... )
    {
        "genusSpecies": {
            "b": {
                "developmentStage": {
                    "a": {
                        "organ": {
                            "c": [
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
    }

    >>> f(
    ...     [
    ...         {
    ...             'uuid': 'u1',
    ...             'version': 'v1',
    ...             'name': 'n1',
    ...             'strata': 'genusSpecies=a;organ=b'
    ...         },
    ...         {
    ...             'uuid': 'u2',
    ...             'version': 'v2',
    ...             'name': 'n2',
    ...             'strata': 'genusSpecies=a;organ=b'
    ...         }
    ...     ]
    ... )
    {
        "genusSpecies": {
            "a": {
                "organ": {
                    "b": [
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

    >>> f(
    ...     [
    ...         {
    ...             'uuid': 'u1',
    ...             'version': 'v1',
    ...             'name': 'n1',
    ...             'strata': 'genusSpecies=a;organ=b\\ngenusSpecies=c;organ=d'
    ...         },
    ...         {
    ...             'uuid': 'u2',
    ...             'version': 'v2',
    ...             'name': 'n2',
    ...             'strata': 'genusSpecies=a,e;organ=f'
    ...         }
    ...     ]
    ... )
    {
        "genusSpecies": {
            "a": {
                "organ": {
                    "b": [
                        {
                            "uuid": "u1",
                            "version": "v1",
                            "name": "n1"
                        }
                    ],
                    "f": [
                        {
                            "uuid": "u2",
                            "version": "v2",
                            "name": "n2"
                        }
                    ]
                }
            },
            "c": {
                "organ": {
                    "d": [
                        {
                            "uuid": "u1",
                            "version": "v1",
                            "name": "n1"
                        }
                    ]
                }
            },
            "e": {
                "organ": {
                    "f": [
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

    >>> f(
    ...     [
    ...         {
    ...             'uuid': 'u',
    ...             'version': 'v',
    ...             'name': 'n',
    ...             'strata': 'genusSpecies=a;organ=b\\ngenusSpecies=a'
    ...         }
    ...     ]
    ... )
    Traceback (most recent call last):
    ...
    AssertionError: ['genusSpecies', 'organ']

    >>> f(
    ...     [
    ...         {
    ...             'uuid': 'u',
    ...             'version': 'v',
    ...             'name': 'n',
    ...             'strata': 'genusSpecies=a;foo=b'
    ...         }
    ...     ]
    ... )
    Traceback (most recent call last):
    ...
    ValueError: 'foo' is not in list
    """
    for key in 'uuid', 'name':
        assert len(set(file[key] for file in files)) == len(files), files

    files = [
        {
            **file,
            # Each line in the stratification string represents a stratum,
            # each stratum is a list of points, each point has a dimension
            # and a list of values. Transform that string into a list of
            # dictionaries. Each entry in those dictionaries maps the
            # dimension to a value in that dimension. If dimension in a
            # stratum has multiple values, the stratum is expanded into
            # multiple strata, one per value. The strata are identical except
            # in the dimension that had the multiple values.
            'strata': list(chain.from_iterable(
                map(dict, product(*(
                    [(dimension, value) for value in values.split(',')]
                    for dimension, values in (point.split('=') for point in stratum.split(';'))
                )))
                for stratum in file['strata'].split('\n')
            )),
        }
        for file in files
    ]

    def dimension_placement(dimension: str) -> Tuple[int, int]:
        dimension_index = default_order_of_matrix_dimensions.index(dimension)
        return len(distinct_values[dimension]), dimension_index

    # To produce a tree with the most shared base branches possible we sort
    # the dimensions by number of distinct values on each dimension, and
    # secondarily sort according to the defined default ordering.
    distinct_values = defaultdict(set)
    for file in files:
        for stratum in file['strata']:
            for dimension, value in stratum.items():
                distinct_values[dimension].add(value)
    sorted_dimensions = sorted(distinct_values, key=dimension_placement)

    # Verify that every stratum uses the same dimensions
    # FIXME: Allow CGM stratification tree with varying dimensions
    # https://github.com/DataBiosphere/azul/issues/2443
    for file in files:
        for stratum in file['strata']:
            assert set(sorted_dimensions) == stratum.keys(), sorted_dimensions

    # Build the tree, as a nested dictionary. The keys in the dictionary
    # alternate between dimensions and values. The leaves of the tree are
    # lists of matrix files. If a matrix covers multiple strata, it will occur
    # multiple times in the tree.
    tree = NestedDict(2 * len(sorted_dimensions) - 1, list)
    for file in files:
        for stratum in file['strata']:
            node = tree
            for dimension in sorted_dimensions:
                value = stratum.get(dimension)
                if value is not None:
                    node = node[dimension][value]
            node.append({k: v for k, v in file.items() if k != 'strata'})

    return tree
