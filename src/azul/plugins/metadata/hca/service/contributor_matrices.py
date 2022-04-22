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
    JSONs,
)

default_order_of_matrix_dimensions = [
    'genusSpecies',
    'developmentStage',
    'organ',
    'libraryConstructionApproach',
]


def parse_strata(strata: str) -> JSONs:
    """
    >>> from azul.doctests import assert_json
    >>> def f(strata):
    ...     return assert_json(parse_strata(strata))

    >>> f('a=A1;b=B1,B2')
    [
        {
            "a": [
                "A1"
            ],
            "b": [
                "B1",
                "B2"
            ]
        }
    ]

    >>> f('a=A1;b=B1\\na=A2;b=B2,B3')
    [
        {
            "a": [
                "A1"
            ],
            "b": [
                "B1"
            ]
        },
        {
            "a": [
                "A2"
            ],
            "b": [
                "B2",
                "B3"
            ]
        }
    ]

    >>> f('')
    Traceback (most recent call last):
    ...
    ValueError: not enough values to unpack (expected 2, got 1)
    """
    return [
        {
            dimension: values.split(',')
            for dimension, values in (point.split('=') for point in stratum.split(';'))
        }
        for stratum in strata.split('\n')
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
    ...             'size': 1,
    ...             'source': 's',
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
                                    "name": "n",
                                    "size": 1,
                                    "source": "s",
                                    "url": null
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
    ...             'size': 1,
    ...             'source': 's1',
    ...             'strata': 'genusSpecies=a;organ=b'
    ...         },
    ...         {
    ...             'uuid': 'u2',
    ...             'version': 'v2',
    ...             'name': 'n2',
    ...             'size': 2,
    ...             'source': 's2',
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
                            "name": "n1",
                            "size": 1,
                            "source": "s1",
                            "url": null
                        },
                        {
                            "uuid": "u2",
                            "version": "v2",
                            "name": "n2",
                            "size": 2,
                            "source": "s2",
                            "url": null
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
    ...             'size': 1,
    ...             'source': 's1',
    ...             'strata': 'genusSpecies=a;organ=b\\ngenusSpecies=c;organ=d'
    ...         },
    ...         {
    ...             'uuid': 'u2',
    ...             'version': 'v2',
    ...             'name': 'n2',
    ...             'size': 2,
    ...             'source': 's2',
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
                            "name": "n1",
                            "size": 1,
                            "source": "s1",
                            "url": null
                        }
                    ],
                    "f": [
                        {
                            "uuid": "u2",
                            "version": "v2",
                            "name": "n2",
                            "size": 2,
                            "source": "s2",
                            "url": null
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
                            "name": "n1",
                            "size": 1,
                            "source": "s1",
                            "url": null
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
                            "name": "n2",
                            "size": 2,
                            "source": "s2",
                            "url": null
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
    ...             'size': 1,
    ...             'source': 's',
    ...             'strata': 'genusSpecies=a;organ=b\\ngenusSpecies=a'
    ...         }
    ...     ]
    ... )
    {
        "genusSpecies": {
            "a": {
                "organ": {
                    "b": [
                        {
                            "uuid": "u",
                            "version": "v",
                            "name": "n",
                            "size": 1,
                            "source": "s",
                            "url": null
                        }
                    ],
                    "Unspecified": [
                        {
                            "uuid": "u",
                            "version": "v",
                            "name": "n",
                            "size": 1,
                            "source": "s",
                            "url": null
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
    ...             'size': 1,
    ...             'source': 's',
    ...             'strata': 'genusSpecies=a;foo=b'
    ...         }
    ...     ]
    ... )
    Traceback (most recent call last):
    ...
    ValueError: 'foo' is not in list
    """
    assert len(set(file['uuid'] for file in files)) == len(files), files

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
                    [(dimension, value) for value in values]
                    for dimension, values in stratum.items()
                )))
                for stratum in parse_strata(file['strata'])
            )),
            'url': None,  # to be injected later in post-processing
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

    # Ensure every stratum of every file has the same dimensions
    for file in files:
        for stratum in file['strata']:
            # FIXME: https://github.com/DataBiosphere/azul/issues/2443
            #        Instead of creating 'Unspecified' nodes the tree branches
            #        should not include those nodes, making the branches shorter
            #        and of different lengths.
            for dimension in set(sorted_dimensions).difference(stratum.keys()):
                stratum[dimension] = 'Unspecified'

    # Build the tree, as a nested dictionary. The keys in the dictionary
    # alternate between dimensions and values. The leaves of the tree are
    # lists of matrix files. If a matrix covers multiple strata, it will occur
    # multiple times in the tree.
    tree = NestedDict(2 * len(sorted_dimensions) - 1, list)
    for file in files:
        for stratum in file['strata']:
            node = tree
            for dimension in sorted_dimensions:
                value = stratum[dimension]
                node = node[dimension][value]
            node.append({k: v for k, v in file.items() if k != 'strata'})

    return tree.to_dict()
