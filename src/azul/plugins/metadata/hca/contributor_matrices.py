from collections import (
    defaultdict,
)
from itertools import (
    chain,
    product,
)

from furl import (
    furl,
)

from azul import (
    config,
)
from azul.collections import (
    NestedDict,
)


class ContributorMatrices:

    @classmethod
    def make_contributor_matrices(cls, catalog, files):
        """
        >>> from azul.doctests import assert_json
        >>> def f(files):
        ...     return assert_json(ContributorMatrices.make_contributor_matrices('dcp', files))

        >>> f([{'uuid':'1', 'version': '2', 'name': 3, 'strata': "y=5,6;x=4\\nx=7;y=8"}])
        {
            "x": {
                "4": {
                    "y": {
                        "5": {
                            "url": [
                                "https://service.dev.singlecell.gi.ucsc.edu/fetch/repository/files/1?version=2&catalog=dcp"
                            ]
                        },
                        "6": {
                            "url": [
                                "https://service.dev.singlecell.gi.ucsc.edu/fetch/repository/files/1?version=2&catalog=dcp"
                            ]
                        }
                    }
                },
                "7": {
                    "y": {
                        "8": {
                            "url": [
                                "https://service.dev.singlecell.gi.ucsc.edu/fetch/repository/files/1?version=2&catalog=dcp"
                            ]
                        }
                    }
                }
            }
        }
        """
        for key in 'uuid', 'name':
            assert len(set(file[key] for file in files)) == len(files), files

        files = [
            {
                # Each line in the stratification string represents a stratum,
                # each stratum is a list of points, each point has a dimension
                # and a list of values. Transform that string into a list of
                # dictionaries. Each entry in those dictionaries maps the
                # dimension to a value in that dimension. If dimension in a
                # stratum has multiple values, the stratum is expanded into
                # multiple strata, one per value. The strata are identical
                # except in the dimension that had the multiple values.
                'strata': list(chain.from_iterable(
                    map(dict, product(*(
                        [(dimension, value) for value in values.split(',')]
                        for dimension, values in (point.split('=') for point in stratum.split(';'))
                    )))
                    for stratum in file['strata'].split('\n')
                )),
                'url': furl(config.service_endpoint(),
                            path=('fetch', 'repository', 'files', file['uuid']),
                            args=dict(version=file['version'],
                                      catalog=catalog))
            }
            for file in files
        ]

        # To produce a tree with the most shared base branches possible we sort
        # the dimensions by number of distinct values on that dimension.
        distinct_values = defaultdict(set)
        for file in files:
            for stratum in file['strata']:
                for dimension, value in stratum.items():
                    distinct_values[dimension].add(value)
        sorted_dimensions = sorted(distinct_values,
                                   key=lambda k: len(distinct_values[k]))

        # Build the tree, as a nested dictionary. The keys in the dictionary
        # alternate between dimensions and values. The leaves of the tree are
        # lists of matrix file URLs. If a matrix covers multiple strata, its
        # URL will occur multiple times in the tree.
        tree = NestedDict(2 * len(sorted_dimensions), list)
        for file in files:
            for stratum in file['strata']:
                node = tree
                for dimension in sorted_dimensions:
                    value = stratum.get(dimension)
                    if value is not None:
                        node = node[dimension][value]
                node['url'].append(file['url'].url)

        return tree
