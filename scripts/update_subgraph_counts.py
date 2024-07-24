"""
Count the number of subgraphs per configured source and produce output to
expedite updated source configurations.
"""

import argparse
from concurrent.futures import (
    ThreadPoolExecutor,
)
import os
import sys
from typing import (
    Mapping,
    Optional,
)

import attr
from more_itertools import (
    first,
)

from azul import (
    CatalogName,
    config,
)
from azul.args import (
    AzulArgumentHelpFormatter,
)
from azul.auth import (
    OAuth2,
)
from azul.azulclient import (
    AzulClient,
)
from azul.indexer import (
    Prefix,
)
from azul.modules import (
    load_module,
)
from azul.openapi import (
    format_description,
)
from azul.plugins import (
    RepositoryPlugin,
)
from azul.terra import (
    SourceRef as TDRSourceRef,
    TDRClient,
)

environment = load_module(module_name='environment',
                          path=os.path.join(config.project_root,
                                            'deployments',
                                            '.active',
                                            'environment.py'))


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class SubgraphCounter:
    partition_sizes: Mapping[str, int]

    @classmethod
    def for_source(cls,
                   plugin: RepositoryPlugin,
                   source: TDRSourceRef,
                   prefix: Prefix
                   ) -> 'SubgraphCounter':
        spec = attr.evolve(source.spec, prefix=prefix)
        source = attr.evolve(source, spec=spec)
        return cls(partition_sizes=plugin.list_partitions(source))

    @property
    def count(self) -> int:
        return sum(self.partition_sizes.values())

    def default_common_prefix(self) -> str:
        """
        The common prefix that will be used for the supplied source if none is
        explicitly configured.
        """
        try:
            func = getattr(environment, 'common_prefix')
        except AttributeError:
            assert config.deployment.is_main, environment.__path__
            return ''
        else:
            return func(self.count)

    def ideal_common_prefix(self) -> str:
        """
        A common prefix of the same length as the :meth:`default_common_prefix`
        that yields an optimally sized, nonempty partition of subgraphs in the
        supplied source.
        """
        best_prefix, best_count = first(self.partition_sizes.items())
        ideal_size = 16
        for prefix, count in sorted(self.partition_sizes.items()):
            assert count > 0, self.partition_sizes
            if abs(count - ideal_size) < abs(best_count - ideal_size):
                best_prefix, best_count = prefix, count
        return best_prefix


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class SourceSpecArgs:
    subdomain: str
    name: str
    subgraph_count: int
    explicit_prefix: Optional[str]

    def __str__(self) -> str:
        params = f'{self.subdomain!r}, {self.name!r}, {self.subgraph_count!r}'
        if self.explicit_prefix is not None:
            params += f', prefix={self.explicit_prefix!r}'
        return f'mksrc({params})'


indexer_auth = OAuth2(TDRClient.for_indexer().credentials.token)


def generate_sources(catalog: CatalogName,
                     old_catalog: Optional[CatalogName] = None
                     ) -> list[SourceSpecArgs]:
    client = AzulClient()
    plugin = client.repository_plugin(catalog)
    sources = plugin.list_sources(indexer_auth)
    if old_catalog is not None:
        old_plugin = client.repository_plugin(old_catalog)
        sources = filter(lambda ref: ref.spec not in old_plugin.sources, sources)
    sources = sorted(sources,
                     key=lambda ref: ref.spec.name)

    def generate_source(source: TDRSourceRef) -> SourceSpecArgs:
        explicit_prefix = None
        counter = SubgraphCounter.for_source(plugin, source, Prefix.of_everything)
        default_prefix = counter.default_common_prefix()
        if len(default_prefix) > 0:
            counter_prefix = Prefix(common='', partition=len(default_prefix))
            prefixed_counter = SubgraphCounter.for_source(plugin, source, counter_prefix)
            if default_prefix not in prefixed_counter.partition_sizes:
                explicit_prefix = prefixed_counter.ideal_common_prefix()
        return SourceSpecArgs(subdomain=source.spec.subdomain,
                              name=source.spec.name,
                              subgraph_count=counter.count,
                              explicit_prefix=explicit_prefix)

    with ThreadPoolExecutor(max_workers=8) as tpe:
        yield from tpe.map(generate_source, sources)


def main(args: list[str]):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=AzulArgumentHelpFormatter)

    parser.add_argument('--catalog',
                        metavar='NAME',
                        default=config.default_catalog,
                        help='The name of the catalogs to determine source specs for.')
    parser.add_argument('--old-catalog',
                        metavar='NAME',
                        default=None,
                        help='If the chosen catalog is based on an older catalog, this option can be '
                             'used to exclude the sources from the older catalog.')

    args = parser.parse_args(args)

    print(args.catalog)
    print('-' * len(args.catalog))
    sep = ''
    for spec_args in generate_sources(args.catalog, args.old_catalog):
        print(f'{sep}{spec_args}', end='')
        sep = ',\n'
    print()

    print(format_description('''
        -----------------
        !!! IMPORTANT !!!
        -----------------

        This script does *not* populate the `ma` or `pop` flags for the source
        specs. Do not copy/paste the above output without checking whether these
        flags should be applied.
    '''))


if __name__ == '__main__':
    main(sys.argv[1:])
