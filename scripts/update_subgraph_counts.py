"""
Count the number of subgraphs per configured source and produce output to
expedite updated source configurations.
"""

import argparse
from concurrent.futures import (
    ThreadPoolExecutor,
)
import sys
from typing import (
    Optional,
)

import attr

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
from azul.openapi import (
    format_description,
)
from azul.terra import (
    SourceRef as TDRSourceRef,
    TDRClient,
)


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class SourceSpecArgs:
    project: str
    snapshot: str
    subgraph_count: int

    def __str__(self) -> str:
        return f'mksrc({self.project!r}, {self.snapshot!r}, {self.subgraph_count!r})'


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
        spec = attr.evolve(source.spec, prefix=Prefix.of_everything)
        source = attr.evolve(source, spec=spec)
        partitions = plugin.list_partitions(source)
        return SourceSpecArgs(project=spec.project,
                              snapshot=spec.name,
                              subgraph_count=sum(partitions.values()))

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
        flags should be applied. If `mksrc` generates a common prefix, manual
        adjustment of the generated common prefix may be required.
    '''))


if __name__ == '__main__':
    main(sys.argv[1:])
