"""
Count the number of subgraphs per configured source and produce output to
expedite updated source configurations.
"""

import argparse
from concurrent.futures import (
    ThreadPoolExecutor,
)
import sys

import attr

from azul import (
    config,
)
from azul.args import (
    AzulArgumentHelpFormatter,
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
    TDRSourceSpec,
)


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class SourceSpecArgs:
    project: str
    snapshot: str
    subgraph_count: int

    def __str__(self) -> str:
        return f'mksrc({self.project!r}, {self.snapshot!r}, {self.subgraph_count!r})'


def generate_sources(catalog: str) -> list[SourceSpecArgs]:
    plugin = AzulClient().repository_plugin(catalog)

    def generate_source(spec: TDRSourceSpec) -> SourceSpecArgs:
        spec = attr.evolve(spec, prefix=Prefix.of_everything)
        ref = plugin.resolve_source(str(spec))
        partitions = plugin.list_partitions(ref)
        return SourceSpecArgs(project=spec.project,
                              snapshot=spec.name,
                              subgraph_count=sum(partitions.values()))

    with ThreadPoolExecutor(max_workers=8) as tpe:
        sources = tpe.map(generate_source, plugin.sources)
    return list(sources)


def main(args: list[str]):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=AzulArgumentHelpFormatter)

    parser.add_argument('--catalogs',
                        nargs='+',
                        metavar='NAME',
                        default=[
                            c for c in config.catalogs
                            if c not in config.integration_test_catalogs
                        ],
                        help='The names of the catalogs to determine source specs for.')

    args = parser.parse_args(args)

    for catalog in args.catalogs:
        print(catalog)
        print('-' * len(catalog))
        spec_args_list = generate_sources(catalog)
        spec_args_list.sort(key=lambda spec_args: spec_args.snapshot)
        print(',\n'.join(map(str, spec_args_list)), end='\n\n')

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
