import argparse
import fnmatch
import logging
import shutil
from typing import (
    AbstractSet,
    Mapping,
)

from azul import (
    CatalogName,
    R,
)

log = logging.getLogger(__name__)


class AzulArgumentHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):

    def __init__(self, prog: str):
        super().__init__(prog,
                         max_help_position=50,
                         width=min(shutil.get_terminal_size((80, 25)).columns, 120))


def matching_sources(sources_by_catalog: Mapping[CatalogName, AbstractSet[str]],
                     source_globs: set[str]
                     ) -> dict[CatalogName, set[str]]:
    result = {}
    globs_matched = set()
    for catalog, sources in sources_by_catalog.items():
        catalog_matches = set()
        for source_glob in source_globs:
            matches = fnmatch.filter(sources, source_glob)
            if matches:
                globs_matched.add(source_glob)
            log.debug('Source glob %r matched sources %r in catalog %r',
                      source_glob, matches, catalog)
            catalog_matches.update(matches)
        result[catalog] = catalog_matches
    unmatched = source_globs - globs_matched
    if unmatched:
        log.warning('Source(s) not found in any catalog: %r', unmatched)
    assert any(result.values()), R(
        'No valid sources specified for any catalog')
    return result
